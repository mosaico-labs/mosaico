"""
ULog loader: the read side of the ULog bridge.

Same role as `MCAPLoader` (bridges/mcap/loader.py): it opens the source file, classifies
every topic as accepted or rejected (via the shared `BaseLoader` bookkeeping), resolves
one adapter per topic and then yields the data to the injector.

The difference is how data flows: MCAP streams one decoded message at a time, while
pyulog loads the whole file and returns one set of flat numpy columns per topic. So
here one yielded item is one whole topic, already converted into an Arrow
`RecordBatch` with the Mosaico topic schema.

    .ulg -> pyulog (flat numpy columns per topic)
         -> _valid_rows       (timestamp policy)
         -> _to_arrow_arrays  (flat columns -> nested Arrow arrays)
         -> RecordBatch       (timestamp_ns + payload)  -> ULogInjector
"""

from pathlib import Path
from typing import Dict, Generator, List, Optional, Tuple, Union

import numpy as np
import pyarrow as pa
from pyulog import ULog

from mosaicolabs.enum.serialization_format import SerializationFormat
from mosaicolabs.logging_config import get_logger
from mosaicolabs.models.core import Message
from mosaicolabs.models.core.helpers import resolve_ontology_class

from ..helpers import _filter_from_list
from ..loader_base import BaseLoader
from ..protocols.ulog.converter import ULogSchemaConverter
from ..topic_status import TopicStatus
from .adapter_base import ULogAdapterBase, ULogSchemaMetadata
from .adapters.unmodeled import UnmodeledAdapter
from .bridge import ULogBridge
from .helpers import _json_safe, _synthetic_topics, _to_arrow_arrays, _valid_rows
from .ulog_message import ULogMessage

logger = get_logger(__name__)


class ULogLoader(BaseLoader[ULogAdapterBase]):
    """
    ULog loader: reads a PX4 `.ulg` file with pyulog and yields its topics as Arrow batches.

    Every `(format name, multi_id)` pair of the file is a topic named
    `/{name}/{multi_id}`. Logged strings, parameters changed in flight and dropouts
    become the fixed-schema topics `/ulog/logging`, `/ulog/changed_parameters` and
    `/ulog/dropouts` (see `_synthetic_topics`). pyulog loads the whole file in memory
    (there is no streaming reader), then each accepted topic is turned into one
    `RecordBatch` at a time while iterating, already in the Mosaico topic schema
    (`timestamp_ns` + payload).

    Timestamps follow `_valid_rows`: rows with a zero or overflowing timestamp are
    dropped and counted. A topic left without rows, with no `timestamp` field or whose
    schema cannot be converted is rejected as `TopicStatus.UNRESOLVED_ADAPTER` (the
    actual reason is logged).

    Example:
        ```python
        from mosaicolabs.bridges.ulog import ULogLoader

        with ULogLoader("flight.ulg", topics=["/sensor_*", "!/sensor_baro/*"]) as loader:
            for msg, exc in loader:
                if not exc:
                    print(msg.topic_name, msg.data.num_rows)
        ```
    """

    def __init__(
        self,
        file_path: Union[str, Path],
        topics: Optional[Union[str, List[str]]] = None,
        serialization_formats: Optional[Dict[str, SerializationFormat]] = None,
    ):
        """
        Args:
            file_path (Union[str, Path]): Path to the `.ulg` file.
            topics (Optional[Union[str, List[str]]]): Topic name(s) or glob patterns
                (`!` excludes), evaluated in order. If None, all topics are loaded.
            serialization_formats (Optional[Dict[str, SerializationFormat]]): Maps a ULog
                format name (e.g. `sensor_accel`) to the `SerializationFormat` of its
                `Unmodeled` ontology. Defaults to `SerializationFormat.Default`.
        """
        # Same constructor shape as MCAPLoader: BaseLoader keeps _resolved_topics,
        # _accepted_topics, _rejections and _topic_cached_adapters for us.
        super().__init__(container_type=dict)
        self._file_path = Path(file_path)
        self._requested_topics = [topics] if isinstance(topics, str) else topics
        self._serialization_formats = serialization_formats or {}
        self._ulog: Optional[ULog] = None
        """The parsed file (pyulog keeps everything in memory), lazily loaded."""
        # NOTE: keeps the valid-row indices of every accepted topic (8 B/row)
        self._rows: Dict[str, Tuple[np.ndarray, int, int]] = {}
        """Per accepted topic: (valid row indices sorted by time, rows with ts == 0,
        rows with an overflowing ts). Computed once in `_resolve_topic`."""
        self._schemas: Dict[str, pa.Schema] = {}
        """Per accepted topic: the Mosaico topic schema, `Message._get_schema(struct)`."""

    def _ensure_resolved(self) -> None:
        """
        Lazily reads the file and classifies every topic as accepted or rejected.

        Mirrors `MCAPLoader._resolve_channels`: list all topics, apply the user's glob
        filter (`FILTERED`), resolve an adapter for each remaining topic
        (`UNRESOLVED_ADAPTER` if that fails), and raise if nothing is accepted.
        """
        if self._ulog is not None:
            return

        # Same checks MCAPFileReader._validate_file does for `.mcap`, inlined here
        if self._file_path.suffix != ".ulg":
            raise ValueError(
                f"Unsupported format '{self._file_path.suffix}'. Supported: .ulg"
            )
        if not self._file_path.is_file():
            raise FileNotFoundError(f"ULog file not found: {self._file_path}")

        # NOTE: whole file, no message_name_filter_list (it would skew dropout times)
        self._ulog = ULog(str(self._file_path))

        # 1) All topics of the file: one per (format name, multi_id) pair...
        for d in self._ulog.data_list:
            topic = f"/{d.name}/{d.multi_id}"
            prev = self._resolved_topics.setdefault(topic, d)
            if prev is not d:
                # same topic subscribed again (appended data, or 'R' then 'A'): one
                # format per file, so the columns match; _valid_rows sorts the rows
                prev.data = {
                    k: np.concatenate([prev.data[k], d.data[k]]) for k in prev.data
                }
        # ...plus the fixed-schema topics (/ulog/logging, ...)
        self._resolved_topics.update(_synthetic_topics(self._ulog))

        # 2) User filter: same glob helper and `!` semantics as MCAP and ROS
        matched = set(
            _filter_from_list(list(self._resolved_topics), self._requested_topics)
        )

        for topic, data in self._resolved_topics.items():
            if topic not in matched:
                self._reject(topic, TopicStatus.FILTERED)
                continue
            # 3) Adapter resolution; any reason to skip the topic is logged, and the
            # topic is reported as UNRESOLVED_ADAPTER (no new TopicStatus added)
            try:
                adapter = self._resolve_topic(topic, data)
            except Exception as e:
                logger.warning(f"Skipping topic '{topic}': {e}")
                self._reject(topic, TopicStatus.UNRESOLVED_ADAPTER)
                continue
            self._accepted_topics[topic] = data
            self._topic_cached_adapters[topic] = adapter

        # 4) Same failure mode as MCAPLoader when nothing survives
        if not self._accepted_topics:
            raise RuntimeError(
                "Unable to initialize ULogLoader: no topic matched the criteria. "
                "Try checking the topic filter, if any."
            )

    def _resolve_topic(self, topic: str, data: ULog.Data) -> type[ULogAdapterBase]:
        """
        Checks that `topic` can be ingested and returns its adapter (raises otherwise).

        The adapter part mirrors `MCAPLoader._get_or_create_adapter`: a hand-written
        adapter from the bridge registry first, otherwise schema -> `pa.StructType` ->
        `resolve_ontology_class` -> `UnmodeledAdapter.get_or_create`.
        """
        # Timestamp policy first: a topic with no valid row is not ingested at all
        if "timestamp" not in data.data:
            raise ValueError("no top-level 'timestamp' field")
        rows = _valid_rows(data.data["timestamp"], topic)
        if not len(rows[0]):
            raise ValueError("no row with a valid timestamp")

        # Payload schema: from the `F` record (data topics) or fixed (synthetic topics)
        payload = getattr(data, "payload", None)  # synthetic topics (_synthetic_topics)
        struct = (
            pa.struct(list(payload.schema))
            if payload is not None
            else ULogSchemaConverter.to_pyarrow(self._ulog.message_formats, data.name)
        )
        if not len(struct):
            raise ValueError("empty payload")
        schema = Message._get_schema(struct)  # raises on a field named `timestamp_ns`

        # Same two strategies as MCAP: hand-written adapter (registry empty for now),
        # else an Unmodeled ontology tagged with the format name. All the multi_id
        # instances of a format share it; resolve_ontology_class creates a separate
        # variant if another file has the same name with a different schema.
        adapter = ULogBridge.get_default_adapter(
            data.name
        ) or UnmodeledAdapter.get_or_create(
            resolve_ontology_class(
                ontology_tag=data.name,
                schema=struct,
                serialization_format=self._serialization_formats.get(
                    data.name, SerializationFormat.Default
                ),
            )
        )
        self._rows[topic] = rows
        self._schemas[topic] = schema
        return adapter

    def _build_batch(self, topic: str, data: ULog.Data) -> pa.RecordBatch:
        """
        Builds the whole topic as one Arrow `RecordBatch` in the Mosaico topic schema:
        `timestamp_ns` (envelope) + the payload fields, valid rows only, sorted by time.
        """
        # Keep only the valid rows (in time order) before any conversion
        idx = self._rows[topic][0]
        payload = getattr(data, "payload", None)
        if payload is not None:
            payload_arrays = payload.take(pa.array(idx)).columns
        else:
            columns = {k: v[idx] for k, v in data.data.items()}
            payload_arrays = _to_arrow_arrays(
                self._ulog.message_formats, data.name, columns
            )
        # safe: _valid_rows only keeps timestamps whose value in ns fits int64
        ts_ns = pa.array(data.data["timestamp"][idx].astype(np.int64) * 1000)
        arrays = [ts_ns, *payload_arrays]
        schema = self._schemas[topic]
        # from_arrays(schema=) would silently cast a wrongly built array
        if not all(a.type.equals(f.type) for a, f in zip(arrays, schema)):
            raise TypeError(f"Arrow arrays of topic '{topic}' do not match its schema.")
        return pa.RecordBatch.from_arrays(arrays, schema=schema)

    # --- Properties ---

    # Same properties as MCAPLoader: ProgressManager (LoaderUIAPI) reads msg_count()
    # before iterating to size the progress bars, which advance by rows.

    def msg_count(self, topic: Optional[str] = None) -> int:
        """Returns the number of valid rows of `topic`, or of all accepted topics if None."""
        self._ensure_resolved()
        if topic is None:
            return sum(len(r[0]) for r in self._rows.values())
        return len(self._rows[topic][0]) if topic in self._rows else 0

    def _time_range_ns(self) -> Tuple[int, int]:
        """First and last valid timestamp (ns) across the accepted topics."""
        self._ensure_resolved()
        # valid rows are sorted: first and last index are each topic's bounds
        bounds = np.array(
            [
                d.data["timestamp"][self._rows[t][0][[0, -1]]]
                for t, d in self._accepted_topics.items()
            ]
        )
        return int(bounds[:, 0].min()) * 1000, int(bounds[:, 1].max()) * 1000

    @property
    def duration(self) -> int:
        """Returns the time span (ns) of the valid data of the accepted topics."""
        first, last = self._time_range_ns()
        return last - first

    def sequence_metadata(self) -> dict:
        """
        Returns the file-level ULog content to store as sequence metadata: info, info
        multiple (with their types), initial and default parameters, and ingestion stats.

        ULog-only (MCAP has no file-level records like these). Kept so a future
        Mosaico -> .ulg extraction can write them back. Everything goes through
        `_json_safe`, otherwise one odd value (bytes, NaN, ...) makes the server
        reject the whole sequence.

        The time range and the dropped-row counts of the ingestion stats cover the
        accepted topics only: the rows of a rejected topic (e.g. one whose timestamps
        are all zero) are not counted.
        """
        self._ensure_resolved()
        u = self._ulog
        first, last = self._time_range_ns()
        return _json_safe(
            {
                "ulog_info": u.msg_info_dict,
                # NOTE: private pyulog attributes, pinned by pyulog<1.3; needed to write back
                "ulog_info_types": u._msg_info_dict_types,
                "ulog_info_multiple": u.msg_info_multiple_dict,
                "ulog_info_multiple_types": u._msg_info_multiple_dict_types,
                "ulog_initial_parameters": u.initial_parameters,
                "ulog_default_parameters": u._default_parameters,
                "ulog_ingestion": {
                    "source_file": self._file_path.name,
                    "file_corruption": u.file_corruption,
                    "first_timestamp_ns": first,
                    "last_timestamp_ns": last,
                    "rows_dropped_zero_ts": sum(r[1] for r in self._rows.values()),
                    "rows_dropped_overflow_ts": sum(r[2] for r in self._rows.values()),
                },
            }
        )

    def topic_schema_metadata(self, topic: str) -> ULogSchemaMetadata:
        """
        Returns the `_ulog_` metadata of an accepted topic (original format and drop counts).

        Same purpose as the `_mcap_` block built by `MCAPAdapterBase.schema_metadata`.
        It lives in the loader rather than in the adapter because the original `F`
        definition (padding included) depends on the file, while one adapter class is
        shared by every file with the same Arrow schema.
        """
        self._ensure_resolved()
        data = self._accepted_topics[topic]
        formats = self._ulog.message_formats
        _, zero, overflow = self._rows[topic]
        synthetic = hasattr(data, "payload")  # no `F` record behind it
        return ULogSchemaMetadata(
            message_name=data.name,
            multi_id=data.multi_id,
            format_def=""
            if synthetic
            else ULogSchemaConverter.format_def(formats, data.name),
            nested_formats={}
            if synthetic
            else ULogSchemaConverter.nested_formats(formats, data.name),
        ).update(
            rows_dropped_zero_ts=zero,
            rows_dropped_overflow_ts=overflow,
            **getattr(data, "extra", {}),
        )

    # --- Core Logic ---

    def __iter__(
        self,
    ) -> Generator[Tuple[ULogMessage, Optional[Exception]], None, None]:
        """
        Yields one `(ULogMessage, exception)` per accepted topic, built lazily.

        Same `(item, exc)` contract as `MCAPLoader.__iter__`, so the injector loop is
        unchanged; here one item is a whole topic instead of a single message. Topics
        are built one at a time, so memory holds pyulog's data plus one topic batch.
        """
        self._ensure_resolved()
        for topic, data in self._accepted_topics.items():
            msg = ULogMessage(topic, data.name, data.multi_id, None)
            try:
                msg.data = self._build_batch(topic, data)
            except Exception as e:
                yield msg, e
                continue
            yield msg, None

    # Same resource lifecycle as MCAPLoader (close + context manager)

    def close(self):
        """Releases the in-memory ULog file (it is read again on next access)."""
        self._ulog = None
        self._resolved_topics, self._accepted_topics, self._rejections = {}, {}, {}
        self._topic_cached_adapters, self._rows, self._schemas = {}, {}, {}

    def __enter__(self):
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        self.close()
