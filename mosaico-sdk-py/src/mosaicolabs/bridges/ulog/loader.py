"""
ULog loader: the read side of the ULog bridge.

It opens the `.ulg` file, classifies every topic as accepted or rejected, resolves one
adapter per topic and then yields the data to the injector, one whole topic at a time.

    .ulg -> pyulog (flat numpy columns per topic)
         -> _valid_rows       (timestamp policy)
         -> _to_arrow_arrays  (flat columns -> nested Arrow arrays)
         -> RecordBatch       (timestamp_ns + payload), chunk by chunk -> ULogInjector
"""

from pathlib import Path
from typing import Dict, Iterator, List, NamedTuple, Optional, Tuple, Union

import numpy as np
import pyarrow as pa
from pyulog import ULog

from mosaicolabs.enum.serialization_format import SerializationFormat
from mosaicolabs.logging_config import get_logger
from mosaicolabs.models.core import Message
from mosaicolabs.models.core.helpers import resolve_ontology_class

from ..helpers import _filter_from_list, _fit_batch
from ..loader_base import AdaptedLoader
from ..protocols.ulog.converter import ULogSchemaConverter
from ..topic_status import TopicStatus
from .adapter_base import ULogAdapterBase, ULogSchemaMetadata, ULogSequenceMetadata
from .adapters.unmodeled import UnmodeledAdapter
from .bridge import ULogBridge
from .helpers import (
    _json_safe,
    _synthetic_topics,
    _to_arrow_arrays,
    _utc_offset_ns,
    _valid_rows,
)
from .ulog_message import ULogMessage

logger = get_logger(__name__)


class _TopicRows(NamedTuple):
    """The valid rows of an accepted topic, computed once in `_resolve_topic`."""

    idx: np.ndarray
    """Indices of the valid rows, sorted by time."""
    zero: int
    """Rows dropped for a zero timestamp."""
    overflow: int
    """Rows dropped for an overflowing timestamp."""
    first_ns: int
    """First valid timestamp. Kept with the last one because the columns they are read
    from are released while iterating."""
    last_ns: int
    """Last valid timestamp."""


class ULogLoader(AdaptedLoader[ULogAdapterBase]):
    """
    ULog loader: reads a PX4 `.ulg` file with pyulog and yields its topics as Arrow batches.

    Every `(format name, multi_id)` pair of the file is a topic named
    `/{name}/{multi_id}`. Logged strings, parameters changed in flight and dropouts
    become the fixed-schema topics `/ulog/logging`, `/ulog/changed_parameters` and
    `/ulog/dropouts` (see `_synthetic_topics`).

    Time: the ULog `timestamp` counts microseconds since the boot of the vehicle, not
    since the Unix epoch, and it is stored as it is (in nanoseconds) in `timestamp_ns`.
    Every flight therefore starts near zero, and time-based queries across sequences are
    meaningless. When the log carries a GPS time, the offset to UTC is stored in the
    sequence metadata (`ulog_utc_offset_ns`, see `sequence_metadata`).

    Timestamps follow `_valid_rows`: rows with a zero or overflowing timestamp are
    dropped and counted. A topic with no `timestamp` field, or left without rows, is
    rejected as `TopicStatus.NO_VALID_TIMESTAMP`; one with no payload field as
    `TopicStatus.EMPTY_TOPIC`; one whose schema cannot be converted as
    `TopicStatus.UNRESOLVED_ADAPTER`. The actual reason is logged.

    Memory: pyulog has no streaming reader and loads the whole file. On top of that the
    loader copies one chunk at a time: the rows of an accepted topic are turned into Arrow
    batches chunk by chunk while iterating, each within `max_batch_size_bytes`, and its
    pyulog columns are released as soon as the next topic is requested. The expected peak
    is therefore about the file size plus one chunk, and memory goes down as the iteration
    proceeds.

    This makes the loader single-pass: the topics can be iterated only once, and the
    chunks of a topic must be consumed before moving to the next one. `close()`, or
    leaving the `with` block, resets the loader: the file is read again on the next access.

    Example:
        ```python
        from mosaicolabs.bridges.ulog import ULogLoader

        topics = ["/sensor_*", "!/sensor_baro/*"]
        with ULogLoader("flight.ulg", client.max_batch_size_bytes, topics) as loader:
            for msg in loader:
                for batch in msg.data:
                    print(msg.topic_name, batch.num_rows)
        ```
    """

    def __init__(
        self,
        file_path: Union[str, Path],
        max_batch_size_bytes: int,
        topics: Optional[Union[str, List[str]]] = None,
        serialization_formats: Optional[Dict[str, SerializationFormat]] = None,
    ):
        """
        Args:
            file_path (Union[str, Path]): Path to the `.ulg` file.
            max_batch_size_bytes (int): The maximum size of the batches a topic is cut
                into (see `MosaicoClient.max_batch_size_bytes`).
            topics (Optional[Union[str, List[str]]]): Topic name(s) or glob patterns
                (`!` excludes), evaluated in order. If None, all topics are loaded.
            serialization_formats (Optional[Dict[str, SerializationFormat]]): Maps a ULog
                format name (e.g. `sensor_accel`) to the `SerializationFormat` of its
                `Unmodeled` ontology. Defaults to `SerializationFormat.Default`.
        """
        super().__init__(container_type=dict)
        self._file_path = Path(file_path)
        self._requested_topics = [topics] if isinstance(topics, str) else topics
        self._serialization_formats = serialization_formats or {}
        self._max_batch_size_bytes = max_batch_size_bytes
        self._ulog: Optional[ULog] = None
        """The parsed file (pyulog keeps everything in memory), lazily loaded."""
        # NOTE: keeps the valid-row indices of every accepted topic (8 B/row)
        self._rows: Dict[str, _TopicRows] = {}
        """Per accepted topic: its valid rows."""
        self._schemas: Dict[str, pa.Schema] = {}
        """Per accepted topic: the Mosaico topic schema, `Message._get_schema(struct)`."""
        self._utc_offset_ns: Optional[int] = None
        """Offset from the timestamps of the log to UTC, if the log carries a GPS time."""

    def _ensure_resolved(self) -> None:
        """
        Lazily reads the file and classifies every topic as accepted or rejected: the
        user's glob filter first (`FILTERED`), then `_resolve_topic`.

        Raises:
            RuntimeError: If no topic is accepted.
        """
        if self._ulog is not None:
            return

        if self._file_path.suffix != ".ulg":
            raise ValueError(
                f"Unsupported format '{self._file_path.suffix}'. Supported: .ulg"
            )
        if not self._file_path.is_file():
            raise FileNotFoundError(f"ULog file not found: {self._file_path}")

        # NOTE: whole file, no message_name_filter_list (it would skew dropout times)
        self._ulog = ULog(str(self._file_path))
        # Read now: it needs the GPS columns, which are released while iterating
        self._utc_offset_ns = _utc_offset_ns(self._ulog)

        # All topics of the file: one per (format name, multi_id) pair...
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

        matched = set(
            _filter_from_list(list(self._resolved_topics), self._requested_topics)
        )

        for topic, data in self._resolved_topics.items():
            if topic not in matched:
                self._reject(topic, TopicStatus.FILTERED)
                continue
            status, reason = self._resolve_topic(topic, data)
            if status is not TopicStatus.ACCEPTED:
                logger.warning(f"Skipping topic '{topic}': {reason}")
                self._reject(topic, status)
                continue
            self._accepted_topics[topic] = data

        if not self._accepted_topics:
            raise RuntimeError(
                "Unable to initialize ULogLoader: no topic matched the criteria. "
                "Try checking the topic filter, if any."
            )

    def _resolve_topic(self, topic: str, data: ULog.Data) -> Tuple[TopicStatus, str]:
        """
        Checks that `topic` can be ingested.

        Returns:
            Tuple[TopicStatus, str]: `TopicStatus.ACCEPTED`, once the adapter, the valid
                rows and the schema of the topic are cached, or the status the topic is
                rejected with, followed by the reason.
        """
        # Timestamp policy first: a topic with no valid row is not ingested at all
        if "timestamp" not in data.data:
            return TopicStatus.NO_VALID_TIMESTAMP, "no top-level 'timestamp' field"
        timestamp_us = data.data["timestamp"]
        idx, zero, overflow = _valid_rows(timestamp_us, topic)
        if not len(idx):
            return TopicStatus.NO_VALID_TIMESTAMP, "no row with a valid timestamp"

        try:
            # Payload schema: from the `F` record (data topics) or fixed (synthetic topics)
            payload = getattr(data, "payload", None)
            struct = (
                pa.struct(list(payload.schema))
                if payload is not None
                else ULogSchemaConverter.to_pyarrow(
                    self._ulog.message_formats, data.name
                )
            )
            if not len(struct):
                return TopicStatus.EMPTY_TOPIC, "empty payload"
            schema = Message._get_schema(
                struct
            )  # raises on a field named `timestamp_ns`

            # A hand-written adapter if the format has one (none for now), else an
            # Unmodeled ontology tagged with the format name. All the multi_id
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
        except Exception as e:
            return TopicStatus.UNRESOLVED_ADAPTER, str(e)

        # safe: _valid_rows only keeps timestamps whose value in ns fits int64, and
        # sorts them, so the first and last index are the bounds of the topic
        self._rows[topic] = _TopicRows(
            idx,
            zero,
            overflow,
            int(timestamp_us[idx[0]]) * 1000,
            int(timestamp_us[idx[-1]]) * 1000,
        )
        self._schemas[topic] = schema
        self._topic_cached_adapters[topic] = adapter
        return TopicStatus.ACCEPTED, ""

    def _build_batch(
        self, topic: str, data: ULog.Data, idx: np.ndarray
    ) -> pa.RecordBatch:
        """
        Builds the rows `idx` of a topic as an Arrow `RecordBatch` in the Mosaico topic
        schema: `timestamp_ns` (envelope) + the payload fields.
        """
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

    def _iter_batches(self, topic: str, data: ULog.Data) -> Iterator[pa.RecordBatch]:
        """
        Yields the valid rows of a topic, in time order, one chunk at a time: only the
        rows of the chunk are copied out of the pyulog columns, so the extra memory stays
        within `max_batch_size_bytes`.
        """
        idx = self._rows[topic].idx
        limit = self._max_batch_size_bytes

        # Rows per chunk, from the size of a row: the pyulog columns, plus the payload
        # of a synthetic topic, which is not among them.
        # NOTE: 10% of margin, since an Arrow batch can be slightly larger than the
        # numpy columns (string offsets, buffer padding, IPC metadata). `_fit_batch`
        # enforces the limit in any case, at the cost of an extra small batch whenever
        # the estimate is too low: measure the built batch instead if that shows up.
        row_size = sum(column.itemsize for column in data.data.values())
        payload = getattr(data, "payload", None)
        if payload is not None:
            row_size += payload.nbytes // payload.num_rows
        step = max(1, limit * 9 // 10 // row_size)
        for start in range(0, len(idx), step):
            batch = self._build_batch(topic, data, idx[start : start + step])
            full_batches, remainder = _fit_batch(batch, limit)
            yield from full_batches
            if remainder.num_rows:
                yield remainder

    # --- Properties ---

    # ProgressManager reads msg_count() before iterating to size the progress bars,
    # which advance by rows

    def msg_count(self, topic: Optional[str] = None) -> int:
        """Returns the number of valid rows of `topic`, or of all accepted topics if None."""
        self._ensure_resolved()
        if topic is None:
            return sum(len(rows.idx) for rows in self._rows.values())
        return len(self._rows[topic].idx) if topic in self._rows else 0

    def _time_range_ns(self) -> Tuple[int, int]:
        """First and last valid timestamp (ns) across the accepted topics."""
        self._ensure_resolved()
        return (
            min(rows.first_ns for rows in self._rows.values()),
            max(rows.last_ns for rows in self._rows.values()),
        )

    @property
    def duration(self) -> int:
        """Returns the time span (ns) of the valid data of the accepted topics."""
        first, last = self._time_range_ns()
        return last - first

    def sequence_metadata(self) -> ULogSequenceMetadata:
        """
        Returns the file-level ULog content to store as sequence metadata, under the
        reserved `_ulog_` key: info, info multiple (with their types), initial and default
        parameters, ingestion stats and, when the log carries a GPS time, the UTC offset.

        `ulog_utc_offset_ns`, added to a `timestamp_ns` of the sequence (which counts from
        the boot of the vehicle), gives UTC epoch nanoseconds (see `_utc_offset_ns`). The
        `time_ref_utc` info record is not that offset: PX4 writes there the time-zone
        offset, in seconds, configured on the vehicle.

        Kept so a future Mosaico -> .ulg extraction can write them back. Everything goes
        through `_json_safe`, otherwise one odd value (bytes, NaN, ...) makes the server
        reject the whole sequence.

        The time range and the dropped-row counts of the ingestion stats cover the
        accepted topics only: the rows of a rejected topic (e.g. one whose timestamps
        are all zero) are not counted.
        """
        self._ensure_resolved()
        u = self._ulog
        first, last = self._time_range_ns()
        fields = {
            "ulog_info": u.msg_info_dict,
            # NOTE: private pyulog attributes, pinned by pyulog<1.3; needed to write back
            "ulog_info_types": u._msg_info_dict_types,
            "ulog_info_multiple": u.msg_info_multiple_dict,
            "ulog_info_multiple_types": u._msg_info_multiple_dict_types,
            "ulog_initial_parameters": u.initial_parameters,
            "ulog_default_parameters": {t: u.get_default_parameters(t) for t in (0, 1)}
            if u.has_default_parameters
            else {},
            "ulog_ingestion": {
                "source_file": self._file_path.name,
                "file_corruption": u.file_corruption,
                "first_timestamp_ns": first,
                "last_timestamp_ns": last,
                "rows_dropped_zero_ts": sum(r.zero for r in self._rows.values()),
                "rows_dropped_overflow_ts": sum(
                    r.overflow for r in self._rows.values()
                ),
            },
        }
        if self._utc_offset_ns is not None:
            fields["ulog_utc_offset_ns"] = self._utc_offset_ns
        return ULogSequenceMetadata(**_json_safe(fields))

    def topic_schema_metadata(self, topic: str) -> ULogSchemaMetadata:
        """
        Returns the `_ulog_` metadata of an accepted topic (original format and drop counts).

        It lives in the loader rather than in the adapter because the original `F`
        definition (padding included) depends on the file, while one adapter class is
        shared by every file with the same Arrow schema.
        """
        self._ensure_resolved()
        data = self._accepted_topics[topic]
        formats = self._ulog.message_formats
        rows = self._rows[topic]
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
            rows_dropped_zero_ts=rows.zero,
            rows_dropped_overflow_ts=rows.overflow,
            **getattr(data, "extra", {}),
        )

    # --- Core Logic ---

    def __iter__(self) -> Iterator[ULogMessage]:
        """
        Yields one `ULogMessage` per accepted topic, whose rows are built lazily, one
        chunk at a time (see `ULogMessage.data`).

        Single-pass: when the next topic is requested, the pyulog columns of the one
        just yielded are released, so memory goes down as the iteration proceeds.
        """
        self._ensure_resolved()
        for topic, data in self._accepted_topics.items():
            yield ULogMessage(
                topic, data.name, data.multi_id, self._iter_batches(topic, data)
            )
            data.data = {}

    def close(self):
        """Releases the in-memory ULog file (it is read again on next access)."""
        self._ulog = None
        self._resolved_topics, self._accepted_topics, self._rejections = {}, {}, {}
        self._topic_cached_adapters, self._rows, self._schemas = {}, {}, {}
        self._utc_offset_ns = None

    def __enter__(self):
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        self.close()
