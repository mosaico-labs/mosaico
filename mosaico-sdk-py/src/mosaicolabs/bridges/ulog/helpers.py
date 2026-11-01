"""
ULog bridge helpers.

- `_valid_rows`: the timestamp policy (which rows are kept, and in which order).
- `_to_arrow_arrays`: pyulog flat numpy columns -> nested Arrow arrays. It plays the
  role `MCAPMsgDecoderBase._to_dict()` plays for MCAP (native data -> the shape of the
  Unmodeled schema), but on whole columns instead of one message at a time.
- `_chunks`: cuts a topic into the batches sent with `TopicWriter.push_batch`.
- `_synthetic_topics`: the guide's fixed-schema topics for non-data ULog records.
- `_json_safe`: makes pyulog values acceptable as server metadata.
"""

import base64
import math
import re
from types import SimpleNamespace
from typing import Any, Dict, Iterator, List, Tuple

import numpy as np
import pyarrow as pa
import pyarrow.ipc as pa_ipc

from mosaicolabs.handlers.helpers import _SUPPORTED_METADATA_KEY_CHARS
from mosaicolabs.logging_config import get_logger

from ..protocols.ulog.converter import _PRIMITIVES, ULogSchemaConverter

logger = get_logger(__name__)

_MAX_TIMESTAMP_US = (2**63 - 1) // 1000
"""Largest timestamp (us) whose value in ns still fits `Message.timestamp_ns` (int64)."""


def _valid_rows(timestamp_us: np.ndarray, topic: str) -> Tuple[np.ndarray, int, int]:
    """
    Applies the ULog timestamp policy to a topic's `timestamp` column (uint64, us).

    Rows with `timestamp == 0` (struct published without a timestamp) and rows whose
    value in ns would overflow int64 (corrupted records) are dropped, *before* any
    multiplication: `numpy.uint64 * 1000` overflows silently. The remaining rows are
    stably sorted by timestamp, which should never be needed and is only a safety net,
    since Mosaico expects non-decreasing timestamps per topic.

    Returns:
        Tuple[np.ndarray, int, int]: The indices of the valid rows in timestamp order,
            the number of rows dropped for a zero timestamp and for an overflowing one.
    """
    # Checked on the raw uint64 values, before any `* 1000`
    zero = timestamp_us == 0
    overflow = timestamp_us > _MAX_TIMESTAMP_US
    idx = np.flatnonzero(~(zero | overflow))
    # Safety net only: after the filter above, all the real logs analysed were sorted
    ts = timestamp_us[idx]
    if np.any(ts[1:] < ts[:-1]):
        logger.warning(f"Topic '{topic}' has decreasing timestamps: sorting it.")
        idx = idx[np.argsort(ts, kind="stable")]
    return idx, int(zero.sum()), int(overflow.sum())


def _strings(chars: np.ndarray) -> pa.Array:
    """`char[N]` rows (int8) -> strings, cut at the first NUL."""
    # ponytail: undecodable bytes become U+FFFD instead of rejecting the whole topic
    return pa.array(
        [
            r.tobytes().split(b"\0", 1)[0].decode("utf-8", "replace")
            for r in chars.view(np.uint8)
        ],
        pa.string(),
    )


def _scalar(
    formats: dict, ulog_type: str, key: str, columns: Dict[str, np.ndarray]
) -> pa.Array:
    """One non-array field: a primitive column, or a nested format rebuilt as a struct."""
    if ulog_type in _PRIMITIVES:
        col = columns[key]
        # pyulog returns bool as int8
        return pa.array(
            col.astype(bool) if ulog_type == "bool" else col,
            _PRIMITIVES[ulog_type],
        )
    # Nested format: its fields are the columns named `key.<field>`, e.g.
    # `heartbeats[0].system_id`; padding is skipped by `fields`
    fields = ULogSchemaConverter.fields(formats, ulog_type)
    return pa.StructArray.from_arrays(
        [_column(formats, t, s, f"{key}.{n}", columns) for t, s, n in fields],
        [n for _, _, n in fields],
    )


def _column(
    formats: dict, ulog_type: str, size: int, key: str, columns: Dict[str, np.ndarray]
) -> pa.Array:
    """
    One field of the format, array or not, rebuilt from pyulog's flat columns.

    Example: `uint8_t[3] clip_counter` comes as the columns `clip_counter[0]`,
    `clip_counter[1]`, `clip_counter[2]` and becomes one list column with
    `[c0, c1, c2]` per row.
    """
    # char / char[N]: N int8 columns (one per character) -> one string per row
    if ulog_type == "char":
        keys = [f"{key}[{i}]" for i in range(size)] if size else [key]
        return _strings(np.stack([columns[k] for k in keys], axis=1))
    if not size:
        return _scalar(formats, ulog_type, key, columns)
    # T[N] (primitive or nested format): build the N elements, then one fixed-size list
    elements = [
        _scalar(formats, ulog_type, f"{key}[{i}]", columns) for i in range(size)
    ]
    rows = len(elements[0])
    # element-major (a[0] of every row, then a[1], ...) -> row-major
    idx = (np.arange(rows)[:, None] + np.arange(size)[None, :] * rows).reshape(-1)
    return pa.FixedSizeListArray.from_arrays(
        pa.concat_arrays(elements).take(pa.array(idx)), size
    )


def _to_arrow_arrays(
    formats: dict, name: str, columns: Dict[str, np.ndarray]
) -> List[pa.Array]:
    """
    Rebuilds the nested payload of format `name` from pyulog's flat columns (`a`, `a[i]`,
    `a.b`, `a[i].b`), one Arrow array per top-level field of
    `ULogSchemaConverter.to_pyarrow(formats, name)`, in the same order and type.

    pyulog gives no pandas or Arrow object, only one numpy array per flat column, so
    the nesting described by the `F` record has to be rebuilt here, field by field.
    """
    # Same walk as ULogSchemaConverter.to_pyarrow, so arrays and schema line up
    return [
        _column(formats, t, s, n, columns)
        for t, s, n in ULogSchemaConverter.fields(formats, name)
        if n != "timestamp"
    ]


def _chunks(batch: pa.RecordBatch, max_bytes: int) -> Iterator[pa.RecordBatch]:
    """
    Splits `batch` into consecutive slices within `max_bytes`, each as large as possible.

    `TopicWriter.push_batch` (from the LeRobot bridge, issue #945) sends each call as a
    single Flight message and does not split it, so a topic is cut here. The size is the
    Arrow IPC size, the one `push_batch` checks. As in the LeRobot bridge, the length of a
    slice is found by bisection, since the size never decreases when rows are added.

    Raises:
        ValueError: If a single row exceeds `max_bytes` (a ULog record is at most 64 KiB).
    """
    while batch.num_rows:
        if pa_ipc.get_record_batch_size(batch) <= max_bytes:
            yield batch
            return
        # `lo` leading rows are known to fit, `hi` rows are known not to
        lo, hi = 0, batch.num_rows
        while hi - lo > 1:
            mid = (lo + hi) // 2
            if pa_ipc.get_record_batch_size(batch.slice(0, mid)) <= max_bytes:
                lo = mid
            else:
                hi = mid
        if lo == 0:
            raise ValueError(
                f"A single row exceeds the batch limit of {max_bytes} bytes."
            )
        yield batch.slice(0, lo)
        batch = batch.slice(lo)


def _synthetic(name: str, timestamps: list, columns: dict, **extra) -> SimpleNamespace:
    """
    A pyulog-`Data`-like topic whose payload is already an Arrow batch.

    It has the same `name`, `multi_id` and `data["timestamp"]` attributes as a real
    `ULog.Data`, so the loader handles it like any other topic (same filter, same
    timestamp policy, same write path); only the payload is built differently.
    """
    return SimpleNamespace(
        name=name,
        multi_id=0,
        data={"timestamp": np.array(timestamps, np.uint64)},
        payload=pa.RecordBatch.from_pydict(columns),
        extra=extra,
    )


def _synthetic_topics(ulog) -> Dict[str, SimpleNamespace]:
    """
    Fixed-schema topics for the ULog records that are not data messages: logged strings
    (`L` untagged with a null `tag`, `C` tagged), parameters changed in flight (`P`) and
    dropouts (`O`). These last two have no time of their own: pyulog gives them the last
    data timestamp read before them, so they are flagged `timestamp_estimated`.
    Topics without records are left out.

    Topic names and contents follow the ULog bridge design guide.
    """
    logs = sorted(
        [
            *ulog.logged_messages,
            *(m for ms in ulog.logged_messages_tagged.values() for m in ms),
        ],
        key=lambda m: m.timestamp,
    )
    params, drops = ulog.changed_parameters, ulog.dropouts
    topics = {
        "/ulog/logging": _synthetic(
            "ulog_logging",
            [m.timestamp for m in logs],
            {
                "level": pa.array([m.log_level for m in logs], pa.uint8()),
                "tag": pa.array([getattr(m, "tag", None) for m in logs], pa.uint16()),
                "message": pa.array([m.message for m in logs], pa.string()),
            },
        ),
        "/ulog/changed_parameters": _synthetic(
            "ulog_changed_parameters",
            [t for t, _, _ in params],
            {
                "name": pa.array([n for _, n, _ in params], pa.string()),
                # ULog parameters are int32_t or float
                "value_int": pa.array(
                    [v if isinstance(v, int) else None for _, _, v in params],
                    pa.int32(),
                ),
                "value_float": pa.array(
                    [v if isinstance(v, float) else None for _, _, v in params],
                    pa.float32(),
                ),
            },
            timestamp_estimated=True,
        ),
        "/ulog/dropouts": _synthetic(
            "ulog_dropouts",
            [d.timestamp for d in drops],
            {"duration_ms": pa.array([d.duration for d in drops], pa.uint16())},
            timestamp_estimated=True,
        ),
    }
    return {t: s for t, s in topics.items() if len(s.data["timestamp"])}


# --- Metadata sanitising ---
# The sequence metadata is sent as JSON and validated by the server: numpy scalars,
# bytes, NaN, NUL characters or unsupported key characters would make it reject the
# whole sequence, so every value coming from pyulog goes through `_json_safe`.


def _safe_key(key: Any) -> str:
    """Replaces the characters the server rejects in metadata keys (corrupted names) with `_`."""
    key = "".join(c if c in _SUPPORTED_METADATA_KEY_CHARS else "_" for c in str(key))
    return re.sub("-(?=-)", "_", key) or "_"  # "--" is rejected too


def _json_safe(value: Any) -> Any:
    """Makes pyulog values valid server metadata (JSON without NaN/NUL, supported keys)."""
    if isinstance(value, dict):
        return {_safe_key(k): _json_safe(v) for k, v in value.items()}
    if isinstance(value, (list, tuple)):
        return [_json_safe(v) for v in value]
    if isinstance(value, (bytes, bytearray)):
        # e.g. PX4 `metadata_events` blobs in INFO_MULTIPLE
        return base64.b64encode(value).decode("ascii")
    if isinstance(value, np.generic):
        value = value.item()
    if isinstance(value, float) and not math.isfinite(value):
        return None
    if isinstance(value, str):
        return value.replace("\x00", "")
    return value
