import json
import re
from pathlib import Path
from types import SimpleNamespace as Fmt

import numpy as np
import pyarrow as pa
import pyarrow.ipc as pa_ipc
import pytest
from pyulog import ULog

from mosaicolabs.bridges.protocols.ulog import ULogSchemaConverter
from mosaicolabs.bridges.topic_status import TopicStatus
from mosaicolabs.bridges.ulog import ULogLoader
from mosaicolabs.bridges.ulog.helpers import (
    _chunks,
    _json_safe,
    _to_arrow_arrays,
    _valid_rows,
)

ULOG_DIR = Path(__file__).resolve().parents[3] / "data" / "ulog"
ULOG_FILES = sorted(ULOG_DIR.glob("*.ulg"))


def _get(row: dict, path: str):
    """Reads a pyulog flat column name (`a[1].b`) out of a nested row."""
    for part in re.findall(r"\[\d+\]|[^.\[\]]+", path):
        row = row[int(part[1:-1])] if part.startswith("[") else row[part]
    return row


# --- Loader ---


@pytest.mark.parametrize("path", ULOG_FILES, ids=lambda p: p.stem)
def test_batches_match_pyulog(path):
    """Every value of every accepted topic equals pyulog's flat columns."""
    ulog = ULog(str(path))
    with ULogLoader(path) as loader:
        data_topics = [t for t in loader.resolved_topics if not t.startswith("/ulog/")]
        assert sorted(data_topics) == sorted(
            f"/{d.name}/{d.multi_id}" for d in ulog.data_list
        )
        for msg, exc in loader:
            assert exc is None
            if msg.topic_name.startswith("/ulog/"):  # see the synthetic topics test
                continue
            data = next(
                d
                for d in ulog.data_list
                if (d.name, d.multi_id) == (msg.message_name, msg.multi_id)
            )
            idx, _, _ = _valid_rows(data.data["timestamp"], msg.topic_name)
            rows = msg.data.to_pylist()
            assert len(rows) == loader.msg_count(msg.topic_name) > 0
            assert [r["timestamp_ns"] for r in rows] == [
                int(t) * 1000 for t in data.data["timestamp"][idx]
            ]
            for col, values in data.data.items():
                if col == "timestamp" or "_padding" in col:
                    continue
                actual = [_get(r, col) for r in rows]
                if isinstance(actual[0], str):  # char[N], see test_to_arrow_arrays
                    continue
                if isinstance(actual[0], bool):
                    values = values.astype(bool)
                np.testing.assert_array_equal(actual, values[idx], err_msg=col)


def test_filter_rejections_and_metadata():
    """Rejected topics, glob filters, row counts and the bridge metadata of a real file."""
    path = ULOG_DIR / "sample_log_small.ulg"

    with ULogLoader(path) as loader:
        assert (
            "/test_motor/0",
            TopicStatus.UNRESOLVED_ADAPTER,
        ) in loader.rejected_topics
        assert loader.msg_count() == sum(loader.msg_count(t) for t in loader.topics)
        assert loader.duration > 0
        meta = loader.sequence_metadata()
        json.dumps(meta, allow_nan=False)
        assert meta["ulog_info"]["sys_name"]
        assert meta["ulog_ingestion"]["source_file"] == path.name
        topic_meta = loader.topic_schema_metadata("/sensor_accel/1").to_dict()["_ulog_"]
        assert topic_meta["message_name"] == "sensor_accel"
        assert topic_meta["multi_id"] == 1
        assert topic_meta["format_def"].startswith("sensor_accel:uint64_t timestamp;")
        # The fixture has 3 logged strings and 1 dropout
        assert loader.msg_count("/ulog/logging") == 3
        assert loader.msg_count("/ulog/dropouts") == 1

    with ULogLoader(path, ["/sensor_accel/*", "!/sensor_accel/2"]) as loader:
        assert sorted(loader.topics) == ["/sensor_accel/0", "/sensor_accel/1"]
        assert "/sensor_accel/2" in loader.filtered_topics
        # Instances of the same format share one adapter (and ontology class)
        assert loader.resolve_adapter("/sensor_accel/0") is loader.resolve_adapter(
            "/sensor_accel/1"
        )


def test_resubscribed_and_synthetic_topics(monkeypatch):
    """
    Two subscriptions of one (format, multi_id), e.g. appended data, are one topic;
    logged strings, changed parameters and dropouts become the `/ulog/*` topics.
    """

    def data(ts):
        ts = np.array(ts, np.uint64)
        return Fmt(
            name="baro", multi_id=0, data={"timestamp": ts, "p": ts.astype(np.float32)}
        )

    fake = Fmt(
        data_list=[data([100, 300]), data([200])],
        message_formats={
            "baro": Fmt(fields=[("uint64_t", 0, "timestamp"), ("float", 0, "p")])
        },
        logged_messages=[Fmt(log_level=54, timestamp=150, message="untagged")],
        logged_messages_tagged={
            7: [Fmt(log_level=52, tag=7, timestamp=50, message="t")]
        },
        changed_parameters=[(250, "P_INT", 3), (260, "P_FLT", 0.5)],
        dropouts=[Fmt(timestamp=0, duration=12)],  # no valid timestamp: rejected
    )
    monkeypatch.setattr("mosaicolabs.bridges.ulog.loader.ULog", lambda _: fake)

    with ULogLoader(ULOG_FILES[0]) as loader:
        batches = {msg.topic_name: msg.data.to_pylist() for msg, _ in loader}
        assert (
            "/ulog/dropouts",
            TopicStatus.UNRESOLVED_ADAPTER,
        ) in loader.rejected_topics
        meta = loader.topic_schema_metadata("/ulog/changed_parameters").fields
        assert meta["timestamp_estimated"] and meta["format_def"] == ""

    assert batches == {
        "/baro/0": [{"timestamp_ns": t * 1000, "p": float(t)} for t in (100, 200, 300)],
        "/ulog/logging": [
            {"timestamp_ns": 50_000, "level": 52, "tag": 7, "message": "t"},
            {"timestamp_ns": 150_000, "level": 54, "tag": None, "message": "untagged"},
        ],
        "/ulog/changed_parameters": [
            {
                "timestamp_ns": 250_000,
                "name": "P_INT",
                "value_int": 3,
                "value_float": None,
            },
            {
                "timestamp_ns": 260_000,
                "name": "P_FLT",
                "value_int": None,
                "value_float": 0.5,
            },
        ],
    }


# --- Helpers ---


def test_valid_rows():
    """Zero and overflowing timestamps are dropped, the rest is stably sorted."""
    ts = np.array([5, 0, 3, 12826408969104612878, 3, 0], dtype=np.uint64)
    idx, zero, overflow = _valid_rows(ts, "/t/0")
    assert idx.tolist() == [2, 4, 0]
    assert (zero, overflow) == (2, 1)


def test_to_arrow_arrays():
    """Nested format arrays with rows != elements, chars cut at NUL, invalid utf-8, bools."""
    formats = {
        "inner": Fmt(
            fields=[
                ("uint64_t", 0, "timestamp"),
                ("int16_t", 2, "x"),
                ("uint8_t", 1, "_padding0"),
            ]
        ),
        "outer": Fmt(
            fields=[
                ("uint64_t", 0, "timestamp"),
                ("char", 4, "name"),
                ("bool", 0, "ok"),
                ("inner", 3, "items"),
            ]
        ),
    }
    cols = {"timestamp": np.array([1, 2], np.uint64), "ok": np.array([0, 1], np.int8)}
    for i, chars in enumerate(zip(b"ab\0z", b"\xffA\0B")):
        cols[f"name[{i}]"] = np.frombuffer(bytes(chars), np.int8)
    for i in range(3):
        cols[f"items[{i}].timestamp"] = np.array([10 + i, 20 + i], np.uint64)
        cols[f"items[{i}]._padding0[0]"] = np.zeros(2, np.uint8)
        for j in range(2):
            cols[f"items[{i}].x[{j}]"] = np.array(
                [100 * i + j, -(100 * i + j)], np.int16
            )

    arrays = _to_arrow_arrays(formats, "outer", cols)
    struct = ULogSchemaConverter.to_pyarrow(formats, "outer")
    assert [a.type for a in arrays] == [f.type for f in struct]
    rows = pa.RecordBatch.from_arrays(
        arrays, schema=pa.schema(list(struct))
    ).to_pylist()
    assert rows == [
        {
            "name": "ab",
            "ok": False,
            "items": [
                {"timestamp": 10, "x": [0, 1]},
                {"timestamp": 11, "x": [100, 101]},
                {"timestamp": 12, "x": [200, 201]},
            ],
        },
        {
            "name": "�A",
            "ok": True,
            "items": [
                {"timestamp": 20, "x": [0, -1]},
                {"timestamp": 21, "x": [-100, -101]},
                {"timestamp": 22, "x": [-200, -201]},
            ],
        },
    ]


def test_chunks():
    """A topic is cut into the largest slices within the limit, in order and without gaps."""
    n_rows = 1000
    batch = pa.RecordBatch.from_pydict(
        {
            "timestamp_ns": pa.array(range(n_rows), pa.int64()),
            "message": [f"row {i}" for i in range(n_rows)],  # rows of different sizes
        }
    )
    size = pa_ipc.get_record_batch_size(batch)
    max_bytes = size // 3
    chunks = list(_chunks(batch, max_bytes))

    assert len(chunks) > 1
    assert pa.Table.from_batches(chunks).equals(pa.Table.from_batches([batch]))
    offset = 0
    for chunk in chunks:
        assert pa_ipc.get_record_batch_size(chunk) <= max_bytes
        # As large as possible: one more row would not fit
        if offset + chunk.num_rows < n_rows:
            next_slice = batch.slice(offset, chunk.num_rows + 1)
            assert pa_ipc.get_record_batch_size(next_slice) > max_bytes
        offset += chunk.num_rows

    # A topic within the limit is sent as it is
    assert [c.num_rows for c in _chunks(batch, size)] == [n_rows]
    with pytest.raises(ValueError, match="single row"):
        list(_chunks(batch, 8))


def test_json_safe():
    """Metadata values become JSON-safe and keys are made valid for the server."""
    meta = _json_safe(
        {
            1: np.int64(3),
            "b": b"\x01\x02",
            "f": [float("nan"), np.float32(1.5)],
            "s": "a\x00",
            "FW$TRIM--X": 0,  # corrupted parameter name
        }
    )
    assert meta == {"1": 3, "b": "AQI=", "f": [None, 1.5], "s": "a", "FW_TRIM_-X": 0}
    json.dumps(meta, allow_nan=False)
