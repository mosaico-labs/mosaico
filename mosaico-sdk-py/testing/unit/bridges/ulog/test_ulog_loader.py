import json
import re
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
    _json_safe,
    _to_arrow_arrays,
    _valid_rows,
)

MAX_BATCH_SIZE_BYTES = 64 * 1024 * 1024
"""Large enough for every topic of the generated file to fit in a single batch."""


def _get(row: dict, path: str):
    """Reads a pyulog flat column name (`a[1].b`) out of a nested row."""
    for part in re.findall(r"\[\d+\]|[^.\[\]]+", path):
        row = row[int(part[1:-1])] if part.startswith("[") else row[part]
    return row


def _rows(msg) -> list:
    """All the rows of a yielded topic, its chunks put together."""
    return pa.Table.from_batches(list(msg.data)).to_pylist()


# --- Loader ---


def test_batches_match_pyulog(ulog_file):
    """Every value of every accepted topic equals pyulog's flat columns."""
    ulog = ULog(str(ulog_file))
    with ULogLoader(ulog_file, MAX_BATCH_SIZE_BYTES) as loader:
        data_topics = [t for t in loader.resolved_topics if not t.startswith("/ulog/")]
        assert sorted(data_topics) == sorted(
            {f"/{d.name}/{d.multi_id}" for d in ulog.data_list}
        )
        for msg in loader:
            if msg.topic_name.startswith("/ulog/"):  # see the synthetic topics test
                continue
            # pyulog gives a topic subscribed twice as two sets of columns
            parts = [
                d.data
                for d in ulog.data_list
                if (d.name, d.multi_id) == (msg.message_name, msg.multi_id)
            ]
            columns = {k: np.concatenate([p[k] for p in parts]) for k in parts[0]}
            idx, _, _ = _valid_rows(columns["timestamp"], msg.topic_name)
            rows = _rows(msg)
            assert len(rows) == loader.msg_count(msg.topic_name) > 0
            assert [r["timestamp_ns"] for r in rows] == [
                int(t) * 1000 for t in columns["timestamp"][idx]
            ]
            for col, values in columns.items():
                if col == "timestamp" or "_padding" in col:
                    continue
                field = re.match(r"[^.\[]+", col).group()
                if isinstance(rows[0][field], str):  # char[N], see test_to_arrow_arrays
                    continue
                actual = [_get(r, col) for r in rows]
                if isinstance(actual[0], bool):
                    values = values.astype(bool)
                np.testing.assert_array_equal(actual, values[idx], err_msg=col)


def test_timestamps_are_non_decreasing(ulog_file):
    """`timestamp_ns` never decreases within a topic, from one chunk to the next too."""
    with ULogLoader(ulog_file, 4096) as loader:
        for msg in loader:
            ts = np.concatenate([b.column("timestamp_ns").to_numpy() for b in msg.data])
            assert np.all(ts[1:] >= ts[:-1]), msg.topic_name


def test_topic_comes_in_chunks_within_the_limit(ulog_file):
    """A topic comes in consecutive chunks within the batch limit, without gaps."""
    limit = 4096
    with ULogLoader(ulog_file, MAX_BATCH_SIZE_BYTES, "/baro/1") as loader:
        (whole,) = next(iter(loader)).data
    with ULogLoader(ulog_file, limit, "/baro/1") as loader:
        chunks = list(next(iter(loader)).data)

    assert len(chunks) > 1
    assert all(pa_ipc.get_record_batch_size(chunk) <= limit for chunk in chunks)
    assert pa.Table.from_batches(chunks).equals(pa.Table.from_batches([whole]))

    with ULogLoader(ulog_file, 8, "/baro/1") as loader:
        with pytest.raises(ValueError, match="Single row"):
            list(next(iter(loader)).data)


def test_filter_rejections_and_metadata(ulog_file):
    """Rejected topics, glob filters, row counts and the bridge metadata of a file."""
    with ULogLoader(ulog_file, MAX_BATCH_SIZE_BYTES) as loader:
        assert dict(loader.rejected_topics) == {
            "/heartbeat/0": TopicStatus.EMPTY_TOPIC,
            "/preflight/0": TopicStatus.NO_VALID_TIMESTAMP,
        }
        assert loader.msg_count() == sum(loader.msg_count(t) for t in loader.topics)
        assert loader.duration > 0

        metadata = loader.sequence_metadata().to_dict()
        json.dumps(metadata, allow_nan=False)
        # Everything the bridge writes is under its reserved key
        assert metadata == {
            "_ulog_": {
                "ulog_info": {
                    "sys_name": "PX4",
                    "ver_hw": "TEST",
                    "time_ref_utc": 3600,
                },
                "ulog_info_types": {
                    "sys_name": "char[3]",
                    "ver_hw": "char[4]",
                    "time_ref_utc": "int32_t",
                },
                "ulog_info_multiple": {"boot_console_output": [["first ", "second"]]},
                "ulog_info_multiple_types": {"boot_console_output": "char[6]"},
                "ulog_initial_parameters": {"P_INT": 1, "P_FLT": 0.25},
                "ulog_default_parameters": {"0": {"P_INT": 0}, "1": {"P_FLT": 0.5}},
                "ulog_ingestion": {
                    "source_file": ulog_file.name,
                    "file_corruption": False,
                    "first_timestamp_ns": 50_000,
                    "last_timestamp_ns": 19_990_000,
                    # The row of /status/0: those of a rejected topic are not counted
                    "rows_dropped_zero_ts": 1,
                    "rows_dropped_overflow_ts": 0,
                },
                # The GPS gives 1_600_000_000_000_500 us (UTC) at 500 us since boot
                "ulog_utc_offset_ns": 1_600_000_000_000_000_000,
            }
        }

        assert loader.topic_schema_metadata("/status/0").to_dict() == {
            "_ulog_": {
                "message_name": "status",
                "multi_id": 0,
                "format_def": "status:uint64_t timestamp;char[4] name;bool ok;"
                "report[2] reports;uint8_t[3] _padding0;",
                "nested_formats": {
                    "report": "report:uint64_t timestamp;int16_t[2] x;uint8_t[4] _padding0;"
                },
                "rows_dropped_zero_ts": 1,
                "rows_dropped_overflow_ts": 0,
            }
        }

    topics = ["/baro/*", "!/baro/1"]
    with ULogLoader(ulog_file, MAX_BATCH_SIZE_BYTES, topics) as loader:
        assert loader.topics == ["/baro/0"]
        assert "/baro/1" in loader.filtered_topics

    with ULogLoader(ulog_file, MAX_BATCH_SIZE_BYTES, "/baro/*") as loader:
        # Instances of the same format share one adapter (and ontology class)
        assert loader.resolve_adapter("/baro/0") is loader.resolve_adapter("/baro/1")


def test_rows_of_each_topic(ulog_file):
    """
    Nested formats, `char[N]`, `bool` and padding are rebuilt from the flat columns; a
    row without timestamp is dropped and rows sharing one are kept; two subscriptions
    of one (format, multi_id) are one topic.
    """

    def reports(timestamp):
        return [
            {"timestamp": timestamp + 1, "x": [10, -10]},
            {"timestamp": timestamp + 2, "x": [20, -20]},
        ]

    topics = ["!/baro/1", "!/gps/0"]
    with ULogLoader(ulog_file, MAX_BATCH_SIZE_BYTES, topics) as loader:
        rows = {msg.topic_name: _rows(msg) for msg in loader}

    assert rows == {
        # The row without timestamp is dropped, the two at 1000 us are both kept
        "/status/0": [
            {
                "timestamp_ns": 1_000_000,
                "name": "ab",
                "ok": True,
                "reports": reports(1000),
            },
            {
                "timestamp_ns": 1_000_000,
                "name": "abcd",
                "ok": False,
                "reports": reports(1000),
            },
            {
                "timestamp_ns": 2_000_000,
                "name": "",
                "ok": True,
                "reports": reports(2000),
            },
        ],
        # Subscribed twice, with rows at [100, 300] and [200] us
        "/baro/0": [{"timestamp_ns": t * 1000, "p": float(t)} for t in (100, 200, 300)],
        "/ulog/logging": [
            {"timestamp_ns": 50_000, "level": 52, "tag": 7, "message": "tagged"},
            {"timestamp_ns": 150_000, "level": 54, "tag": None, "message": "untagged"},
        ],
        # Both estimated at the last data timestamp read before them, hence equal
        "/ulog/changed_parameters": [
            {
                "timestamp_ns": 200_000,
                "name": "P_INT",
                "value_int": 3,
                "value_float": None,
            },
            {
                "timestamp_ns": 200_000,
                "name": "P_FLT",
                "value_int": None,
                "value_float": 0.5,
            },
        ],
        "/ulog/dropouts": [{"timestamp_ns": 500_000, "duration_ms": 12}],
    }


def test_resubscribed_and_synthetic_topics(ulog_file, monkeypatch):
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

    with ULogLoader(ulog_file, MAX_BATCH_SIZE_BYTES) as loader:
        batches = {msg.topic_name: _rows(msg) for msg in loader}
        assert (
            "/ulog/dropouts",
            TopicStatus.NO_VALID_TIMESTAMP,
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
