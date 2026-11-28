"""
Unit tests for the timestamp ordering checks of `_TopicWriteState`.

The `timestamp_ns` of the records written to a topic must be non-decreasing,
within a batch and across `push_record()`/`push_batch()` calls: equal
consecutive timestamps are valid, a decreasing timestamp is rejected.
"""

from unittest.mock import MagicMock

import pyarrow as pa
import pytest

from mosaicolabs.handlers.internal.topic_write_state import (
    _is_non_decreasing,
    _TopicWriteState,
)
from mosaicolabs.models.core import Message
from mosaicolabs.models.data import Floating64

_DATA_SCHEMA = Floating64.__msco_pyarrow_struct__
_DATA_FIELD = _DATA_SCHEMA[0].name


@pytest.fixture
def writer() -> MagicMock:
    return MagicMock()


@pytest.fixture
def state(writer) -> _TopicWriteState:
    return _TopicWriteState(
        topic_name="/topic",
        data_schema=_DATA_SCHEMA,
        writer=writer,
        max_batch_size_bytes=1024 * 1024,
    )


def _push_batch(state: _TopicWriteState, timestamps: list) -> None:
    state.push_batch(
        timestamp_ns=pa.array(timestamps, pa.int64()),
        data=pa.RecordBatch.from_pydict(
            {_DATA_FIELD: [float(i) for i in range(len(timestamps))]},
            schema=pa.schema(list(_DATA_SCHEMA)),
        ),
    )


def _push_record(state: _TopicWriteState, timestamp: int) -> bool:
    return state.push_record(Message(timestamp_ns=timestamp, data=Floating64(data=0.0)))


def _sent_timestamps(writer: MagicMock) -> list:
    """The `timestamp_ns` of every record sent on the wire, in order."""
    return [
        ts
        for call in writer.write.call_args_list
        for ts in call.args[0].column("timestamp_ns").to_pylist()
    ]


# --- _is_non_decreasing ---


@pytest.mark.parametrize(
    "values, expected",
    [
        ([], True),
        ([5], True),
        ([1, 2, 3], True),
        ([1, 1, 1], True),
        ([1, 2, 2, 3], True),
        ([2, 1], False),
        ([1, 3, 3, 2], False),
        ([1, None, 2], False),
    ],
)
def test_is_non_decreasing(values, expected):
    assert _is_non_decreasing(pa.array(values, pa.int64())) is expected


# --- Within a batch ---


@pytest.mark.parametrize("timestamps", [[10, 20, 30], [10, 10, 20], [10, 10, 10]])
def test_push_batch_accepts_non_decreasing_timestamps(state, writer, timestamps):
    _push_batch(state, timestamps)

    assert _sent_timestamps(writer) == timestamps
    assert state._written_records == len(timestamps)


@pytest.mark.parametrize("timestamps", [[20, 10], [10, 20, 20, 15]])
def test_push_batch_rejects_decreasing_timestamps(state, writer, timestamps):
    with pytest.raises(ValueError, match="'timestamp_ns' must be non-decreasing: "):
        _push_batch(state, timestamps)

    writer.write.assert_not_called()
    assert state._written_records == 0


# --- Across pushes ---


def test_push_record_accepts_non_decreasing_timestamps(state, writer):
    for ts in [10, 10, 20, 30, 30]:
        assert _push_record(state, ts)
    state.close()

    assert _sent_timestamps(writer) == [10, 10, 20, 30, 30]


def test_push_record_rejects_timestamp_lower_than_last(state, writer):
    _push_record(state, 20)

    with pytest.raises(ValueError, match="across pushes on topic '/topic': got 10"):
        _push_record(state, 10)

    # The rejected record is not buffered, and the writer keeps accepting records
    # that follow the last accepted one.
    assert _push_record(state, 20)
    state.close()
    assert _sent_timestamps(writer) == [20, 20]


@pytest.mark.parametrize("next_batch", [[30, 40], [40, 40]])
def test_push_batch_accepts_batch_following_previous_one(state, writer, next_batch):
    _push_batch(state, [10, 30])
    _push_batch(state, next_batch)

    assert _sent_timestamps(writer) == [10, 30, *next_batch]


def test_push_batch_rejects_batch_starting_before_previous_one(state, writer):
    _push_batch(state, [10, 30])

    with pytest.raises(ValueError, match="across pushes on topic '/topic': got 20"):
        _push_batch(state, [20, 40])

    assert _sent_timestamps(writer) == [10, 30]
    assert state._written_records == 2


def test_push_batch_rejects_batch_starting_before_buffered_record(state, writer):
    _push_record(state, 30)

    with pytest.raises(ValueError, match="across pushes"):
        _push_batch(state, [20, 40])

    # The rejected batch does not flush the buffer: the record is still pending.
    writer.write.assert_not_called()
    state.close()
    assert _sent_timestamps(writer) == [30]


def test_push_record_rejects_timestamp_lower_than_previous_batch(state, writer):
    _push_batch(state, [10, 30])

    with pytest.raises(ValueError, match="across pushes"):
        _push_record(state, 20)

    assert _push_record(state, 30)
    state.close()
    assert _sent_timestamps(writer) == [10, 30, 30]


def test_rejected_batch_does_not_move_last_timestamp(state, writer):
    _push_batch(state, [10, 20])

    # Rejected within the batch: its last timestamp (50) must not be recorded.
    with pytest.raises(ValueError):
        _push_batch(state, [40, 30, 50])

    _push_batch(state, [20, 25])
    assert _sent_timestamps(writer) == [10, 20, 20, 25]


def test_empty_batch_does_not_move_last_timestamp(state, writer):
    _push_batch(state, [10, 20])
    _push_batch(state, [])

    with pytest.raises(ValueError, match="across pushes"):
        _push_batch(state, [15])
    _push_batch(state, [20])


def test_oversized_record_does_not_move_last_timestamp(state, writer):
    _push_record(state, 10)

    # A record that alone exceeds the size limit is dropped, not stored: its
    # timestamp must not constrain the following records.
    max_size = state.max_batch_size_bytes
    state.max_batch_size_bytes = 1
    assert not _push_record(state, 50)
    state.max_batch_size_bytes = max_size

    assert _push_record(state, 20)
    state.close()
    assert _sent_timestamps(writer) == [10, 20]
