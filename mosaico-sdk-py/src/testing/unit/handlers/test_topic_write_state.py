"""
Unit tests for the timestamp ordering check of `_TopicWriteState.push_batch`.

`push_batch` requires `timestamp_ns` to be non-decreasing within a batch: equal
consecutive timestamps are valid, a decreasing timestamp is rejected.
"""

from unittest.mock import MagicMock

import pyarrow as pa
import pytest

from mosaicolabs.handlers.internal.topic_write_state import (
    _is_non_decreasing,
    _TopicWriteState,
)

_DATA_SCHEMA = pa.struct([pa.field("x", pa.float64())])


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


def _push(state: _TopicWriteState, timestamps: list) -> None:
    state.push_batch(
        timestamp_ns=pa.array(timestamps, pa.int64()),
        data=pa.RecordBatch.from_pydict(
            {"x": [float(i) for i in range(len(timestamps))]},
            schema=pa.schema(list(_DATA_SCHEMA)),
        ),
    )


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


@pytest.mark.parametrize("timestamps", [[10, 20, 30], [10, 10, 20], [10, 10, 10]])
def test_push_batch_accepts_non_decreasing_timestamps(state, writer, timestamps):
    _push(state, timestamps)

    writer.write.assert_called_once()
    sent = writer.write.call_args.args[0]
    assert sent.column("timestamp_ns").to_pylist() == timestamps
    assert state._written_records == len(timestamps)


@pytest.mark.parametrize("timestamps", [[20, 10], [10, 20, 20, 15]])
def test_push_batch_rejects_decreasing_timestamps(state, writer, timestamps):
    with pytest.raises(ValueError, match="'timestamp_ns' must be non-decreasing"):
        _push(state, timestamps)

    writer.write.assert_not_called()
    assert state._written_records == 0
