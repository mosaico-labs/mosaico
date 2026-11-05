import numpy as np
import pyarrow as pa
import pyarrow.compute as pc
import pyarrow.parquet as pq
import pytest

from mosaicolabs.bridges.lerobot.reader import LeRobotReader
from mosaicolabs.bridges.lerobot.validation import (
    _validate_timestamps,
    validate_episode,
)

from .config import DATA_PATH, lengthen_first_episode

FPS = 30

# Valid timestamps: 10 frames at FPS
TS = list(np.arange(10) / FPS)


def _chunked(ts) -> pa.ChunkedArray:
    return pa.chunked_array([ts], pa.float64())


def test_valid_timestamps_pass():
    assert _validate_timestamps(_chunked(TS), FPS) == []
    assert _validate_timestamps(_chunked(TS[:1]), FPS) == []
    assert _validate_timestamps(_chunked([]), FPS) == []


@pytest.mark.parametrize(
    "ts, issue",
    [
        (TS[:5] + [None] + TS[6:], "null or NaN timestamps"),
        (TS[:5] + [TS[4]] + TS[6:], "timestamps not strictly increasing"),
        (TS[:4] + [TS[5], TS[4]] + TS[6:], "timestamps not strictly increasing"),
        (TS[:5] + TS[6:], "timestamps not 1 / fps apart"),
        ([t + 1.0 for t in TS], "timestamps not starting at 0"),
    ],
)
def test_corrupted_timestamps_fail(ts, issue):
    assert issue in _validate_timestamps(_chunked(ts), FPS)


# --- validate_episode ---


def _rewrite_first_episode_timestamps(root, transform):
    """Rewrites the timestamps of episode 0 (in the first data file) with `transform`."""
    path = root / DATA_PATH.format(chunk_index=0, file_index=0)
    table = pq.read_table(path)
    is_first = pc.equal(table.column("episode_index"), 0).to_pylist()
    timestamp = [
        transform(t) if first else t
        for t, first in zip(table.column("timestamp").to_pylist(), is_first)
    ]
    index = table.schema.get_field_index("timestamp")
    pq.write_table(
        table.set_column(
            index, table.schema.field(index), pa.array(timestamp, pa.float32())
        ),
        path,
    )


def _reader(root) -> LeRobotReader:
    return LeRobotReader(root, max_batch_size_bytes=64 * 1024 * 1024)


def test_validate_episode_finds_no_issue_in_a_valid_episode(lerobot_dataset):
    reader = _reader(lerobot_dataset)

    assert all(validate_episode(reader, i) == [] for i in reader.episode_indexes)


def test_validate_episode_finds_null_timestamps(lerobot_dataset_factory):
    root = lerobot_dataset_factory()
    _rewrite_first_episode_timestamps(root, lambda t: None if t > 0 else t)

    issues = validate_episode(_reader(root), 0)

    assert "null or NaN timestamps" in issues
    assert validate_episode(_reader(root), 1) == []


def test_validate_episode_finds_an_unreadable_episode(lerobot_dataset_factory):
    root = lerobot_dataset_factory()
    lengthen_first_episode(root)

    (issue,) = validate_episode(_reader(root), 0)

    assert issue.startswith("unreadable: Episode 0 has")
