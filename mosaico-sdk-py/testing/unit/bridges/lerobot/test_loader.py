import json
from pathlib import Path
from typing import Callable

import pyarrow as pa
import pyarrow.parquet as pq
import pytest

from mosaicolabs.bridges.lerobot.loader import LeRobotLoader
from mosaicolabs.bridges.topic_status import TopicStatus

from .config import (
    ACTION_COLUMN_NAME,
    CAMERA_COLUMN_NAMES,
    COLUMN_NAMES,
    DATA_PATH,
    EPISODE_LENGTHS,
    INDEX_COLUMN_NAMES,
    STATE_COLUMN_NAME,
    TIMESERIES_COLUMN_NAMES,
    TIMESTAMP_COLUMN_NAME,
)

MAX_BATCH_SIZE_BYTES = 64 * 1024 * 1024
"""Large enough for every topic of an episode to be read in a single batch."""

N_FRAMES = sum(EPISODE_LENGTHS)

TIMESERIES_TOPIC_NAME = "/timeseries"
CAMERA_TOPIC_NAMES = {
    "observation.images.front": "/observation/images/front",
    "observation.images.wrist": "/observation/images/wrist",
}


@pytest.fixture
def loader(lerobot_dataset) -> LeRobotLoader:
    return LeRobotLoader(lerobot_dataset, MAX_BATCH_SIZE_BYTES)


def _rewrite_parquet(path: Path, transform: Callable[[pa.Table], pa.Table]):
    pq.write_table(transform(pq.read_table(path)), path)


def _drop_timestamp_column(root: Path):
    _rewrite_parquet(
        root / DATA_PATH.format(chunk_index=0, file_index=0),
        lambda table: table.drop_columns([TIMESTAMP_COLUMN_NAME]),
    )


def _set_unsupported_codebase_version(root: Path):
    info_path = root / "meta" / "info.json"
    info = json.loads(info_path.read_text())
    info["codebase_version"] = "v2.1"
    info_path.write_text(json.dumps(info))


# --- Columns ---


def test_loader_loads_every_column_by_default(loader):
    assert loader.topics == COLUMN_NAMES
    assert loader.resolved_topics == COLUMN_NAMES
    assert loader.rejected_topics == []


@pytest.mark.parametrize(
    ("columns", "accepted"),
    [
        ("observation.images.*", CAMERA_COLUMN_NAMES),
        (["observation.images.*"], CAMERA_COLUMN_NAMES),
        (["*", "!observation.images.*"], TIMESERIES_COLUMN_NAMES),
        (["!observation.*"], [ACTION_COLUMN_NAME, *INDEX_COLUMN_NAMES]),
        # With an inclusion pattern, the selection starts from no column
        (["!observation.*", STATE_COLUMN_NAME], [STATE_COLUMN_NAME]),
        (
            [ACTION_COLUMN_NAME, "*_index"],
            [ACTION_COLUMN_NAME, "frame_index", "episode_index", "task_index"],
        ),
    ],
)
def test_loader_filters_the_columns(lerobot_dataset, columns, accepted):
    loader = LeRobotLoader(lerobot_dataset, MAX_BATCH_SIZE_BYTES, columns)

    # The order of the accepted columns is not defined when they are filtered
    assert sorted(loader.topics) == sorted(accepted)
    assert sorted(loader.filtered_topics) == sorted(set(COLUMN_NAMES) - set(accepted))
    assert all(status is TopicStatus.FILTERED for _, status in loader.rejected_topics)
    assert loader.resolved_topics == COLUMN_NAMES


def test_loader_rejects_a_filter_matching_no_column(lerobot_dataset):
    loader = LeRobotLoader(lerobot_dataset, MAX_BATCH_SIZE_BYTES, ["unknown.*"])

    with pytest.raises(RuntimeError, match="No columns matched"):
        _ = loader.topics


def test_loader_skips_the_video_columns(lerobot_dataset_factory):
    """Video files are not supported yet."""
    root = lerobot_dataset_factory(with_video=True)

    loader = LeRobotLoader(root, MAX_BATCH_SIZE_BYTES)

    assert loader.resolved_topics == COLUMN_NAMES


# --- Opening the dataset ---


@pytest.mark.parametrize(
    ("corrupt", "error"),
    [
        (_set_unsupported_codebase_version, "Unsupported LeRobot dataset version"),
        (_drop_timestamp_column, "no 'timestamp' column"),
    ],
)
def test_loader_reports_a_dataset_that_cannot_be_opened(
    lerobot_dataset_factory, corrupt, error
):
    root = lerobot_dataset_factory()
    corrupt(root)
    loader = LeRobotLoader(root, MAX_BATCH_SIZE_BYTES)

    with pytest.raises(IOError, match=error):
        _ = loader.topics


# --- Properties ---


def test_loader_n_episodes(loader):
    assert loader.n_episodes == len(EPISODE_LENGTHS)


@pytest.mark.parametrize(
    ("column", "topic"),
    [
        *CAMERA_TOPIC_NAMES.items(),
        *[(column, TIMESERIES_TOPIC_NAME) for column in TIMESERIES_COLUMN_NAMES],
        (TIMESTAMP_COLUMN_NAME, None),
        ("unknown", None),
    ],
)
def test_loader_msco_topic_name(loader, column, topic):
    assert loader.msco_topic_name(column) == topic


def test_loader_msco_topic_name_of_a_filtered_column_is_none(lerobot_dataset):
    loader = LeRobotLoader(lerobot_dataset, MAX_BATCH_SIZE_BYTES, [ACTION_COLUMN_NAME])

    assert loader.msco_topic_name(STATE_COLUMN_NAME) is None
    assert loader.msco_topic_name(CAMERA_COLUMN_NAMES[0]) is None


def test_msg_count(loader):
    """Every column has one row per frame."""
    assert loader.msg_count() == len(COLUMN_NAMES) * N_FRAMES
    assert loader.msg_count(ACTION_COLUMN_NAME) == N_FRAMES
    assert loader.msg_count(CAMERA_COLUMN_NAMES[0]) == N_FRAMES
    assert loader.msg_count(TIMESTAMP_COLUMN_NAME) == 0
    assert loader.msg_count("unknown") == 0


@pytest.mark.parametrize("episode_index", range(len(EPISODE_LENGTHS)))
def test_msg_count_of_an_episode(loader, episode_index):
    length = EPISODE_LENGTHS[episode_index]

    assert loader.msg_count(episode_index=episode_index) == len(COLUMN_NAMES) * length
    assert loader.msg_count(ACTION_COLUMN_NAME, episode_index) == length


def test_msg_count_of_an_unknown_episode_is_zero(loader):
    assert loader.msg_count(episode_index=99) == 0


def test_msg_count_of_a_filtered_column(lerobot_dataset):
    """The total counts the accepted columns only, but a filtered column can be counted."""
    loader = LeRobotLoader(lerobot_dataset, MAX_BATCH_SIZE_BYTES, [ACTION_COLUMN_NAME])

    assert loader.msg_count() == N_FRAMES
    assert loader.msg_count(STATE_COLUMN_NAME) == N_FRAMES


# --- Episodes ---


def test_loader_yields_the_episodes_in_order(loader):
    episode_loaders = list(loader)

    assert [e.episode_index for e in episode_loaders] == list(
        range(len(EPISODE_LENGTHS))
    )
    assert [e.n_rows for e in episode_loaders] == EPISODE_LENGTHS


@pytest.mark.parametrize(
    ("columns", "topic"),
    [
        ([CAMERA_COLUMN_NAMES[1]], CAMERA_TOPIC_NAMES[CAMERA_COLUMN_NAMES[1]]),
        ([STATE_COLUMN_NAME], TIMESERIES_TOPIC_NAME),
    ],
)
def test_episode_streams_the_accepted_columns_only(lerobot_dataset, columns, topic):
    episode_loader = next(
        iter(LeRobotLoader(lerobot_dataset, MAX_BATCH_SIZE_BYTES, columns))
    )

    messages = [msg for msg, _ in episode_loader]

    assert [msg.msco_topic_name for msg in messages] == [topic]
    assert [msg.column_names for msg in messages] == [columns]
