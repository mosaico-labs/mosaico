from dataclasses import replace

import pytest

from mosaicolabs.bridges.lerobot.metadata import LeRobotDatasetMeta
from mosaicolabs.bridges.lerobot.metadata.dataset_builder import LeRobotDatasetBuilder

from .config import EPISODE_LENGTHS, VIDEO_COLUMN_NAME


def test_builder_renumbers_the_episodes(lerobot_dataset):
    meta = LeRobotDatasetMeta.load(lerobot_dataset)
    builder = LeRobotDatasetBuilder()

    # Episode 2 of the source dataset first, then episode 0
    builder.add_episode("a", meta.info, meta.episodes_map[2])
    builder.add_episode("b", meta.info, meta.episodes_map[0])

    first, second = EPISODE_LENGTHS[2], EPISODE_LENGTHS[0]
    assert [
        (e.episode_index, e.dataset_from_index, e.dataset_to_index)
        for e in builder.episodes
    ] == [(0, 0, first), (1, first, first + second)]
    assert builder.episode_index_by_sequence == {"a": 0, "b": 1}
    assert all(
        (e.data_chunk_index, e.data_file_index) == (0, 0) for e in builder.episodes
    )
    assert builder.info is not None
    assert (builder.info.total_episodes, builder.info.total_frames) == (
        2,
        first + second,
    )
    assert builder.info.splits == {"train": "0:2"}


def test_builder_rejects_an_incompatible_episode(lerobot_dataset):
    meta = LeRobotDatasetMeta.load(lerobot_dataset)
    builder = LeRobotDatasetBuilder()
    builder.add_episode("a", meta.info, meta.episodes_map[0])

    other_robot = replace(meta.info, robot_type="other_robot")

    with pytest.raises(ValueError, match="robot type"):
        builder.add_episode("b", other_robot, meta.episodes_map[1])
    assert len(builder.episodes) == 1
    assert builder.episode_index_by_sequence == {"a": 0}


def test_builder_rejects_a_sequence_added_twice(lerobot_dataset):
    meta = LeRobotDatasetMeta.load(lerobot_dataset)
    builder = LeRobotDatasetBuilder()
    builder.add_episode("a", meta.info, meta.episodes_map[0])

    with pytest.raises(ValueError, match="already been added"):
        builder.add_episode("a", meta.info, meta.episodes_map[1])
    assert len(builder.episodes) == 1
    assert builder.episode_index_by_sequence == {"a": 0}


def test_builder_ignores_the_video_features(lerobot_dataset_factory):
    """They are dropped: a dataset with video features merges with one without."""
    with_video = LeRobotDatasetMeta.load(lerobot_dataset_factory(with_video=True))
    without_video = LeRobotDatasetMeta.load(
        lerobot_dataset_factory(name="no_video", with_video=False)
    )
    builder = LeRobotDatasetBuilder()

    builder.add_episode("a", with_video.info, with_video.episodes_map[0])
    builder.add_episode("b", without_video.info, without_video.episodes_map[0])

    assert len(builder.episodes) == 2
    assert builder.info is not None
    assert VIDEO_COLUMN_NAME not in [f.key for f in builder.info.features]
