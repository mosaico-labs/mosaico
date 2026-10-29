from dataclasses import replace

import pytest

from mosaicolabs.bridges.lerobot.metadata import LeRobotDatasetMeta, LeRobotFeature
from mosaicolabs.bridges.lerobot.mosaico_loader import MosaicoToLeRobotLoader

from .config import EPISODE_LENGTHS, VIDEO_COLUMN_NAME
from .mosaico_fakes import TOPICS, FakeClient, FakeSequenceHandler, lerobot_sequence


def test_loader_merges_and_renumbers(lerobot_dataset_factory):
    # Two source datasets, the first one with a video feature, both with an episode 0
    a = LeRobotDatasetMeta.load(lerobot_dataset_factory(name="a", with_video=True))
    b = LeRobotDatasetMeta.load(lerobot_dataset_factory(name="b"))
    sequences = [
        lerobot_sequence("a_0", a.info, a.episodes_map[0]),
        lerobot_sequence("b_0", b.info, b.episodes_map[0]),
        lerobot_sequence("a_2", a.info, a.episodes_map[2]),
    ]

    loader = MosaicoToLeRobotLoader(FakeClient(sequences), ["a_0", "b_0", "a_2"])

    lengths = [EPISODE_LENGTHS[0], EPISODE_LENGTHS[0], EPISODE_LENGTHS[2]]
    assert [
        (e.episode_index, e.dataset_from_index, e.dataset_to_index)
        for e in loader.episodes
    ] == [
        (0, 0, lengths[0]),
        (1, lengths[0], lengths[0] + lengths[1]),
        (2, lengths[0] + lengths[1], sum(lengths)),
    ]
    assert loader.episode_index_by_sequence == {"a_0": 0, "b_0": 1, "a_2": 2}
    assert (loader.info.total_episodes, loader.info.total_frames) == (3, sum(lengths))
    assert loader.info.splits == {"train": "0:3"}
    assert VIDEO_COLUMN_NAME not in [f.key for f in loader.info.features]
    assert loader.topics == TOPICS


def test_loader_drops_the_sequences_that_cannot_be_exported(lerobot_dataset):
    meta = LeRobotDatasetMeta.load(lerobot_dataset)
    other_robot = replace(meta.info, robot_type="other_robot")
    sequences = [
        FakeSequenceHandler("plain", {"operator": "x"}),
        lerobot_sequence("a_0", meta.info, meta.episodes_map[0]),
        lerobot_sequence("other_robot", other_robot, meta.episodes_map[1]),
        lerobot_sequence("a_1", meta.info, meta.episodes_map[1]),
    ]

    loader = MosaicoToLeRobotLoader(
        FakeClient(sequences), ["missing", "plain", "a_0", "other_robot", "a_1"]
    )

    assert loader.episode_index_by_sequence == {"a_0": 0, "a_1": 1}
    assert [e.length for e in loader.episodes] == EPISODE_LENGTHS[:2]
    # The handlers of the dropped sequences are closed, the ones of the accepted stay open
    assert {s.name: s.closed for s in sequences} == {
        "plain": True,
        "a_0": False,
        "other_robot": True,
        "a_1": False,
    }


def test_loader_raises_with_no_episode():
    loader = MosaicoToLeRobotLoader(FakeClient([]), ["missing"])

    with pytest.raises(ValueError, match="No episode to export"):
        _ = loader.info


def test_loader_keeps_a_feature_prefix_of_another(lerobot_dataset):
    # The features are stored as a JSON string: `a.b` is not merged into `a`
    meta = LeRobotDatasetMeta.load(lerobot_dataset)
    features = [
        LeRobotFeature.from_dict("a", {"dtype": "float32", "shape": [1]}),
        LeRobotFeature.from_dict("a.b", {"dtype": "float32", "shape": [1]}),
    ]
    info = replace(meta.info, features=features)

    loader = MosaicoToLeRobotLoader(
        FakeClient([lerobot_sequence("s", info, meta.episodes_map[0])]), ["s"]
    )

    assert [f.key for f in loader.info.features] == ["a", "a.b"]


def test_loader_closes_the_handlers_on_exit(lerobot_dataset):
    meta = LeRobotDatasetMeta.load(lerobot_dataset)
    sequences = [
        lerobot_sequence(f"a_{index}", meta.info, episode)
        for index, episode in meta.episodes_map.items()
    ]

    with MosaicoToLeRobotLoader(FakeClient(sequences), [s.name for s in sequences]):
        assert not any(s.closed for s in sequences)

    assert all(s.closed for s in sequences)
