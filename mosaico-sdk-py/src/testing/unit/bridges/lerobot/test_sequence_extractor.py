from dataclasses import replace
from types import SimpleNamespace

import pytest

from mosaicolabs.bridges.lerobot.metadata import LeRobotDatasetMeta, LeRobotFeature
from mosaicolabs.bridges.lerobot.schema_metadata import LeRobotSchemaMetadata
from mosaicolabs.bridges.lerobot.sequence_extractor import (
    LeRobotExtractorConfig,
    LeRobotSequenceExtractor,
    MosaicoClient,
)

from .config import EPISODE_LENGTHS, VIDEO_COLUMN_NAME


class _FakeClient:
    """Stands for a connected `MosaicoClient`: returns the given sequences by name."""

    def __init__(self, sequences):
        self._sequences = {sequence.name: sequence for sequence in sequences}

    def sequence_handler(self, sequence_name):
        return self._sequences.get(sequence_name)

    def __enter__(self):
        return self

    def __exit__(self, *exc_info):
        return False


def _sequence(name, info, episode):
    """A sequence as ingested by the LeRobot bridge (see `LeRobotInjector`)."""
    meta = LeRobotSchemaMetadata(info=info.to_dict(), episode=episode.to_dict())
    return SimpleNamespace(name=name, user_metadata=meta.merge_into({"operator": "x"}))


def _extractor(saving_path, sequence_names, **kwargs):
    config = LeRobotExtractorConfig(
        saving_path=saving_path, sequence_names=sequence_names, **kwargs
    )
    return LeRobotSequenceExtractor(config)


def test_extractor_merges_and_renumbers(lerobot_dataset_factory, tmp_path):
    # Two source datasets, the first one with a video feature, both with an episode 0
    a = LeRobotDatasetMeta.load(lerobot_dataset_factory(name="a", with_video=True))
    b = LeRobotDatasetMeta.load(lerobot_dataset_factory(name="b"))
    sequences = [
        _sequence("a_0", a.info, a.episodes_map[0]),
        _sequence("b_0", b.info, b.episodes_map[0]),
        _sequence("a_2", a.info, a.episodes_map[2]),
    ]
    extractor = _extractor(tmp_path / "out", ["a_0", "b_0", "a_2"])

    meta = extractor._build_metadata(_FakeClient(sequences))

    lengths = [EPISODE_LENGTHS[0], EPISODE_LENGTHS[0], EPISODE_LENGTHS[2]]
    assert [
        (e.episode_index, e.dataset_from_index, e.dataset_to_index)
        for e in meta.episodes_map.values()
    ] == [
        (0, 0, lengths[0]),
        (1, lengths[0], lengths[0] + lengths[1]),
        (2, lengths[0] + lengths[1], sum(lengths)),
    ]
    assert (meta.info.total_episodes, meta.info.total_frames) == (3, sum(lengths))
    assert meta.info.splits == {"train": "0:3"}
    assert VIDEO_COLUMN_NAME not in [f.key for f in meta.features]


def test_extractor_drops_the_sequences_that_cannot_be_exported(
    lerobot_dataset, tmp_path
):
    meta = LeRobotDatasetMeta.load(lerobot_dataset)
    other_robot = replace(meta.info, robot_type="other_robot")
    sequences = [
        SimpleNamespace(name="plain", user_metadata={"operator": "x"}),
        _sequence("a_0", meta.info, meta.episodes_map[0]),
        _sequence("other_robot", other_robot, meta.episodes_map[1]),
        _sequence("a_1", meta.info, meta.episodes_map[1]),
    ]
    extractor = _extractor(
        tmp_path / "out", ["missing", "plain", "a_0", "other_robot", "a_1"]
    )

    exported = extractor._build_metadata(_FakeClient(sequences))

    assert [(e.episode_index, e.length) for e in exported.episodes_map.values()] == [
        (0, EPISODE_LENGTHS[0]),
        (1, EPISODE_LENGTHS[1]),
    ]


def test_extractor_raises_with_no_episode(tmp_path):
    extractor = _extractor(tmp_path / "out", ["missing"])

    with pytest.raises(ValueError, match="No episode to export"):
        extractor._build_metadata(_FakeClient([]))


def test_extractor_keeps_a_feature_prefix_of_another(lerobot_dataset, tmp_path):
    # The features are stored as a JSON string: `a.b` is not merged into `a`
    meta = LeRobotDatasetMeta.load(lerobot_dataset)
    features = [
        LeRobotFeature.from_dict("a", {"dtype": "float32", "shape": [1]}),
        LeRobotFeature.from_dict("a.b", {"dtype": "float32", "shape": [1]}),
    ]
    info = replace(meta.info, features=features)
    extractor = _extractor(tmp_path / "out", ["s"])

    exported = extractor._build_metadata(
        _FakeClient([_sequence("s", info, meta.episodes_map[0])])
    )

    assert [f.key for f in exported.features] == ["a", "a.b"]


def test_extractor_run_writes_the_metadata(lerobot_dataset, tmp_path, monkeypatch):
    meta = LeRobotDatasetMeta.load(lerobot_dataset)
    sequences = [
        _sequence(f"episode_{index}", meta.info, episode)
        for index, episode in meta.episodes_map.items()
    ]
    monkeypatch.setattr(
        MosaicoClient, "connect", lambda **kwargs: _FakeClient(sequences)
    )
    saving_path = tmp_path / "out"

    exported = _extractor(saving_path, [s.name for s in sequences]).run()

    assert LeRobotDatasetMeta.load(saving_path) == exported
    assert exported.n_episodes == len(EPISODE_LENGTHS)


def test_extractor_run_refuses_an_existing_folder(tmp_path, monkeypatch):
    def connect(**kwargs):
        raise AssertionError("The extractor should not connect")

    monkeypatch.setattr(MosaicoClient, "connect", connect)
    saving_path = tmp_path / "out"
    saving_path.mkdir()

    with pytest.raises(FileExistsError, match="already exists"):
        _extractor(saving_path, ["s"]).run()
