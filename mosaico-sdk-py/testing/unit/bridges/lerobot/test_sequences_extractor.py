from typing import List, cast

import pytest

from mosaicolabs.bridges.lerobot.metadata import LeRobotDatasetMeta, LeRobotInfo
from mosaicolabs.bridges.lerobot.mosaico_loader import (
    LeRobotEpisodeStreamer,
    MosaicoToLeRobotLoader,
)
from mosaicolabs.bridges.lerobot.sequences_extractor import (
    ExtractionStatus,
    LeRobotExtractorConfig,
    LeRobotSequencesExtractor,
    lerobot_sequences_extractor,
)
from mosaicolabs.bridges.lerobot.ui_lerobot import LeRobotProgressManager
from testing.unit.bridges.lerobot.test_writer import (
    TASKS,
    _episode_indexes,
    _info,
    _streamers,
)


def _run_script(monkeypatch, tmp_path, status: ExtractionStatus) -> None:
    """Runs the script with `LeRobotSequencesExtractor.run` stubbed to return `status`."""
    monkeypatch.setattr(LeRobotSequencesExtractor, "run", lambda self: status)
    lerobot_sequences_extractor(["seq_a", "--lerobot-path", str(tmp_path / "out")])


def test_partial_extraction_exits_non_zero(monkeypatch, tmp_path):
    with pytest.raises(SystemExit) as exc_info:
        _run_script(monkeypatch, tmp_path, ExtractionStatus.PARTIAL)

    assert exc_info.value.code == 1


def test_completed_extraction_does_not_exit(monkeypatch, tmp_path):
    _run_script(monkeypatch, tmp_path, ExtractionStatus.COMPLETED)


# --- _write_dataset ---


class _FakeLoader:
    """Stands for a resolved `MosaicoToLeRobotLoader`: the parts `_write_dataset` uses."""

    def __init__(self, info: LeRobotInfo, streamers: List[LeRobotEpisodeStreamer]):
        self.info = info
        self.tasks = TASKS
        self.episodes = [s.episode_metadata for s in streamers]
        self._streamers = streamers

    def __iter__(self):
        return iter(self._streamers)


class _NoUi:
    """Stands for a `LeRobotProgressManager` showing nothing."""

    def __getattr__(self, name):
        return lambda *args, **kwargs: None


def test_write_dataset_stores_the_data_file_of_each_episode(tmp_path):
    """The `data/chunk_index` and `data/file_index` of `meta/episodes` are those of the data
    file each episode was written to: with a limit of 0 MB, every episode after the first
    one starts a new data file, and a new chunk after `chunks_size` files."""
    info = _info(chunks_size=2, data_files_size_in_mb=0)
    loader = _FakeLoader(info, _streamers([3, 2, 4]))
    extractor = LeRobotSequencesExtractor(
        LeRobotExtractorConfig(saving_path=tmp_path, sequence_names=[])
    )

    # Duck-typed: `_write_dataset` only uses what the fakes expose
    extractor._write_dataset(
        cast(MosaicoToLeRobotLoader, loader),
        tmp_path,
        cast(LeRobotProgressManager, _NoUi()),
    )

    meta = LeRobotDatasetMeta.load(tmp_path)
    episodes = [meta.episodes_map[i] for i in range(3)]
    assert [(e.data_chunk_index, e.data_file_index) for e in episodes] == [
        (0, 0),
        (0, 1),
        (1, 0),
    ]
    for episode in episodes:
        data_path = info.get_path_to_data(
            tmp_path, episode.data_chunk_index, episode.data_file_index
        )
        assert set(_episode_indexes(data_path)) == {episode.episode_index}


@pytest.mark.parametrize(
    "source, config, expected",
    [
        ((None, None), (None, None), (1000, 100)),  # LeRobot's defaults
        ((2, 50), (None, None), (2, 50)),  # From the source dataset
        ((2, 50), (5, 70), (5, 70)),  # From the config
    ],
    ids=["defaults", "source", "config"],
)
def test_write_dataset_limits_of_the_data_files(tmp_path, source, config, expected):
    """`chunks_size` and `data_files_size_in_mb` come from the config, else from the source
    dataset, else from LeRobot's defaults, and are written to `meta/info.json`."""
    info = _info(chunks_size=source[0], data_files_size_in_mb=source[1])
    loader = _FakeLoader(info, _streamers([3, 2]))
    extractor = LeRobotSequencesExtractor(
        LeRobotExtractorConfig(
            saving_path=tmp_path,
            sequence_names=[],
            chunks_size=config[0],
            data_files_size_in_mb=config[1],
        )
    )

    extractor._write_dataset(
        cast(MosaicoToLeRobotLoader, loader),
        tmp_path,
        cast(LeRobotProgressManager, _NoUi()),
    )

    meta = LeRobotDatasetMeta.load(tmp_path)
    assert (meta.info.chunks_size, meta.info.data_files_size_in_mb) == expected


@pytest.mark.parametrize("name", ["chunks_size", "data_files_size_in_mb"])
def test_config_rejects_a_limit_that_is_not_positive(tmp_path, name):
    with pytest.raises(ValueError, match=f"`{name}` must be greater than 0"):
        LeRobotExtractorConfig(saving_path=tmp_path, sequence_names=[], **{name: 0})
