"""
Tests of `MosaicoToLeRobotLoader` against a Mosaico server: the episodes of the example LeRobot
dataset are ingested by `LeRobotInjector`, one sequence each, then read back as the episodes
of the exported dataset.
"""

from typing import Iterator

import pytest

from mosaicolabs.bridges.base_injector import InjectionStatus
from mosaicolabs.bridges.lerobot import LeRobotInjectionConfig, LeRobotInjector
from mosaicolabs.bridges.lerobot.mosaico_loader import MosaicoToLeRobotLoader
from testing.integration.config import UPLOADED_SEQUENCE_NAME
from testing.unit.bridges.lerobot.config import EPISODE_LENGTHS, VIDEO_COLUMN_NAME

SEQUENCE_PREFIX = "lerobot_export"

MISSING_SEQUENCE_NAME = f"{SEQUENCE_PREFIX}_missing"
"""A sequence that does not exist on the server."""

TOPIC_NAMES = [
    "/timeseries",
    "/observation/images/front",
    "/observation/images/wrist",
]


def _sequence_name(episode_index: int) -> str:
    """The sequence an episode of the example dataset is ingested as."""
    return f"{SEQUENCE_PREFIX}_episode_{episode_index:06d}"


def _delete_sequences(mosaico_client):
    for episode_index in range(len(EPISODE_LENGTHS)):
        sequence_name = _sequence_name(episode_index)
        if mosaico_client.sequence_exists(sequence_name):
            mosaico_client.sequence_delete(sequence_name)


@pytest.fixture
def ingested_dataset(
    host,
    port,
    api_key_manage,
    with_tls,
    tls_cert_path,
    mosaico_client,
    lerobot_dataset_factory,
) -> Iterator[None]:
    """Ingests the example LeRobot dataset, with a video feature, one sequence per episode.
    Its sequences are deleted before and after the test."""
    _delete_sequences(mosaico_client)
    config = LeRobotInjectionConfig(
        file_path=lerobot_dataset_factory(with_video=True),
        sequence_name=SEQUENCE_PREFIX,
        host=host,
        port=port,
        mosaico_api_key=api_key_manage,
        tls_cert_path=tls_cert_path,
        enable_tls=with_tls,
    )
    assert LeRobotInjector(config).run() is InjectionStatus.COMPLETED

    yield

    _delete_sequences(mosaico_client)


def test_loader_renumbers_the_episodes_in_the_order_of_the_sequences(
    mosaico_client, ingested_dataset
):
    """The episodes are exported in the order of the sequences, not of their source dataset,
    and the video feature, whose data is not ingested, is dropped."""
    names = [_sequence_name(2), _sequence_name(0)]
    lengths = [EPISODE_LENGTHS[2], EPISODE_LENGTHS[0]]

    with MosaicoToLeRobotLoader(mosaico_client, names) as loader:
        assert [
            (e.episode_index, e.length, e.dataset_from_index, e.dataset_to_index)
            for e in loader.episodes
        ] == [
            (0, lengths[0], 0, lengths[0]),
            (1, lengths[1], lengths[0], sum(lengths)),
        ]
        assert loader.episode_index_by_sequence == {names[0]: 0, names[1]: 1}
        assert (loader.info.total_episodes, loader.info.total_frames) == (
            2,
            sum(lengths),
        )
        assert loader.info.splits == {"train": "0:2"}
        assert VIDEO_COLUMN_NAME not in [f.key for f in loader.info.features]
        assert sorted(loader.topics) == sorted(TOPIC_NAMES)


def test_loader_drops_the_sequences_that_cannot_be_exported(
    mosaico_client, ingested_dataset, inject_synthetic_sequence
):
    """A missing sequence, and one with no `_lerobot_` metadata, are dropped."""
    names = [
        MISSING_SEQUENCE_NAME,
        UPLOADED_SEQUENCE_NAME,
        _sequence_name(0),
        _sequence_name(1),
    ]

    with MosaicoToLeRobotLoader(mosaico_client, names) as loader:
        assert loader.episode_index_by_sequence == {
            _sequence_name(0): 0,
            _sequence_name(1): 1,
        }
        assert [e.length for e in loader.episodes] == EPISODE_LENGTHS[:2]


def test_loader_raises_with_no_episode(mosaico_client):
    loader = MosaicoToLeRobotLoader(mosaico_client, [MISSING_SEQUENCE_NAME])

    with pytest.raises(RuntimeError, match="No episode to export"):
        _ = loader.info
