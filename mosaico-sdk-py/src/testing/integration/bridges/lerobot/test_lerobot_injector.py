from dataclasses import replace
from typing import Iterator, List

import pytest

from mosaicolabs.bridges.base_injector import InjectionStatus
from mosaicolabs.bridges.lerobot.injector import (
    LeRobotInjectionConfig,
    LeRobotInjector,
)
from testing.unit.bridges.lerobot.config import (
    EPISODE_LENGTHS,
    lengthen_first_episode,
)

SEQUENCE_PREFIX = "lerobot_injection"

TOPIC_NAMES = [
    "/timeseries",
    "/observation/images/front",
    "/observation/images/wrist",
]


def _sequence_names() -> List[str]:
    """The sequences the episodes of the example dataset are ingested as."""
    return [
        f"{SEQUENCE_PREFIX}_episode_{episode_index:06d}"
        for episode_index in range(len(EPISODE_LENGTHS))
    ]


def _delete_sequences(mosaico_client):
    for sequence_name in _sequence_names():
        if mosaico_client.sequence_exists(sequence_name):
            mosaico_client.sequence_delete(sequence_name)


@pytest.fixture
def default_lerobot_injector_config(
    host, port, api_key_manage, with_tls, tls_cert_path, lerobot_dataset, mosaico_client
) -> Iterator[LeRobotInjectionConfig]:
    """The config ingesting the example LeRobot dataset, whose sequences are deleted before
    and after the test."""
    yield LeRobotInjectionConfig(
        file_path=lerobot_dataset,
        sequence_name=SEQUENCE_PREFIX,
        host=host,
        port=port,
        mosaico_api_key=api_key_manage,
        tls_cert_path=tls_cert_path,
        enable_tls=with_tls,
    )


def test_lerobot_injection_succeeds(mosaico_client, default_lerobot_injector_config):
    """Tests that the whole ingestion pipeline works: each episode is ingested as its own
    sequence"""

    status = LeRobotInjector(default_lerobot_injector_config).run()

    assert status is InjectionStatus.COMPLETED
    for sequence_name in _sequence_names():
        sequence_handler = mosaico_client.sequence_handler(sequence_name)
        assert sequence_handler is not None
        assert sorted(sequence_handler.topics) == sorted(TOPIC_NAMES)

    _delete_sequences(mosaico_client)


def test_lerobot_injection_deletes_a_failed_episode(
    mosaico_client, default_lerobot_injector_config, lerobot_dataset_factory
):
    """Tests that a failed episode is deleted (default `on_error` policy), without stopping
    the ingestion of the next ones"""
    root = lerobot_dataset_factory()
    lengthen_first_episode(root)

    status = LeRobotInjector(
        replace(default_lerobot_injector_config, file_path=root)
    ).run()

    assert status is InjectionStatus.COMPLETED
    first_sequence, *other_sequences = _sequence_names()
    assert mosaico_client.sequence_exists(first_sequence) is False
    assert all(mosaico_client.sequence_exists(name) for name in other_sequences)

    _delete_sequences(mosaico_client)
