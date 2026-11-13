from contextlib import contextmanager
from types import SimpleNamespace

import pytest

import mosaicolabs.bridges.lerobot.injector as injector_module
from mosaicolabs.bridges.base_injector import InjectionStatus
from mosaicolabs.bridges.lerobot import LeRobotInjectionConfig, LeRobotInjector

from .config import EPISODE_LENGTHS, lengthen_first_episode


@pytest.fixture
def offline(monkeypatch):
    """Runs the injector without a server: the client only reports its batch size, and
    every episode that passes the validation is reported as uploaded."""

    @contextmanager
    def connect(**kwargs):
        yield SimpleNamespace(max_batch_size_bytes=4 * 1024 * 1024)

    monkeypatch.setattr(injector_module.MosaicoClient, "connect", connect)
    monkeypatch.setattr(
        LeRobotInjector, "_upload_episode", lambda self, mclient, loader, ui: True
    )


def _injector(root) -> LeRobotInjector:
    return LeRobotInjector(LeRobotInjectionConfig(file_path=root, sequence_name="seq"))


def test_failed_episodes_is_empty_before_a_run(lerobot_dataset):
    assert _injector(lerobot_dataset).failed_episodes == []


def test_no_failed_episodes_when_every_episode_is_ingested(offline, lerobot_dataset):
    injector = _injector(lerobot_dataset)

    assert injector.run() is InjectionStatus.COMPLETED
    assert injector.failed_episodes == []


def test_failed_episodes_lists_the_skipped_episode(offline, lerobot_dataset_factory):
    root = lerobot_dataset_factory()
    lengthen_first_episode(root)
    injector = _injector(root)

    assert injector.run() is InjectionStatus.PARTIAL
    assert injector.failed_episodes == [0]


def test_failed_episodes_lists_an_episode_that_failed_to_upload(
    monkeypatch, offline, lerobot_dataset
):
    monkeypatch.setattr(
        LeRobotInjector,
        "_upload_episode",
        lambda self, mclient, loader, ui: loader.episode_index != 1,
    )
    injector = _injector(lerobot_dataset)

    assert injector.run() is InjectionStatus.PARTIAL
    assert injector.failed_episodes == [1]


def test_run_raises_when_every_episode_fails(monkeypatch, offline, lerobot_dataset):
    monkeypatch.setattr(
        LeRobotInjector, "_upload_episode", lambda self, mclient, loader, ui: False
    )
    injector = _injector(lerobot_dataset)

    with pytest.raises(RuntimeError):
        injector.run()
    assert injector.failed_episodes == list(range(len(EPISODE_LENGTHS)))
