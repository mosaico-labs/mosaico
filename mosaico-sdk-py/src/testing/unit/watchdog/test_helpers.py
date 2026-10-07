from pathlib import Path

import pytest

from mosaicolabs import SessionLevelErrorPolicy, TopicLevelErrorPolicy
from mosaicolabs.bridges.mcap import MCAPInjectionConfig, MCAPInjector
from mosaicolabs.bridges.ros import RosbagInjector, ROSInjectionConfig
from mosaicolabs.watchdog.file_source import FileRef
from mosaicolabs.watchdog.helpers import (
    create_mcap_configs,
    create_ros_configs,
    instantiate_injector,
    sequence_name_from_fileref,
)

CONN_KWARGS = dict(
    path=Path("/data/run/a.mcap"),
    sequence_name="run-a__mcap",
    host="mosaico.local",
    port=1234,
    mosaico_api_key="key",
    tls_cert_path="/certs/ca.pem",
    enable_tls=True,
    log_level="DEBUG",
)


def ref(rel_path: str) -> FileRef:
    return FileRef(rel_path, "u", 0, 0)


class TestSequenceName:
    @pytest.mark.parametrize(
        "rel_path, expected",
        [
            ("drive.mcap", "drive__mcap"),
            ("run_01/drive.mcap", "run_01-drive__mcap"),
            ("a/b/c.db3", "a-b-c__db3"),
            ("x.bag", "x__bag"),
            # only the last dot separates the extension
            ("drive.v2.mcap", "drive_v2__mcap"),
            # unsupported characters become `_`
            ("my drive (1).mcap", "my_drive__1___mcap"),
            ("élan.mcap", "lan__mcap"),
            # leading `-`/`_` are dropped
            ("_hidden/x.mcap", "hidden-x__mcap"),
            ("-x.mcap", "x__mcap"),
            ("__x.mcap", "x__mcap"),
        ],
    )
    def test_names(self, rel_path, expected):
        assert sequence_name_from_fileref(ref(rel_path)) == expected

    def test_name_only_has_supported_chars(self):
        name = sequence_name_from_fileref(ref("a b/c#d!e?.mcap"))
        assert all(c.isascii() and (c.isalnum() or c in "-_") for c in name)
        assert name[0].isalnum()

    def test_same_extension_different_paths_differ(self):
        assert sequence_name_from_fileref(
            ref("a/x.mcap")
        ) != sequence_name_from_fileref(ref("b/x.mcap"))

    def test_extension_is_part_of_name(self):
        assert sequence_name_from_fileref(ref("x.mcap")) != sequence_name_from_fileref(
            ref("x.bag")
        )

    def test_known_collision(self):
        # Documented limitation: different paths can give the same name
        assert sequence_name_from_fileref(
            ref("a-b/c.mcap")
        ) == sequence_name_from_fileref(ref("a/b-c.mcap"))


@pytest.mark.parametrize(
    "factory, cfg_cls",
    [
        (create_ros_configs, ROSInjectionConfig),
        (create_mcap_configs, MCAPInjectionConfig),
    ],
    ids=["ros", "mcap"],
)
def test_create_configs(factory, cfg_cls):
    cfg = factory(**CONN_KWARGS)

    assert isinstance(cfg, cfg_cls)
    assert cfg.file_path == CONN_KWARGS["path"]
    assert cfg.sequence_name == CONN_KWARGS["sequence_name"]
    assert cfg.host == "mosaico.local"
    assert cfg.port == 1234
    assert cfg.mosaico_api_key == "key"
    assert cfg.tls_cert_path == "/certs/ca.pem"
    assert cfg.enable_tls is True
    assert cfg.log_level == "DEBUG"
    # Fixed Watchdog policy
    assert cfg.update_if_exists is False
    assert cfg.dry_run is False
    assert cfg.on_error is SessionLevelErrorPolicy.Delete
    assert cfg.topics_on_error is TopicLevelErrorPolicy.Raise


class TestInstantiateInjector:
    @pytest.fixture
    def captured(self, monkeypatch):
        """Replaces the injectors' __init__ (keeping class identity) to capture the config
        without touching logging or the network."""
        seen = {}

        def fake_init(self, config):
            seen["cls"] = type(self)
            seen["config"] = config

        monkeypatch.setattr(RosbagInjector, "__init__", fake_init)
        monkeypatch.setattr(MCAPInjector, "__init__", fake_init)
        return seen

    @pytest.mark.parametrize(
        "injector_cls, cfg_cls",
        [(RosbagInjector, ROSInjectionConfig), (MCAPInjector, MCAPInjectionConfig)],
        ids=["ros", "mcap"],
    )
    def test_builds_matching_config(self, captured, injector_cls, cfg_cls):
        injector = instantiate_injector(**CONN_KWARGS, injector_cls=injector_cls)

        assert type(injector) is injector_cls
        assert isinstance(captured["config"], cfg_cls)
        assert captured["config"].sequence_name == CONN_KWARGS["sequence_name"]

    def test_unknown_class_raises(self, captured):
        with pytest.raises(RuntimeError):
            instantiate_injector(**CONN_KWARGS, injector_cls=object)  # type: ignore[arg-type]
