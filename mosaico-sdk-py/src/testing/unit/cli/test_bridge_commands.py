import json
from pathlib import Path

import pytest
from rosbags.rosbag2 import StoragePlugin
from rosbags.typesys import Stores
from typer.testing import CliRunner

import mosaicolabs.bridges.mcap as mcap_bridge
import mosaicolabs.bridges.ros as ros_bridge
from mosaicolabs.bridges.base_injector import InjectionStatus
from mosaicolabs_cli.main import app

runner = CliRunner()

CONNECTION_ENV = (
    "MOSAICO_DAEMON_URL",
    "MOSAICO_API_KEY",
    "MOSAICO_TLS",
    "MOSAICO_CERT_PATH",
    "MOSAICO_PROFILE",
)


@pytest.fixture
def no_profile_env(monkeypatch, tmp_path):
    monkeypatch.setenv("MOSAICO_CONFIG_PATH", str(tmp_path / "empty.toml"))
    for var in CONNECTION_ENV:
        monkeypatch.delenv(var, raising=False)


@pytest.fixture
def profile_env(no_profile_env, monkeypatch):
    monkeypatch.setenv("MOSAICO_DAEMON_URL", "bridge.test:1234")
    monkeypatch.setenv("MOSAICO_API_KEY", "secret")


@pytest.fixture
def input_file(tmp_path):
    path = tmp_path / "recording.mcap"
    path.write_bytes(b"")
    return path


class _FakeRunner:
    """Records the config it was built with instead of touching a file or server."""

    configs: list = []
    outcome = None

    def __init__(self, config):
        type(self).configs.append(config)

    def run(self):
        if isinstance(type(self).outcome, BaseException):
            raise type(self).outcome
        return type(self).outcome


@pytest.fixture
def fake(monkeypatch):
    _FakeRunner.configs = []
    _FakeRunner.outcome = None
    for module, name in [
        (ros_bridge, "RosbagInjector"),
        (ros_bridge, "ROSSequenceExtractor"),
        (mcap_bridge, "MCAPInjector"),
        (mcap_bridge, "MCAPSequenceExtractor"),
    ]:
        monkeypatch.setattr(module, name, _FakeRunner)
    return _FakeRunner


BRIDGES = ["ros", "mcap"]


class TestHelp:
    def test_groups_listed_in_main_help(self):
        result = runner.invoke(app, ["--help"])
        assert result.exit_code == 0
        for bridge in BRIDGES:
            assert bridge in result.output

    @pytest.mark.parametrize("bridge", BRIDGES)
    def test_group_help_lists_commands(self, bridge):
        result = runner.invoke(app, [bridge, "--help"])
        assert result.exit_code == 0
        assert "inject" in result.output
        assert "extract" in result.output

    @pytest.mark.parametrize("bridge", BRIDGES)
    @pytest.mark.parametrize("command", ["inject", "extract"])
    def test_command_help(self, no_profile_env, bridge, command):
        result = runner.invoke(app, [bridge, command, "--help"])
        assert result.exit_code == 0
        assert "Usage" in result.output


class TestRequiresProfile:
    @pytest.mark.parametrize("bridge", BRIDGES)
    def test_extract_without_profile_exits(self, no_profile_env, fake, bridge):
        result = runner.invoke(app, [bridge, "extract", "my_seq"])
        assert result.exit_code == 1
        assert fake.configs == []

    @pytest.mark.parametrize("bridge", BRIDGES)
    def test_inject_without_profile_exits(
        self, no_profile_env, fake, input_file, bridge
    ):
        result = runner.invoke(app, [bridge, "inject", str(input_file), "-n", "seq"])
        assert result.exit_code == 1
        assert fake.configs == []

    @pytest.mark.parametrize("bridge", BRIDGES)
    def test_inject_dry_run_needs_no_profile(
        self, no_profile_env, fake, input_file, bridge
    ):
        fake.outcome = InjectionStatus.DRY_RUN
        result = runner.invoke(
            app, [bridge, "inject", str(input_file), "-n", "seq", "--dry-run"]
        )
        assert result.exit_code == 0, result.output
        (cfg,) = fake.configs
        assert cfg.dry_run is True
        assert cfg.host == "localhost"


class TestRosInject:
    def test_defaults(self, profile_env, fake, input_file):
        fake.outcome = InjectionStatus.COMPLETED
        result = runner.invoke(app, ["ros", "inject", str(input_file), "-n", "seq"])
        assert result.exit_code == 0, result.output

        (cfg,) = fake.configs
        assert isinstance(cfg, ros_bridge.ROSInjectionConfig)
        assert cfg.file_path == input_file
        assert cfg.sequence_name == "seq"
        assert cfg.host == "bridge.test"
        assert cfg.port == 1234
        assert cfg.mosaico_api_key == "secret"
        assert cfg.ros_distro is Stores.EMPTY
        assert cfg.topics is None
        assert cfg.metadata == {"rosbag_injection": input_file.name}
        assert cfg.topic_metadata is None
        assert cfg.update_if_exists is False
        assert cfg.dry_run is False
        assert cfg.log_level == "INFO"

    def test_options(self, profile_env, fake, input_file, tmp_path):
        topic_md_file = tmp_path / "topic_md.json"
        topic_md_file.write_text(json.dumps({"/imu": {"unit": "rad/s"}}))

        result = runner.invoke(
            app,
            [
                "ros",
                "inject",
                str(input_file),
                "--name",
                "seq",
                "--ros-distro",
                "ros2_humble",
                "-t",
                "/cam/*",
                "--topics",
                "!/cam/debug*",
                "--metadata",
                '{"driver": "alice"}',
                "--topic-metadata",
                str(topic_md_file),
                "--update-if-exists",
                "--log",
                "warning",
            ],
        )
        assert result.exit_code == 0, result.output

        (cfg,) = fake.configs
        assert cfg.ros_distro is Stores.ROS2_HUMBLE
        assert cfg.topics == ["/cam/*", "!/cam/debug*"]
        assert cfg.metadata == {
            "driver": "alice",
            "rosbag_injection": input_file.name,
        }
        assert cfg.topic_metadata == {"/imu": {"unit": "rad/s"}}
        assert cfg.update_if_exists is True
        assert cfg.log_level == "WARNING"

    def test_invalid_metadata(self, profile_env, fake, input_file):
        result = runner.invoke(
            app,
            ["ros", "inject", str(input_file), "-n", "seq", "--metadata", "{nope"],
        )
        assert result.exit_code == 2
        assert fake.configs == []

    def test_missing_file(self, profile_env, fake, tmp_path):
        result = runner.invoke(
            app, ["ros", "inject", str(tmp_path / "missing.bag"), "-n", "seq"]
        )
        assert result.exit_code == 2
        assert fake.configs == []

    @pytest.mark.parametrize(
        "outcome, exit_code",
        [
            (RuntimeError("upload failed"), 1),
            (KeyboardInterrupt(), 130),
            (InjectionStatus.CANCELLED, 130),
        ],
    )
    def test_failure_exit_codes(
        self, profile_env, fake, input_file, outcome, exit_code
    ):
        fake.outcome = outcome
        result = runner.invoke(app, ["ros", "inject", str(input_file), "-n", "seq"])
        assert result.exit_code == exit_code


class TestRosExtract:
    def test_defaults(self, profile_env, fake):
        result = runner.invoke(app, ["ros", "extract", "my_seq"])
        assert result.exit_code == 0, result.output

        (cfg,) = fake.configs
        assert isinstance(cfg, ros_bridge.ROSExtractorConfig)
        assert cfg.sequence_name == "my_seq"
        assert cfg.saving_path == Path(".")
        assert cfg.host == "bridge.test"
        assert cfg.port == 1234
        assert cfg.mosaico_api_key == "secret"
        assert cfg.enable_tls is False
        assert cfg.tls_cert_path is None
        assert cfg.topics is None
        assert cfg.ros_distro is Stores.ROS2_JAZZY
        assert cfg.storage_plugin is StoragePlugin.MCAP
        assert cfg.log_level == "INFO"
        assert cfg.overwrite is False
        assert cfg.dry_run is False

    def test_options(self, profile_env, fake, tmp_path):
        result = runner.invoke(
            app,
            [
                "ros",
                "extract",
                "my_seq",
                "--rosbag-path",
                str(tmp_path),
                "--ros_distro",
                "ROS1_NOETIC",
                "--storage-plugin",
                "SQLITE3",
                "--topics",
                "/cam/*",
                "-t",
                "!/cam/debug*",
                "--start-timestamp-ns",
                "10",
                "--end_timestamp_ns",
                "20",
                "--overwrite",
                "--dry-run",
                "--log",
                "debug",
            ],
        )
        assert result.exit_code == 0, result.output

        (cfg,) = fake.configs
        assert cfg.saving_path == tmp_path
        assert cfg.ros_distro is Stores.ROS1_NOETIC
        assert cfg.storage_plugin is StoragePlugin.SQLITE3
        assert cfg.topics == ["/cam/*", "!/cam/debug*"]
        assert cfg.start_timestamp_ns == 10
        assert cfg.end_timestamp_ns == 20
        assert cfg.overwrite is True
        assert cfg.dry_run is True
        assert cfg.log_level == "DEBUG"

    def test_invalid_distro(self, profile_env, fake):
        result = runner.invoke(
            app, ["ros", "extract", "my_seq", "--ros-distro", "NOPE"]
        )
        assert result.exit_code != 0
        assert fake.configs == []


class TestMcapInject:
    def test_options(self, profile_env, fake, input_file):
        fake.outcome = InjectionStatus.COMPLETED
        result = runner.invoke(
            app,
            [
                "mcap",
                "inject",
                str(input_file),
                "-n",
                "seq",
                "-c",
                "front_car.*",
                "--channels",
                "!front_car.debug",
                "--metadata",
                '{"driver": "alice"}',
            ],
        )
        assert result.exit_code == 0, result.output

        (cfg,) = fake.configs
        assert isinstance(cfg, mcap_bridge.MCAPInjectionConfig)
        assert cfg.file_path == input_file
        assert cfg.sequence_name == "seq"
        assert cfg.host == "bridge.test"
        assert cfg.port == 1234
        assert cfg.channels == ["front_car.*", "!front_car.debug"]
        assert cfg.metadata == {"driver": "alice", "mcap_injection": input_file.name}


class TestMcapExtract:
    def test_options(self, profile_env, fake, tmp_path):
        result = runner.invoke(
            app,
            [
                "mcap",
                "extract",
                "my_seq",
                "--mcap_path",
                str(tmp_path),
                "--topics",
                "/imu",
                "--dry-run",
            ],
        )
        assert result.exit_code == 0, result.output

        (cfg,) = fake.configs
        assert isinstance(cfg, mcap_bridge.MCAPExtractorConfig)
        assert cfg.sequence_name == "my_seq"
        assert cfg.saving_path == tmp_path
        assert cfg.host == "bridge.test"
        assert cfg.port == 1234
        assert cfg.topics == ["/imu"]
        assert cfg.dry_run is True

    @pytest.mark.parametrize(
        "error", [FileExistsError("already exists"), ValueError("no such sequence")]
    )
    def test_expected_errors_exit_cleanly(self, profile_env, fake, error):
        fake.outcome = error
        result = runner.invoke(app, ["mcap", "extract", "my_seq"])
        assert result.exit_code == 1
        assert str(error) in result.output
