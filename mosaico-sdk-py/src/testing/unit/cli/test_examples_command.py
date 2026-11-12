import csv
import re
import types

import pytest
from click.testing import CliRunner as ClickRunner
from typer.testing import CliRunner

import mosaicolabs_cli.commands.examples as examples_cmd
from mosaicolabs.examples import cli as legacy_cli, config
from mosaicolabs_cli.main import app

runner = CliRunner()

ANSI_ESCAPE = re.compile(r"\x1b\[[0-9;]*m")


def plain(result, stream="output"):
    """Command output without colour codes (rich colours it when a CI runner forces colour)."""
    return ANSI_ESCAPE.sub("", getattr(result, stream))


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
    monkeypatch.setenv("MOSAICO_DAEMON_URL", "examples.test:1234")
    monkeypatch.setenv("MOSAICO_API_KEY", "secret")


@pytest.fixture(autouse=True)
def restore_config(monkeypatch):
    """The runner writes into the shared config module; undo it after each test."""
    for name in (
        "MOSAICO_HOST",
        "MOSAICO_PORT",
        "ENABLE_TLS",
        "API_KEY",
        "LOG_LEVEL",
    ):
        monkeypatch.setattr(config, name, getattr(config, name))


class FakeImport:
    """Stands in for import_module, recording the config seen at import time."""

    def __init__(self, main=None, has_main=True, import_error=None):
        self.calls = []
        self.config_at_import = None
        self.main_calls = 0
        self._main = main
        self._has_main = has_main
        self._import_error = import_error

    def __call__(self, module_path):
        self.calls.append(module_path)
        self.config_at_import = {
            "host": config.MOSAICO_HOST,
            "port": config.MOSAICO_PORT,
            "tls": config.ENABLE_TLS,
            "api_key": config.API_KEY,
            "log_level": config.LOG_LEVEL,
        }
        if self._import_error is not None:
            raise self._import_error
        module = types.ModuleType(module_path)
        if self._has_main:

            def main():
                self.main_calls += 1
                if self._main:
                    self._main()

            module.main = main
        return module


@pytest.fixture
def fake_import(monkeypatch):
    def install(**kwargs):
        fake = FakeImport(**kwargs)
        monkeypatch.setattr(examples_cmd, "import_module", fake)
        monkeypatch.setattr(legacy_cli, "import_module", fake)
        return fake

    return install


class TestHelp:
    def test_group_listed_in_main_help(self):
        result = runner.invoke(app, ["--help"])
        assert result.exit_code == 0
        assert "examples" in plain(result)

    def test_every_example_is_a_subcommand(self):
        result = runner.invoke(app, ["examples", "--help"])
        assert result.exit_code == 0
        for name in [*examples_cmd.EXAMPLES, "ls"]:
            assert name in plain(result)

    def test_example_help_needs_no_profile(self, no_profile_env):
        result = runner.invoke(app, ["examples", "data_inspection", "--help"])
        assert result.exit_code == 0
        assert "--log" in plain(result)


class TestList:
    def test_csv_lists_every_example(self, no_profile_env):
        result = runner.invoke(app, ["examples", "ls", "--output", "csv"])
        assert result.exit_code == 0
        rows = list(csv.reader(plain(result).strip().splitlines()))
        assert [row[0] for row in rows] == list(examples_cmd.EXAMPLES)
        # Descriptions may contain commas, so every row must still parse as two fields
        assert all(len(row) == 2 for row in rows)
        assert dict(rows)["mujoco_vis"] == examples_cmd.EXAMPLES["mujoco_vis"].help

    def test_table_output(self, no_profile_env):
        result = runner.invoke(app, ["examples", "ls", "--output", "table"])
        assert result.exit_code == 0
        assert "ros_injection" in plain(result)


class TestRun:
    def test_profile_is_written_to_config_before_import(self, profile_env, fake_import):
        fake = fake_import()
        result = runner.invoke(app, ["examples", "query_catalogs", "--log", "DEBUG"])
        assert result.exit_code == 0, result.output
        assert fake.calls == ["mosaicolabs.examples.query_catalogs"]
        assert fake.config_at_import == {
            "host": "examples.test",
            "port": 1234,
            "tls": False,
            "api_key": "secret",
            "log_level": "DEBUG",
        }
        assert fake.main_calls == 1

    def test_short_log_option(self, profile_env, fake_import):
        fake = fake_import()
        result = runner.invoke(app, ["examples", "data_inspection", "-l", "warning"])
        assert result.exit_code == 0, result.output
        assert fake.config_at_import["log_level"] == "WARNING"

    def test_tls_from_profile(self, profile_env, monkeypatch, fake_import):
        monkeypatch.setenv("MOSAICO_TLS", "1")
        fake = fake_import()
        result = runner.invoke(app, ["examples", "data_inspection"])
        assert result.exit_code == 0, result.output
        assert fake.config_at_import["tls"] is True

    def test_api_key_is_never_printed(self, profile_env, fake_import):
        fake_import()
        result = runner.invoke(app, ["examples", "data_inspection"])
        assert "secret" not in plain(result)

    def test_cert_path_is_reported_as_ignored(
        self, profile_env, monkeypatch, tmp_path, fake_import
    ):
        monkeypatch.setenv("MOSAICO_TLS", "1")
        monkeypatch.setenv("MOSAICO_CERT_PATH", str(tmp_path / "ca.pem"))
        fake = fake_import()
        result = runner.invoke(app, ["examples", "data_inspection"])
        assert result.exit_code == 0, result.output
        assert "certificate" in plain(result, "stderr")
        assert "certificate" not in plain(result, "stdout")
        assert fake.main_calls == 1

    def test_no_profile_exits_before_import(self, no_profile_env, fake_import):
        fake = fake_import()
        result = runner.invoke(app, ["examples", "data_inspection"])
        assert result.exit_code == 1
        assert "No default profile" in plain(result)
        assert fake.calls == []

    def test_example_without_main(self, profile_env, fake_import):
        fake_import(has_main=False)
        result = runner.invoke(app, ["examples", "data_inspection"])
        assert result.exit_code == 1
        assert "No 'main' function" in plain(result)

    def test_failing_import(self, profile_env, fake_import):
        fake_import(import_error=ModuleNotFoundError("No module named 'mujoco'"))
        result = runner.invoke(app, ["examples", "mujoco_vis"])
        assert result.exit_code == 1
        assert result.exception is None or isinstance(result.exception, SystemExit)
        assert "No module named 'mujoco'" in plain(result)

    def test_interrupted_import(self, profile_env, fake_import):
        fake_import(import_error=KeyboardInterrupt())
        result = runner.invoke(app, ["examples", "data_inspection"])
        assert result.exit_code == 130

    def test_failing_example(self, profile_env, fake_import):
        def boom():
            raise RuntimeError("server unreachable")

        fake_import(main=boom)
        result = runner.invoke(app, ["examples", "data_inspection"])
        assert result.exit_code == 1
        assert "server unreachable" in plain(result)

    def test_interrupted_example(self, profile_env, fake_import):
        def interrupt():
            raise KeyboardInterrupt

        fake_import(main=interrupt)
        result = runner.invoke(app, ["examples", "data_inspection"])
        assert result.exit_code == 130


class TestDeprecatedScript:
    """`mosaicolabs.examples` stays as a deprecated alias for one release."""

    def test_same_examples_as_the_cli(self):
        assert legacy_cli.EXAMPLES_MAP == {
            name: example.module for name, example in examples_cmd.EXAMPLES.items()
        }

    def test_still_runs_and_warns(self, fake_import):
        fake = fake_import()
        result = ClickRunner().invoke(
            legacy_cli.run_example_cli,
            ["data_inspection", "--host", "old.test", "--port", "99", "--api-key", "k"],
        )
        assert result.exit_code == 0, result.output
        assert "deprecated" in plain(result)
        assert "mosaico examples data_inspection" in plain(result)
        assert fake.config_at_import["host"] == "old.test"
        assert fake.config_at_import["port"] == 99
        assert fake.config_at_import["api_key"] == "k"
        assert fake.main_calls == 1

    def test_target_line_separates_flags(self, fake_import):
        fake_import()
        result = ClickRunner().invoke(
            legacy_cli.run_example_cli,
            ["data_inspection", "--tls", "--api-key", "k"],
        )
        assert "localhost:6726 --tls --api-key" in plain(result)
