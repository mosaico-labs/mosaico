import pytest
from typer.testing import CliRunner

from mosaicolabs_cli.main import app

runner = CliRunner()


class TestAppEntrypoint:
    def test_no_args_shows_help(self):
        result = runner.invoke(app, [])
        assert result.exit_code == 0 or result.exit_code == 2
        assert "Usage" in result.output or "Mosaico CLI" in result.output

    def test_help_flag(self):
        result = runner.invoke(app, ["--help"])
        assert result.exit_code == 0
        assert "Mosaico CLI" in result.output
        assert "profile" in result.output
        assert "sequence" in result.output
        assert "topic" in result.output
        assert "extension" in result.output

    def test_unknown_command(self, monkeypatch, tmp_path):
        monkeypatch.setenv("MOSAICO_CONFIG_PATH", str(tmp_path / "c.toml"))
        monkeypatch.setenv("PATH", str(tmp_path))
        result = runner.invoke(app, ["nonexistent-command-xyz"])
        assert result.exit_code != 0


class TestMainCallback:
    def test_profile_flag_propagates(self, monkeypatch, tmp_path):
        config_path = tmp_path / "config.toml"
        config_path.write_text(
            '[testprofile]\nhost = "callback.test"\nport = "6276"\ndefault = false\n'
        )
        monkeypatch.setenv("MOSAICO_CONFIG_PATH", str(config_path))
        monkeypatch.delenv("MOSAICO_DAEMON_URL", raising=False)
        monkeypatch.delenv("MOSAICO_API_KEY", raising=False)
        monkeypatch.delenv("MOSAICO_TLS", raising=False)
        monkeypatch.delenv("MOSAICO_CERT_PATH", raising=False)

        result = runner.invoke(app, ["--profile", "testprofile", "profile", "ls"])
        assert result.exit_code == 0

    def test_profile_subcommand_does_not_require_connection(
        self, monkeypatch, tmp_path
    ):
        monkeypatch.setenv("MOSAICO_CONFIG_PATH", str(tmp_path / "empty.toml"))
        monkeypatch.delenv("MOSAICO_DAEMON_URL", raising=False)
        monkeypatch.delenv("MOSAICO_API_KEY", raising=False)
        monkeypatch.delenv("MOSAICO_TLS", raising=False)
        monkeypatch.delenv("MOSAICO_CERT_PATH", raising=False)

        result = runner.invoke(app, ["profile", "ls"])
        assert result.exit_code == 0


class TestSubcommandHelp:
    @pytest.mark.parametrize(
        "subcommand",
        ["profile", "sequence", "topic", "extension"],
    )
    def test_subcommand_help(self, subcommand):
        result = runner.invoke(app, [subcommand, "--help"])
        assert result.exit_code == 0
        assert "ls" in result.output or "Usage" in result.output
