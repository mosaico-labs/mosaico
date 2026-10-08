import json
import os

import pytest
from typer.testing import CliRunner

from mosaicolabs_cli.main import app

runner = CliRunner()


@pytest.fixture
def config_file(tmp_path, monkeypatch):
    config_path = tmp_path / "config.toml"
    monkeypatch.setenv("MOSAICO_CONFIG_PATH", str(config_path))
    return config_path


def _add_profile(config_file):
    result = runner.invoke(
        app,
        [
            "profile",
            "add",
            "dev",
            "--no-interactive",
            "--host",
            "localhost",
        ],
    )
    assert result.exit_code == 0
    assert config_file.exists()


class TestDoctor:
    def test_json_output_is_redacted(self, config_file):
        result = runner.invoke(
            app,
            [
                "profile",
                "add",
                "dev",
                "--no-interactive",
                "--host",
                "localhost",
                "--api-key",
                "very-secret",
            ],
        )
        assert result.exit_code == 0

        result = runner.invoke(app, ["doctor", "--no-network", "--output", "json"])
        assert result.exit_code == 0
        payload = json.loads(result.output)
        assert payload["schema_version"] == 1
        assert payload["profile"]["api_key_configured"] is True
        assert "very-secret" not in result.output

    def test_missing_profile_is_an_error(self, config_file):
        result = runner.invoke(app, ["doctor", "--no-network", "--output", "json"])
        assert result.exit_code == 1
        payload = json.loads(result.output)
        assert payload["status"] == "error"
        assert any(check["name"] == "profile" for check in payload["checks"])

    @pytest.mark.skipif(os.name != "posix", reason="POSIX permissions only")
    def test_profile_write_restricts_permissions(self, config_file):
        _add_profile(config_file)
        assert config_file.stat().st_mode & 0o777 == 0o600

    @pytest.mark.skipif(os.name != "posix", reason="POSIX permissions only")
    def test_insecure_permissions_are_reported(self, config_file):
        _add_profile(config_file)
        config_file.chmod(0o644)

        result = runner.invoke(app, ["doctor", "--no-network", "--output", "json"])
        assert result.exit_code == 0
        payload = json.loads(result.output)
        assert payload["status"] == "warning"
        assert any(
            check["name"] == "config_permissions" and check["status"] == "warning"
            for check in payload["checks"]
        )
