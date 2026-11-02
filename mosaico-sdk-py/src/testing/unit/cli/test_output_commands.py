import csv
import io
import json
from types import SimpleNamespace
from unittest.mock import MagicMock

import pytest
from typer.testing import CliRunner

from mosaicolabs_cli.commands import doctor, extension, sequence, topic
from mosaicolabs_cli.main import app
from mosaicolabs_cli.output_format import OutputFormat
from mosaicolabs_cli.utils.mosaico_profile import MosaicoProfile

runner = CliRunner()


@pytest.fixture
def isolated_cli(tmp_path, monkeypatch):
    for key in [
        "MOSAICO_PROFILE",
        "MOSAICO_API_KEY",
        "MOSAICO_TLS",
        "MOSAICO_CERT_PATH",
        "MOSAICO_DAEMON_URL",
    ]:
        monkeypatch.delenv(key, raising=False)
    monkeypatch.setenv("MOSAICO_CONFIG_PATH", str(tmp_path / "config.toml"))
    monkeypatch.setenv("MOSAICO_DAEMON_URL", "localhost")
    monkeypatch.setattr(extension, "discover_extensions", lambda: {})
    client = MagicMock()
    client.query.return_value = []
    connect = MagicMock()
    connect.return_value.__enter__.return_value = client
    monkeypatch.setattr("mosaicolabs.MosaicoClient.connect", connect)
    return client


@pytest.mark.parametrize("command", ["profile", "extension", "sequence", "topic"])
@pytest.mark.parametrize("output", [None, "csv", "json", "jsonl", "table"])
def test_empty_listing_contract(isolated_cli, command, output):
    args = [command, "ls"] + (["--output", output] if output else [])
    result = runner.invoke(app, args)
    assert result.exit_code == 0, result.exception
    if output == "json":
        assert json.loads(result.stdout) == {"schema_version": 1, command + "s": []}
    elif output == "table":
        assert "No " in result.stdout
    else:
        assert result.stdout == ""
    assert result.stderr == ""


@pytest.mark.parametrize("command", ["sequence", "topic"])
@pytest.mark.parametrize("output", ["csv", "json", "jsonl", "table"])
def test_catalog_fields_and_csv_piping_contract(isolated_cli, command, output):
    client = isolated_cli
    client.query.return_value = [
        SimpleNamespace(
            sequence=SimpleNamespace(name="drive"),
            topics=[SimpleNamespace(name="/imu")],
        )
    ]
    handler = SimpleNamespace(
        created_timestamp=123,
        _timestamp_ns_min=10,
        _timestamp_ns_max=20,
        timestamp_ns_min=10,
        timestamp_ns_max=20,
        user_metadata={"vehicle": {"id": "abc"}},
    )
    client.sequence_handler.return_value = handler
    client.topic_handler.return_value = handler
    result = runner.invoke(app, [command, "ls", "--output", output])
    assert result.exit_code == 0, result.exception
    locator = "drive" if command == "sequence" else "drive/imu"
    expected = {"locator": locator, "timestamp_ns_min": "10", "timestamp_ns_max": "20"}
    if command == "sequence":
        expected.update(created_timestamp="123", user_metadata="vehicle.id=abc")
    if output == "json":
        assert json.loads(result.stdout) == {
            "schema_version": 1,
            command + "s": [expected],
        }
    elif output == "jsonl":
        assert [json.loads(line) for line in result.stdout.splitlines()] == [expected]
    elif output == "csv":
        assert result.stdout == f"{locator},10,20\n"
    else:
        assert "Mosaico" in result.stdout
    assert result.stderr == ""


@pytest.mark.parametrize("command,module", [("sequence", sequence), ("topic", topic)])
@pytest.mark.parametrize("output", ["csv", "json", "jsonl"])
def test_machine_output_does_not_start_spinner(
    isolated_cli, monkeypatch, command, module, output
):
    spinner = MagicMock()
    monkeypatch.setattr(module.console, "status", lambda *args: spinner)
    runner.invoke(app, [command, "ls", "--output", output])
    spinner.start.assert_not_called()


@pytest.mark.parametrize("output", ["table", "csv", "json", "jsonl"])
def test_profile_redaction_in_every_format(isolated_cli, output):
    added = runner.invoke(
        app,
        [
            "profile",
            "add",
            "dev",
            "--no-interactive",
            "--host",
            "localhost",
            "--api-key",
            "secret-test-value",
        ],
    )
    assert added.exit_code == 0
    result = runner.invoke(app, ["profile", "ls", "--output", output])
    assert result.exit_code == 0
    assert "secret-test-value" not in result.output
    assert 'api_key"' not in result.output
    if output in ("json", "jsonl"):
        payload = json.loads(result.stdout)
        record = payload["profiles"][0] if output == "json" else payload
        assert record == {
            "name": "dev",
            "host": "localhost",
            "port": 6726,
            "default": True,
            "tls": False,
            "api_key_configured": True,
        }


def test_public_profile_projection_keeps_private_serialization_intact():
    profile = MosaicoProfile(
        host="localhost", api_key="test-only-key", cert_path="private-path"
    )
    assert profile.to_dict()["api_key"] == "test-only-key"
    public = profile.to_public_dict()
    assert public["api_key_configured"] is True
    assert "api_key" not in public
    assert "cert_path" not in public


def test_doctor_csv_escaping_and_document_shape():
    message = 'Cannot connect to "host", retry\nwith another profile'
    payload = {
        "schema_version": 1,
        "status": "error",
        "profile": {},
        "checks": [{"name": "tcp", "status": "error", "message": message}],
    }
    for output in OutputFormat:
        stream = io.StringIO()
        doctor._renderer.render(payload, output, stream=stream)
        if output == OutputFormat.CSV:
            assert list(csv.reader(io.StringIO(stream.getvalue()))) == [
                ["tcp", "error", message]
            ]
        elif output == OutputFormat.JSON:
            assert json.loads(stream.getvalue()) == payload
        elif output == OutputFormat.JSONL:
            assert json.loads(stream.getvalue()) == payload["checks"][0]
        else:
            assert "Mosaico diagnostics: error" in stream.getvalue()
