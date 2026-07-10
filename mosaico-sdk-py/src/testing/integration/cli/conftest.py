import pytest
from typer.testing import CliRunner


@pytest.fixture(scope="session")
def cli_runner():
    return CliRunner()


@pytest.fixture(scope="session")
def cli_env(host, port, api_key_manage, with_tls, tls_cert_path):
    env = {
        "MOSAICO_DAEMON_URL": f"{host}:{port}",
    }
    if api_key_manage:
        env["MOSAICO_API_KEY"] = api_key_manage
    if with_tls:
        env["MOSAICO_TLS"] = "true"
    if tls_cert_path:
        env["MOSAICO_CERT_PATH"] = tls_cert_path
    return env
