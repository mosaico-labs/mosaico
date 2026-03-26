from pathlib import Path
from typing import Optional

import pytest

from mosaicolabs.logging_config import setup_sdk_logging


def pytest_configure(config):
    """
    Hook called by pytest before any tests are run.
    We use it to sync the SDK's internal logging with pytest's CLI options.
    """
    level_str = config.getoption("--log-cli-level")
    if level_str:
        # Initialize the SDK logger
        setup_sdk_logging(level=level_str.upper(), pretty=True, propagate=True)


def pytest_addoption(parser):
    parser.addoption(
        "--host",
        action="store",
        default="localhost",
        type=str,
        help="Set client host.",
    )
    parser.addoption(
        "--port",
        action="store",
        default="6276",
        type=int,
        help="Set client port.",
    )
    parser.addoption(
        "--tls",
        action="store_true",
        default=False,
        help="Enable TLS connection with the server",
    )
    parser.addoption(
        "--api-key",
        action="store",
        default=None,
        type=str,
        help="Set Auth api-key.",
    )


@pytest.fixture(scope="session")
def host(request):
    return request.config.getoption("--host")


@pytest.fixture(scope="session")
def port(request):
    return request.config.getoption("--port")


@pytest.fixture(scope="session")
def with_auth(api_key_mgmt):
    return api_key_mgmt is not None


@pytest.fixture(scope="session")
def api_key_mgmt(request):
    return request.config.getoption("--api-key")


@pytest.fixture(scope="session")
def with_tls(request):
    return request.config.getoption("--tls")


@pytest.fixture(scope="session")
def tls_cert_path(with_tls) -> Optional[str]:
    if with_tls:
        return str(
            (
                Path(__file__).resolve().parent
                / "../../../mosaicod/tests/data/cert.pem"
            ).resolve()
        )
    return None
