import json
import logging
from pathlib import Path
from time import time_ns
from typing import Any, Dict, Optional

import pytest
from google.protobuf.message import Message
from mcap.writer import Writer as JsonschemaWriter
from mcap_protobuf.writer import Writer as ProtobufWriter
from pytest import FixtureRequest

from mosaicolabs.enum.grpc_compression import GRPCCompressionAlgorithm
from mosaicolabs.logging_config import setup_sdk_logging

from .unit.bridges.mcap.config import (
    ALL_CHANNEL_NAMES,
    GPS_CHANNEL_NAME,
    GPS_JSONSCHEMA,
    IMU_CHANNEL_NAME,
    IMU_JSONSCHEMA,
    MAGN_JSONSCHEMA,
    MAGNETOMETER_CHANNEL_NAME,
    N_STEPS,
    START_TIME_NS,
    START_TIME_S,
    STEP_NS,
    make_gps_mcap,
    make_imu_mcap,
    make_magn_mcap,
    time_generator,
)


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
        help="Enable TLS connection with the server.",
    )
    parser.addoption(
        "--gzip",
        action="store_true",
        default=False,
        help="Enable GZIP compression via gRPC.",
    )
    parser.addoption(
        "--api-key-read",
        action="store",
        default=None,
        type=str,
        help="Set Auth Read api-key.",
    )
    parser.addoption(
        "--api-key-write",
        action="store",
        default=None,
        type=str,
        help="Set Auth Write api-key.",
    )
    parser.addoption(
        "--api-key-delete",
        action="store",
        default=None,
        type=str,
        help="Set Auth Delete api-key.",
    )
    parser.addoption(
        "--api-key-manage",
        action="store",
        default=None,
        type=str,
        help="Set Auth Manage api-key.",
    )


@pytest.fixture(scope="session")
def host(request):
    return request.config.getoption("--host")


@pytest.fixture(scope="session")
def port(request):
    return request.config.getoption("--port")


@pytest.fixture(scope="session")
def with_auth(
    api_key_read,
    api_key_write,
    api_key_delete,
    api_key_manage,
):
    return any(
        perm is not None
        for perm in (
            api_key_read,
            api_key_write,
            api_key_delete,
            api_key_manage,
        )
    )


@pytest.fixture(scope="session")
def api_key_read(request):
    return request.config.getoption("--api-key-read")


@pytest.fixture(scope="session")
def api_key_write(request):
    return request.config.getoption("--api-key-write")


@pytest.fixture(scope="session")
def api_key_delete(request):
    return request.config.getoption("--api-key-delete")


@pytest.fixture(scope="session")
def api_key_manage(request):
    return request.config.getoption("--api-key-manage")


@pytest.fixture(scope="session")
def with_tls(request):
    return request.config.getoption("--tls")


@pytest.fixture(scope="session")
def with_gzip(request):
    return request.config.getoption("--gzip")


@pytest.fixture(scope="session")
def compression(with_gzip):
    return (
        GRPCCompressionAlgorithm.StreamGzip
        if with_gzip
        else GRPCCompressionAlgorithm.Null
    )


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


@pytest.fixture
def pristine_mosaico_logger():
    logger = logging.getLogger("mosaicolabs")

    logger.handlers.clear()
    logger.addHandler(logging.NullHandler())
    logger.propagate = False


# MCAP stuff

channelname_to_maker = {
    IMU_CHANNEL_NAME: make_imu_mcap,
    GPS_CHANNEL_NAME: make_gps_mcap,
    MAGNETOMETER_CHANNEL_NAME: make_magn_mcap,
}

channelname_to_jsonschema: Dict[str, Any] = {
    IMU_CHANNEL_NAME: IMU_JSONSCHEMA,
    GPS_CHANNEL_NAME: GPS_JSONSCHEMA,
    MAGNETOMETER_CHANNEL_NAME: MAGN_JSONSCHEMA,
}


@pytest.fixture(scope="session")
def mcap_jsonschema_file(tmp_path_factory):
    """Creates and returns the path to an example mcap file with jsonschema encoding"""

    fn = tmp_path_factory.mktemp("data") / "example_mcap_jsonschema.mcap"

    jsonschema_writer = JsonschemaWriter(str(fn))

    jsonschema_writer.start()

    # Channels are registered once, up front: one schema + one channel per topic.
    channelname_to_channel_id = {}

    for channel_name in ALL_CHANNEL_NAMES:
        schema_id = jsonschema_writer.register_schema(
            name=channel_name,
            encoding="jsonschema",
            data=json.dumps(channelname_to_jsonschema[channel_name]).encode("utf-8"),
        )

        channelname_to_channel_id[channel_name] = jsonschema_writer.register_channel(
            schema_id=schema_id,
            topic=channel_name,
            message_encoding="json",
        )

    for timestamp in time_generator(START_TIME_S, START_TIME_NS, STEP_NS, N_STEPS):
        for channel_name in ALL_CHANNEL_NAMES:
            data_generator = channelname_to_maker[channel_name]

            jsonschema_writer.add_message(
                channel_id=channelname_to_channel_id[channel_name],
                log_time=timestamp.to_nanoseconds(),
                data=json.dumps(data_generator(timestamp, "jsonschema")).encode(
                    "utf-8"
                ),
                publish_time=time_ns(),
            )

    jsonschema_writer.finish()

    return fn


@pytest.fixture(scope="session")
def mcap_protobuf_file(tmp_path_factory):
    """Creates and returns the path to an example mcap file with protobuf encoding"""

    fn = tmp_path_factory.mktemp("data") / "example_mcap_protobuf.mcap"

    mcap_writer = ProtobufWriter(str(fn))

    for timestamp in time_generator(START_TIME_S, START_TIME_NS, STEP_NS, N_STEPS):
        for channel_name in ALL_CHANNEL_NAMES:
            msg_generator = channelname_to_maker[channel_name]

            msg = msg_generator(timestamp, "protobuf")
            assert isinstance(msg, Message)

            mcap_writer.write_message(
                topic=channel_name,
                message=msg,
                log_time=timestamp.to_nanoseconds(),
                publish_time=timestamp.to_nanoseconds(),
            )

    mcap_writer.finish()

    return fn


@pytest.fixture
def mcap_file(request: FixtureRequest):
    """Resolves an indirect parametrize value (a fixture name) to that fixture's value"""

    return request.getfixturevalue(request.param)
