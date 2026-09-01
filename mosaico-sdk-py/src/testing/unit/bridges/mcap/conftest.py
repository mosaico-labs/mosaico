import json
from time import time_ns
from typing import Any, Dict

import pytest
from google.protobuf.message import Message
from mcap.writer import Writer as JsonschemaWriter
from mcap_protobuf.writer import Writer as ProtobufWriter
from pytest import FixtureRequest

from .config import (
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
