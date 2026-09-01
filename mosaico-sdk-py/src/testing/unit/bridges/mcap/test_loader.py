import pytest

from mosaicolabs.bridges.mcap import MCAPLoaderJsonschema, MCAPLoaderProtobuf
from mosaicolabs.bridges.mcap.loader import _MCAPLoader

from .config import (
    ALL_CHANNEL_NAMES,
    ALL_PROTOBUF_MSGTYPES,
    GPS_CHANNEL_NAME,
    IMU_CHANNEL_NAME,
    MAGNETOMETER_CHANNEL_NAME,
    N_STEPS,
    START_TIME_NS,
    START_TIME_S,
    STEP_NS,
)

MCAP_FILES_TO_TEST = ["mcap_protobuf_file", "mcap_jsonschema_file"]
ENCODING_TO_TEST = ["protobuf", "json"]


@pytest.mark.parametrize("mcap_file", MCAP_FILES_TO_TEST, indirect=True)
def test_loader_filter(mcap_file):
    """Testing loader filtering capabilities"""

    # 1 - No filtering
    mcap_loader_base = _MCAPLoader(mcap_file)
    assert len(mcap_loader_base.topics) == 3
    assert len(mcap_loader_base.resolved_topics) == 3
    assert len(mcap_loader_base.filtered_topics) == 0

    # 2 - Get only Channel containing Imu
    mcap_loader_filtered = _MCAPLoader(mcap_file, "*Imu*")
    assert len(mcap_loader_filtered.topics) == 1
    assert mcap_loader_filtered.topics[0] == IMU_CHANNEL_NAME
    assert len(mcap_loader_filtered.filtered_topics) == 2
    assert MAGNETOMETER_CHANNEL_NAME in mcap_loader_filtered.filtered_topics
    assert GPS_CHANNEL_NAME in mcap_loader_filtered.filtered_topics

    # 3 - Get all Channels except the ones containing Imu
    mcap_loader_filtered = _MCAPLoader(mcap_file, ["*", "!*Imu*"])
    assert len(mcap_loader_filtered.topics) == 2
    assert MAGNETOMETER_CHANNEL_NAME in mcap_loader_filtered.topics
    assert GPS_CHANNEL_NAME in mcap_loader_filtered.topics
    assert len(mcap_loader_filtered.filtered_topics) == 1
    assert mcap_loader_filtered.filtered_topics[0] == IMU_CHANNEL_NAME


@pytest.mark.parametrize("mcap_file", MCAP_FILES_TO_TEST, indirect=True)
def test_loader_duration(mcap_file):
    """Testing duration property from mcap loader"""

    mcap_loader = _MCAPLoader(mcap_file)

    start = START_TIME_S * 1e9 + START_TIME_NS
    end = start + N_STEPS * STEP_NS

    assert mcap_loader.duration == end - start - STEP_NS


@pytest.mark.parametrize("mcap_file", MCAP_FILES_TO_TEST, indirect=True)
def test_msg_count(mcap_file):
    """Testing msg_count property from mcap loader"""

    mcap_loader = _MCAPLoader(mcap_file)

    assert mcap_loader.msg_count() == len(range(N_STEPS)) * len(ALL_CHANNEL_NAMES)
    assert mcap_loader.msg_count(IMU_CHANNEL_NAME) == len(range(N_STEPS))
    assert mcap_loader.msg_count(GPS_CHANNEL_NAME) == len(range(N_STEPS))
    assert mcap_loader.msg_count(MAGNETOMETER_CHANNEL_NAME) == len(range(N_STEPS))
    assert mcap_loader.msg_count("not_existing_channel") == 0


@pytest.mark.parametrize(
    ("mcap_file", "encoding"),
    zip(MCAP_FILES_TO_TEST, ENCODING_TO_TEST),
    indirect=["mcap_file"],
)
def test_channel_types(mcap_file, encoding):
    """Testing channel_types property from mcap loader"""

    mcap_loader = _MCAPLoader(mcap_file)

    assert [ch_name for ch_name, _ in mcap_loader.channel_types] == ALL_CHANNEL_NAMES
    assert all(
        [ch_encoding == encoding for _, ch_encoding in mcap_loader.channel_types]
    )


def test_protobufloader_streaming(mcap_protobuf_file):
    """Testing that streaming is ok for MCAPLoaderProtobuf"""

    mcap_loader = MCAPLoaderProtobuf(mcap_protobuf_file)

    for mcap_msg, _ in mcap_loader:
        assert mcap_msg.channel_name in ALL_CHANNEL_NAMES
        assert mcap_msg.data_field is not None
        assert mcap_msg.schema_encoding == "protobuf"
        assert mcap_msg.schema_name in ALL_PROTOBUF_MSGTYPES


def test_jsonschemaloader_streaming(mcap_jsonschema_file):
    """Testing that streaming is ok for MCAPLoaderJsonschema"""
    mcap_loader = MCAPLoaderJsonschema(mcap_jsonschema_file)

    for mcap_msg, _ in mcap_loader:
        assert mcap_msg.channel_name in ALL_CHANNEL_NAMES
        assert mcap_msg.data_field is not None
        assert mcap_msg.schema_encoding == "jsonschema"
        assert mcap_msg.schema_name in ALL_CHANNEL_NAMES


def _test_equal_stream(mcap_protobuf_file, mcap_jsonschema_file):
    """
    Checks that both loaders (MCAPLoaderProtobuf and MCAPLoaderJsonschema) produce the same MCAPMessage

    NOTE: this test does not work since: the serialization rules of proto3 and the proto3 spec says that int64,
    fixed64, and uint64 are all strings.
    Refer to: https://github.com/grpc-ecosystem/grpc-gateway/issues/219#issuecomment-251250029 for more information
    """

    mcap_loader_protobuf = MCAPLoaderProtobuf(mcap_protobuf_file)
    mcap_loader_jsonschema = MCAPLoaderJsonschema(mcap_jsonschema_file)

    for (mcap_protobuf_msg, _), (mcap_jsonschema_msg, _) in zip(
        mcap_loader_protobuf, mcap_loader_jsonschema
    ):
        assert mcap_protobuf_msg.data_field == mcap_jsonschema_msg.data_field
