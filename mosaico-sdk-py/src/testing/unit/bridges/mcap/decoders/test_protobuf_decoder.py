from mcap.reader import DecodedMessageTuple
from mcap.records import Channel, Message, Schema
from mcap_protobuf.schema import build_file_descriptor_set

from mosaicolabs import Time
from mosaicolabs.bridges.mcap.decoders.protobuf.decoder import MCAPProtobufMsgDecoder

from ...config import (
    IMU_PROTOBUF_CLS,
    IMU_PROTOBUF_MSGTYPE,
    VARIANT_PROTOBUF_MSGTYPE,
    make_imu_mcap,
    make_variant_mcap,
)


def _decode(msg, msgtype: str) -> dict:
    """Runs `msg` through `MCAPProtobufMsgDecoder` exactly as `MCAPLoader.__iter__()` does:
    `register_schema()` once, then `decode()` per message."""
    schema = Schema(
        id=1,
        name=msgtype,
        encoding="protobuf",
        data=build_file_descriptor_set(type(msg)).SerializeToString(),
    )
    channel = Channel(
        id=1, topic="t", message_encoding="protobuf", metadata={}, schema_id=1
    )
    message = Message(channel_id=1, log_time=0, publish_time=0, sequence=0, data=b"")

    decoder = MCAPProtobufMsgDecoder()
    decoder.register_schema(schema)
    return decoder.decode(
        DecodedMessageTuple(
            schema=schema, channel=channel, message=message, decoded_message=msg
        )
    )


def test_unset_singular_message_field_decodes_to_none():
    """Regression test: a singular (non-repeated) message field left unset on the wire has
    real presence, so `MessageToDict` omits it entirely; the decoder must still surface it as
    an explicit `None` rather than silently dropping the key."""
    msg = IMU_PROTOBUF_CLS(calibrated=True)

    result = _decode(msg, IMU_PROTOBUF_MSGTYPE)

    assert result["orientation"] is None
    assert result["angular_velocity"] is None
    assert result["linear_acceleration"] is None
    assert result["calibrated"] is True


def test_set_singular_message_field_decodes_to_dict():
    """A populated singular message field still decodes to its full nested dict."""
    msg = make_imu_mcap(Time(seconds=0, nanoseconds=0), "protobuf")

    result = _decode(msg, IMU_PROTOBUF_MSGTYPE)

    assert result["orientation"] == {"x": 0.0, "y": 1.0, "z": 0.0, "w": 1.0}


def test_oneof_absent_members():
    """For Oneof fields, exactly one member is set, the rest decode to `None`."""
    msg = make_variant_mcap(Time(seconds=0, nanoseconds=0), "protobuf")

    result = _decode(msg, VARIANT_PROTOBUF_MSGTYPE)

    members = {key: result[key] for key in ("text_value", "int_value", "bool_value")}
    assert sum(value is not None for value in members.values()) == 1


def test_stringify_schema_def_round_trips_to_original_bytes():
    """`schema.data` for `protobuf` is a binary serialized `FileDescriptorSet`, not valid
    UTF-8 text, so `stringify_schema_def`/`destringify_schema_def` must round-trip it exactly
    via base64 rather than a plain text decode (which would raise `UnicodeDecodeError`)."""
    original_bytes = build_file_descriptor_set(IMU_PROTOBUF_CLS).SerializeToString()

    schema_def_str = MCAPProtobufMsgDecoder.stringify_schema_def(original_bytes)

    assert isinstance(schema_def_str, str)
    assert (
        MCAPProtobufMsgDecoder.destringify_schema_def(schema_def_str) == original_bytes
    )
