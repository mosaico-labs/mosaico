import pytest
from google.protobuf import any_pb2, descriptor_pb2, descriptor_pool
from google.protobuf.message_factory import GetMessageClass
from mcap.reader import DecodedMessageTuple
from mcap.records import Channel, Message, Schema
from mcap_protobuf.schema import build_file_descriptor_set

from mosaicolabs import Time
from mosaicolabs.bridges.mcap.decoders.protobuf.protobuf_decoder import (
    MCAPProtobufMsgDecoder,
)
from mosaicolabs.bridges.mcap.decoders.protobuf.rules_matcher import (
    _coerce_byte_field,
    _coerce_int64_field,
    _modify_timestamp_field,
)

from ...config import (
    IMU_PROTOBUF_CLS,
    IMU_PROTOBUF_MSGTYPE,
    MAGN_PROTOBUF_CLS,
    MAGN_PROTOBUF_MSGTYPE,
    VARIANT_PROTOBUF_CLS,
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
    message = Message(
        channel_id=1,
        log_time=0,
        publish_time=0,
        sequence=0,
        data=msg.SerializeToString(),
    )

    decoder = MCAPProtobufMsgDecoder()
    decoder.register_schema(schema)
    return decoder.decode(
        DecodedMessageTuple(
            schema=schema, channel=channel, message=message, decoded_message=msg
        )
    )


def _make_blobs_cls():
    """Builds, in memory, a `t.Blobs` message holding a `bytes` member of a `oneof` and a
    proto3 `optional bytes` field (backed by a synthetic oneof), so no `.proto` file needs to
    be compiled for it."""
    field_proto = descriptor_pb2.FieldDescriptorProto
    file_proto = descriptor_pb2.FileDescriptorProto(
        name="blobs_test.proto", package="t", syntax="proto3"
    )
    msg_proto = file_proto.message_type.add(name="Blobs")
    msg_proto.oneof_decl.add(name="payload")
    msg_proto.oneof_decl.add(name="_opt_blob")
    msg_proto.field.add(
        name="blob",
        number=1,
        type=field_proto.TYPE_BYTES,
        label=field_proto.LABEL_OPTIONAL,
        oneof_index=0,
    )
    msg_proto.field.add(
        name="text",
        number=2,
        type=field_proto.TYPE_STRING,
        label=field_proto.LABEL_OPTIONAL,
        oneof_index=0,
    )
    msg_proto.field.add(
        name="opt_blob",
        number=3,
        type=field_proto.TYPE_BYTES,
        label=field_proto.LABEL_OPTIONAL,
        oneof_index=1,
        proto3_optional=True,
    )

    pool = descriptor_pool.DescriptorPool()
    pool.Add(file_proto)
    return GetMessageClass(pool.FindMessageTypeByName("t.Blobs"))


BLOBS_MSGTYPE = "t.Blobs"
BLOBS_CLS = _make_blobs_cls()


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


def test_unset_bytes_members_decode_to_none():
    """Regression test: an unset `bytes` member of a `oneof` (or an unset proto3 `optional
    bytes`) is omitted by `MessageToDict` and then filled with `None`; the base64 decoding of
    `bytes` fields must leave that `None` untouched instead of failing the whole message."""
    result = _decode(BLOBS_CLS(text="hi"), BLOBS_MSGTYPE)

    assert result["text"] == "hi"
    assert result["blob"] is None
    assert result["opt_blob"] is None


def test_set_bytes_members_decode_to_original_bytes():
    """Set `bytes` fields are base64-decoded back to their original value."""
    result = _decode(BLOBS_CLS(blob=b"\x00\x01", opt_blob=b"\xff"), BLOBS_MSGTYPE)

    assert result["blob"] == b"\x00\x01"
    assert result["opt_blob"] == b"\xff"
    assert result["text"] is None


@pytest.mark.parametrize(
    "callback", [_coerce_int64_field, _modify_timestamp_field, _coerce_byte_field]
)
def test_transforming_callbacks_leave_none_untouched(callback):
    """A field already filled with `None` (e.g. an unset `oneof` member) has nothing to
    transform: every transforming callback must leave it as is, whatever the rules order."""
    data = {"k": None}

    callback(data, "k")

    assert data == {"k": None}


def test_any_field_keeps_original_bytes():
    """`google.protobuf.Any` is not unpacked: `value` stays the packed message's original wire
    bytes, exactly as they were serialized."""
    msg = make_variant_mcap(Time(seconds=0, nanoseconds=0), "protobuf")

    result = _decode(msg, VARIANT_PROTOBUF_MSGTYPE)

    assert result["details"] == {
        "type_url": msg.details.type_url,
        "value": msg.details.value,
    }


def test_any_field_with_unresolvable_type():
    """Regression test: the type packed into an Any is usually not part of the channel's schema
    (nor of the default descriptor pool), so decoding must never need to resolve it."""
    details = any_pb2.Any(
        type_url="type.googleapis.com/not.registered.Type", value=b"\x08\x01"
    )
    msg = VARIANT_PROTOBUF_CLS(int_value=1, details=details)

    result = _decode(msg, VARIANT_PROTOBUF_MSGTYPE)

    assert result["details"] == {"type_url": details.type_url, "value": details.value}


def test_any_round_trips_byte_identical():
    """Rebuilding the message from the decoded dict, as `UnmodeledAdapter.to_mcap()` does,
    serializes back to the original bytes, Any payload included."""
    msg = make_variant_mcap(Time(seconds=0, nanoseconds=0), "protobuf")

    result = _decode(msg, VARIANT_PROTOBUF_MSGTYPE)

    assert type(msg)(**result).SerializeToString() == msg.SerializeToString()


def test_undefined_list_is_empty():
    """Test that a protobuf field of type repeated when transformed into dict is
    an empty list rather than None, if NOT specified in protobuf"""

    single_reading = {
        "axis": "great_axis",
        "value": 2.0,
        "saturated": False,
    }

    msg = MAGN_PROTOBUF_CLS(readings=[single_reading])

    result = _decode(msg, MAGN_PROTOBUF_MSGTYPE)

    assert result["calibration_notes"] == []  # list
    assert result["readings"][0]["axis_value_covariance"] == []  # nested list


def test_rules_applied_to_nested_lists():
    """Checks that rules are applied also within nested lists (list of objects containing lists).
    In this test 2**53+1 is casted to a string when using MessageToDict and it is an element of a List
    nested within another List of AxisReading objects. If _coerce_int64_field would not be applied to 2**53+1
    it would result as a string in the assert"""

    single_reading = {
        "axis": "great_axis",
        "value": 2.0,
        "axis_value_covariance": [2**53, 2**53 + 1, 2**53 + 2],
    }

    msg = MAGN_PROTOBUF_CLS(readings=[single_reading])

    result = _decode(msg, MAGN_PROTOBUF_MSGTYPE)

    assert all(
        [
            type(cov) is int
            for reading in result["readings"]
            for cov in reading["axis_value_covariance"]
        ]
    )  # check that all elements have been casted to int from string
