from mcap.reader import DecodedMessageTuple
from mcap.records import Channel, Message, Schema
from mcap_protobuf.schema import build_file_descriptor_set

# TODO: uncomment when Adapter PR will be merged
# from mosaicolabs import IMU, Time, Vector3d
# from mosaicolabs.bridges.mcap import MCAPAdapterBase, MCAPMessage
from mosaicolabs import Time
from mosaicolabs.bridges.mcap.decoders.protobuf.decoder import MCAPProtobufMsgDecoder

from ...config import (
    IMU_PROTOBUF,
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
    msg = IMU_PROTOBUF(calibrated=True)

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


# TODO: uncomment when Adapter PR will be merged
# class MyImuProtobufAdapter(MCAPAdapterBase[IMU]):
#     """Custom adapter created for the Imu.proto message available at src/testing/unit/bridges/utils/proto/imu.proto"""

#     schema_name = "Mosaico.Imu"
#     schema_encoding = "protobuf"
#     __mosaico_ontology_type__ = IMU

#     @classmethod
#     def from_dict(cls, mcap_data: dict) -> IMU:

#         mcap_acceleration = mcap_data["linear_acceleration"]
#         mcap_angular_velocity = mcap_data["angular_velocity"]

#         return IMU(
#             acceleration=Vector3d(
#                 x=mcap_acceleration["x"],
#                 y=mcap_acceleration["y"],
#                 z=mcap_acceleration["z"],
#             ),
#             angular_velocity=Vector3d(
#                 x=mcap_angular_velocity["x"],
#                 y=mcap_angular_velocity["y"],
#                 z=mcap_angular_velocity["z"],
#             ),
#         )

# TODO: uncomment when Adapter PR will be merged
# def test_custom_adapter():
#     """Test that MyImuProtobufAdapter custom adapter create correctly a Mosaico IMU message"""

#     msg = make_imu_mcap(Time(seconds=0, nanoseconds=0), "protobuf")

#     mcap_data = _decode(msg, IMU_PROTOBUF_MSGTYPE)

#     mcap_message = MCAPMessage(
#         channel_name="front_car/imu",
#         channel_encoding="protobuf",
#         schema_name=MyImuProtobufAdapter.schema_name,
#         schema_encoding=MyImuProtobufAdapter.schema_encoding,
#         data=mcap_data,
#         log_time_ns=1,
#         publish_time_ns=1,
#     )

#     msco_imu = MyImuProtobufAdapter.translate(mcap_message).get_data(IMU)

#     assert msco_imu is not None
#     assert msco_imu.acceleration.x == mcap_data["linear_acceleration"]["x"]
#     assert msco_imu.acceleration.y == mcap_data["linear_acceleration"]["y"]
#     assert msco_imu.acceleration.z == mcap_data["linear_acceleration"]["z"]
#     assert msco_imu.angular_velocity.x == mcap_data["angular_velocity"]["x"]
#     assert msco_imu.angular_velocity.y == mcap_data["angular_velocity"]["y"]
#     assert msco_imu.angular_velocity.z == mcap_data["angular_velocity"]["z"]
