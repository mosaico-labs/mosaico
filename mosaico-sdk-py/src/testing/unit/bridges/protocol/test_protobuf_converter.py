import pyarrow as pa
import pytest
from mcap_protobuf.schema import build_file_descriptor_set

from mosaicolabs.bridges.protocols.mcap.converters import ProtobufSchemaConverter

from ..config import (
    GPS_PROTOBUF,
    GPS_PROTOBUF_MSGTYPE,
    IMU_PROTOBUF,
    IMU_PROTOBUF_MSGTYPE,
    MAGN_PROTOBUF,
    MAGN_PROTOBUF_MSGTYPE,
    VARIANT_PROTOBUF,
    VARIANT_PROTOBUF_MSGTYPE,
)
from .config import (
    GPS_EXPECTED_STRUCT,
    IMU_EXPECTED_STRUCT,
    MAGNETOMETER_EXPECTED_STRUCT,
)

VARIANT_EXPECTED_STRUCT = pa.struct(
    [
        pa.field("text_value", pa.string(), nullable=True),
        pa.field("int_value", pa.int32(), nullable=True),
        pa.field("bool_value", pa.bool_(), nullable=True),
        pa.field(
            "details",
            pa.struct(
                [
                    pa.field("type_url", pa.string(), nullable=False),
                    pa.field("value", pa.binary(), nullable=False),
                ]
            ),
            nullable=True,
        ),
    ]
)


@pytest.mark.parametrize(
    ("message_class", "msgtype", "expected_struct"),
    [
        (IMU_PROTOBUF, IMU_PROTOBUF_MSGTYPE, IMU_EXPECTED_STRUCT),
        (GPS_PROTOBUF, GPS_PROTOBUF_MSGTYPE, GPS_EXPECTED_STRUCT),
        (MAGN_PROTOBUF, MAGN_PROTOBUF_MSGTYPE, MAGNETOMETER_EXPECTED_STRUCT),
        (VARIANT_PROTOBUF, VARIANT_PROTOBUF_MSGTYPE, VARIANT_EXPECTED_STRUCT),
    ],
    ids=["Imu", "Gps", "Magnetometer", "Variant"],
)
def test_convert_protobuf(message_class, msgtype, expected_struct):
    """Every message's FileDescriptorSet converts to the expected pa.StructType.

    ``Variant`` covers a protobuf ``oneof`` (surfaced as flat nullable sibling
    fields, matching how ``MessageToDict`` decodes it) and a ``google.protobuf.Any``
    field (surfaced as its own raw ``{type_url, value}`` struct, since Any is just
    an ordinary well-known message and needs no special-casing).
    """

    fds_bytes = build_file_descriptor_set(message_class).SerializeToString()

    result = ProtobufSchemaConverter.convert_protobuf(fds_bytes, msgtype)

    assert result == expected_struct
