import pyarrow as pa
import pytest
from mcap.reader import make_reader

from mosaicolabs.bridges.protocols.mcap.converters import JsonschemaSchemaConverter

from ..config import (
    GPS_CHANNEL_NAME,
    IMU_CHANNEL_NAME,
    MAGNETOMETER_CHANNEL_NAME,
    VARIANT_CHANNEL_NAME,
)

IMU_EXPECTED_STRUCT = pa.struct(
    [
        pa.field(
            "header",
            pa.struct(
                [
                    pa.field(
                        "stamp",
                        pa.struct(
                            [
                                pa.field("sec", pa.int64(), nullable=False),
                                pa.field("nanosec", pa.int64(), nullable=False),
                            ]
                        ),
                        nullable=False,
                    ),
                    pa.field("frame_id", pa.string(), nullable=False),
                ]
            ),
            nullable=False,
        ),
        pa.field(
            "orientation",
            pa.struct(
                [
                    pa.field("x", pa.float64(), nullable=False),
                    pa.field("y", pa.float64(), nullable=False),
                    pa.field("z", pa.float64(), nullable=False),
                    pa.field("w", pa.float64(), nullable=False),
                ]
            ),
            nullable=False,
        ),
        pa.field(
            "angular_velocity",
            pa.struct(
                [
                    pa.field("x", pa.float64(), nullable=False),
                    pa.field("y", pa.float64(), nullable=False),
                    pa.field("z", pa.float64(), nullable=False),
                ]
            ),
            nullable=False,
        ),
        pa.field(
            "linear_acceleration",
            pa.struct(
                [
                    pa.field("x", pa.float64(), nullable=False),
                    pa.field("y", pa.float64(), nullable=False),
                    pa.field("z", pa.float64(), nullable=False),
                ]
            ),
            nullable=False,
        ),
        pa.field(
            "linear_acceleration_covariance", pa.list_(pa.float64()), nullable=False
        ),
        pa.field("status", pa.int64(), nullable=False),
        pa.field("calibrated", pa.bool_(), nullable=False),
        pa.field("sequence", pa.int64(), nullable=False),
        pa.field("temperature_millideg", pa.int64(), nullable=False),
        pa.field("uptime_ns", pa.int64(), nullable=False),
        pa.field("drift_estimate", pa.float64(), nullable=False),
        pa.field("diagnostic_note", pa.string(), nullable=False),
    ]
)

GPS_EXPECTED_STRUCT = pa.struct(
    [
        pa.field(
            "header",
            pa.struct(
                [
                    pa.field(
                        "stamp",
                        pa.struct(
                            [
                                pa.field("sec", pa.int64(), nullable=False),
                                pa.field("nanosec", pa.int64(), nullable=False),
                            ]
                        ),
                        nullable=False,
                    ),
                    pa.field("frame_id", pa.string(), nullable=False),
                ]
            ),
            nullable=False,
        ),
        pa.field(
            "position",
            pa.struct(
                [
                    pa.field("latitude", pa.float64(), nullable=False),
                    pa.field("longitude", pa.float64(), nullable=False),
                    pa.field("altitude", pa.float64(), nullable=False),
                ]
            ),
            nullable=False,
        ),
        pa.field("position_covariance", pa.list_(pa.float64()), nullable=False),
        pa.field("fix_type", pa.int64(), nullable=False),
        pa.field("satellites_visible", pa.int64(), nullable=False),
        pa.field("satellites_used", pa.int64(), nullable=False),
        pa.field("differential", pa.bool_(), nullable=False),
        pa.field("horizontal_accuracy", pa.float64(), nullable=False),
        pa.field("vertical_accuracy", pa.float64(), nullable=False),
        pa.field("utc_time_micros", pa.int64(), nullable=False),
        pa.field("station_id", pa.string(), nullable=False),
        pa.field("active_satellite_ids", pa.list_(pa.string()), nullable=False),
    ]
)

MAGNETOMETER_EXPECTED_STRUCT = pa.struct(
    [
        pa.field(
            "header",
            pa.struct(
                [
                    pa.field(
                        "stamp",
                        pa.struct(
                            [
                                pa.field("sec", pa.int64(), nullable=False),
                                pa.field("nanosec", pa.int64(), nullable=False),
                            ]
                        ),
                        nullable=False,
                    ),
                    pa.field("frame_id", pa.string(), nullable=False),
                ]
            ),
            nullable=False,
        ),
        pa.field(
            "magnetic_field",
            pa.struct(
                [
                    pa.field("x", pa.float64(), nullable=False),
                    pa.field("y", pa.float64(), nullable=False),
                    pa.field("z", pa.float64(), nullable=False),
                ]
            ),
            nullable=False,
        ),
        pa.field("magnetic_field_covariance", pa.list_(pa.float64()), nullable=False),
        pa.field("temperature_celsius", pa.float64(), nullable=False),
        pa.field("sensor_id", pa.int64(), nullable=False),
        pa.field("saturated", pa.bool_(), nullable=False),
        pa.field("calibration_notes", pa.list_(pa.string()), nullable=False),
        pa.field(
            "readings",
            pa.list_(
                pa.struct(
                    [
                        pa.field("axis", pa.string(), nullable=False),
                        pa.field("value", pa.float64(), nullable=False),
                        pa.field("saturated", pa.bool_(), nullable=False),
                    ]
                )
            ),
            nullable=False,
        ),
        pa.field("hardware_revision", pa.int64(), nullable=False),
        pa.field("raw_counter", pa.int64(), nullable=False),
    ]
)

VARIANT_EXPECTED_STRUCT = pa.struct(
    [
        pa.field("text_value", pa.string(), nullable=False),
        pa.field("int_value", pa.int64(), nullable=False),
        pa.field("bool_value", pa.bool_(), nullable=False),
    ]
)


def _schema_data_for_channel(mcap_path, channel_name: str) -> bytes:
    """Pulls the real, mcap-registered ``Schema.data`` bytes for ``channel_name``
    out of an already-written ``.mcap`` file, mirroring what a real reader sees."""

    with open(mcap_path, "rb") as f:
        summary = make_reader(f).get_summary()
        channel = next(c for c in summary.channels.values() if c.topic == channel_name)
        return summary.schemas[channel.schema_id].data


@pytest.mark.parametrize(
    ("channel_name", "expected_struct"),
    [
        (IMU_CHANNEL_NAME, IMU_EXPECTED_STRUCT),
        (GPS_CHANNEL_NAME, GPS_EXPECTED_STRUCT),
        (MAGNETOMETER_CHANNEL_NAME, MAGNETOMETER_EXPECTED_STRUCT),
        (VARIANT_CHANNEL_NAME, VARIANT_EXPECTED_STRUCT),
    ],
    ids=["Imu", "Gps", "Magnetometer", "Variant"],
)
def test_convert_jsonschema(mcap_jsonschema_file, channel_name, expected_struct):
    """Every channel's real, mcap-registered jsonschema converts to the expected
    pa.StructType.

    ``Variant`` covers a jsonschema ``oneOf`` used the idiomatic way, as a
    validation-only combinator alongside ``properties`` (e.g. ``"oneOf":
    [{"required": ["text_value"]}, ...]``) rather than as a field's sole type
    definition -- it's silently ignored by the converter, surfacing each branch as
    a flat nullable sibling field, matching how the protobuf ``oneof`` converts.
    """

    schema_data = _schema_data_for_channel(mcap_jsonschema_file, channel_name)

    result = JsonschemaSchemaConverter.convert_jsonschema(schema_data)

    assert result == expected_struct
