import pyarrow as pa

STAMP_EXPECTED_STRUCT = pa.struct(
    [
        pa.field("sec", pa.int32(), nullable=False),
        pa.field("nanosec", pa.uint32(), nullable=False),
    ]
)

HEADER_EXPECTED_STRUCT = pa.struct(
    [
        pa.field(
            "stamp",
            STAMP_EXPECTED_STRUCT,
            nullable=True,
        ),
        pa.field("frame_id", pa.string(), nullable=False),
    ]
)

VECTOR_EXPECTED_STRUCT = pa.struct(
    [
        pa.field("x", pa.float64(), nullable=False),
        pa.field("y", pa.float64(), nullable=False),
        pa.field("z", pa.float64(), nullable=False),
    ]
)

QUATERNION_EXPECTED_STRUCT = pa.struct(
    [
        pa.field("x", pa.float64(), nullable=False),
        pa.field("y", pa.float64(), nullable=False),
        pa.field("z", pa.float64(), nullable=False),
        pa.field("w", pa.float64(), nullable=False),
    ]
)

IMU_EXPECTED_STRUCT = pa.struct(
    [
        pa.field(
            "header",
            HEADER_EXPECTED_STRUCT,
            nullable=True,
        ),
        pa.field(
            "orientation",
            QUATERNION_EXPECTED_STRUCT,
            nullable=True,
        ),
        pa.field(
            "angular_velocity",
            VECTOR_EXPECTED_STRUCT,
            nullable=True,
        ),
        pa.field(
            "linear_acceleration",
            VECTOR_EXPECTED_STRUCT,
            nullable=True,
        ),
        pa.field(
            "linear_acceleration_covariance",
            pa.list_(pa.float64()),
            nullable=False,
        ),
        pa.field("status", pa.int32(), nullable=False),
        pa.field("calibrated", pa.bool_(), nullable=False),
        pa.field("sequence", pa.uint32(), nullable=False),
        pa.field("temperature_millideg", pa.int32(), nullable=False),
        pa.field("uptime_ns", pa.uint64(), nullable=False),
        pa.field("drift_estimate", pa.float32(), nullable=False),
        pa.field("diagnostic_note", pa.string(), nullable=False),
    ]
)

GPS_EXPECTED_STRUCT = pa.struct(
    [
        pa.field(
            "header",
            HEADER_EXPECTED_STRUCT,
            nullable=True,
        ),
        pa.field(
            "position",
            VECTOR_EXPECTED_STRUCT,
            nullable=True,
        ),
        pa.field("position_covariance", pa.list_(pa.float64()), nullable=False),
        pa.field("fix_type", pa.int32(), nullable=False),
        pa.field("satellites_visible", pa.uint32(), nullable=False),
        pa.field("satellites_used", pa.int32(), nullable=False),
        pa.field("differential", pa.bool_(), nullable=False),
        pa.field("horizontal_accuracy", pa.float32(), nullable=False),
        pa.field("vertical_accuracy", pa.float32(), nullable=False),
        pa.field("utc_time_micros", pa.int64(), nullable=False),
        pa.field("station_id", pa.string(), nullable=False),
        pa.field("raw_nmea", pa.binary(), nullable=False),
        pa.field("active_satellite_ids", pa.list_(pa.string()), nullable=False),
    ]
)

GEO_POINT_EXPECTED_STRUCT = pa.struct(
    [
        pa.field("latitude", pa.float64(), nullable=False),
        pa.field("longitude", pa.float64(), nullable=False),
        pa.field("altitude", pa.float64(), nullable=False),
    ]
)

GPS_EXPECTED_STRUCT = pa.struct(
    [
        pa.field(
            "header",
            HEADER_EXPECTED_STRUCT,
            nullable=True,
        ),
        pa.field(
            "position",
            GEO_POINT_EXPECTED_STRUCT,
            nullable=True,
        ),
        pa.field("position_covariance", pa.list_(pa.float64()), nullable=False),
        pa.field("fix_type", pa.int32(), nullable=False),
        pa.field("satellites_visible", pa.uint32(), nullable=False),
        pa.field("satellites_used", pa.int32(), nullable=False),
        pa.field("differential", pa.bool_(), nullable=False),
        pa.field("horizontal_accuracy", pa.float32(), nullable=False),
        pa.field("vertical_accuracy", pa.float32(), nullable=False),
        pa.field("utc_time_micros", pa.int64(), nullable=False),
        pa.field("station_id", pa.string(), nullable=False),
        pa.field("raw_nmea", pa.binary(), nullable=False),
        pa.field("active_satellite_ids", pa.list_(pa.string()), nullable=False),
    ]
)

MAGNETOMETER_EXPECTED_STRUCT = pa.struct(
    [
        pa.field(
            "header",
            HEADER_EXPECTED_STRUCT,
            nullable=True,
        ),
        pa.field(
            "magnetic_field",
            VECTOR_EXPECTED_STRUCT,
            nullable=True,
        ),
        pa.field("magnetic_field_covariance", pa.list_(pa.float64()), nullable=False),
        pa.field("temperature_celsius", pa.float32(), nullable=False),
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
        pa.field("hardware_revision", pa.uint32(), nullable=False),
        pa.field("raw_counter", pa.int64(), nullable=False),
    ]
)
