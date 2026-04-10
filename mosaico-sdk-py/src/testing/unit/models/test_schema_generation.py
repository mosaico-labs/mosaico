from typing import Annotated, Any, Optional, Union

import pyarrow as pa
import pytest

from mosaicolabs import (
    GPS,
    IMU,
    ROI,
    Acceleration,
    Boolean,
    CameraInfo,
    CompressedImage,
    Floating16,
    Floating32,
    Floating64,
    ForceTorque,
    GPSStatus,
    Image,
    Integer8,
    Integer16,
    Integer32,
    Integer64,
    LargeString,
    Magnetometer,
    MosaicoType,
    MotionState,
    NMEASentence,
    Point2d,
    Point3d,
    Pose,
    Pressure,
    Quaternion,
    Range,
    RobotJoint,
    Serializable,
    String,
    Temperature,
    Transform,
    Unsigned8,
    Unsigned16,
    Unsigned32,
    Unsigned64,
    Vector2d,
    Vector3d,
    Vector4d,
    Velocity,
)
from mosaicolabs.models.futures import (
    LaserScan,
    Lidar,
    MultiEchoLaserScan,
    Radar,
    RGBDCamera,
    StereoCamera,
    ToFCamera,
)
from mosaicolabs.ros_bridge.data_ontology import (
    BatteryState,
    FrameTransform,
    PointCloud2,
)

# =============================================================================
# SHARED FIELDS & BASE STRUCTS
# =============================================================================

_COVARIANCE_FIELDS = [
    pa.field(
        "covariance",
        pa.list_(value_type=pa.float64()),
        nullable=True,
        metadata={"description": "The covariance matrix (flattened) of the data."},
    ),
    pa.field(
        "covariance_type",
        pa.int16(),
        nullable=True,
        metadata={
            "description": "Enum integer representing the covariance parameterization."
        },
    ),
]

_VARIANCE_FIELDS = [
    pa.field(
        "variance",
        pa.float64(),
        nullable=True,
        metadata={"description": "The variance of the data."},
    ),
    pa.field(
        "variance_type",
        pa.int16(),
        nullable=True,
        metadata={
            "description": "Enum integer representing the variance parameterization."
        },
    ),
]

_VECTOR_2D_FIELDS = [
    pa.field(
        "x", pa.float64(), nullable=True, metadata={"description": "Vector x component"}
    ),
    pa.field(
        "y", pa.float64(), nullable=True, metadata={"description": "Vector y component"}
    ),
]

_VECTOR_3D_FIELDS = _VECTOR_2D_FIELDS + [
    pa.field(
        "z", pa.float64(), nullable=True, metadata={"description": "Vector z component"}
    ),
]

_VECTOR_4D_FIELDS = _VECTOR_3D_FIELDS + [
    pa.field(
        "w", pa.float64(), nullable=True, metadata={"description": "Vector w component"}
    ),
]

_POSE_FIELDS = [
    pa.field(
        "position",
        pa.struct(_COVARIANCE_FIELDS + _VECTOR_3D_FIELDS),
        nullable=False,
        metadata={"description": "3D translation vector"},
    ),
    pa.field(
        "orientation",
        pa.struct(_COVARIANCE_FIELDS + _VECTOR_4D_FIELDS),
        nullable=False,
        metadata={"description": "Quaternion representing rotation."},
    ),
]

_ACCELERATION_FIELDS = [
    pa.field(
        "linear",
        pa.struct(_COVARIANCE_FIELDS + _VECTOR_3D_FIELDS),
        nullable=True,
        metadata={"description": "3D linear acceleration vector"},
    ),
    pa.field(
        "angular",
        pa.struct(_COVARIANCE_FIELDS + _VECTOR_3D_FIELDS),
        nullable=True,
        metadata={"description": "3D angular acceleration vector"},
    ),
]

_VELOCITY_FIELDS = [
    pa.field(
        "linear",
        pa.struct(_COVARIANCE_FIELDS + _VECTOR_3D_FIELDS),
        nullable=True,
        metadata={"description": "3D linear velocity vector"},
    ),
    pa.field(
        "angular",
        pa.struct(_COVARIANCE_FIELDS + _VECTOR_3D_FIELDS),
        nullable=True,
        metadata={"description": "3D angular velocity vector"},
    ),
]

_GPSSTATUS_FIELDS = [
    pa.field(
        "status", pa.int8(), nullable=False, metadata={"description": "Fix status."}
    ),
    pa.field(
        "service",
        pa.uint16(),
        nullable=False,
        metadata={"description": "Service used (GPS, GLONASS, etc)."},
    ),
    pa.field(
        "satellites",
        pa.int8(),
        nullable=True,
        metadata={"description": "Satellites visible/used."},
    ),
    pa.field(
        "hdop",
        pa.float64(),
        nullable=True,
        metadata={"description": "Horizontal Dilution of Precision."},
    ),
    pa.field(
        "vdop",
        pa.float64(),
        nullable=True,
        metadata={"description": "Vertical Dilution of Precision."},
    ),
]

_ROI_FIELDS = [
    pa.field(
        "offset",
        pa.struct(_COVARIANCE_FIELDS + _VECTOR_2D_FIELDS),
        nullable=False,
        metadata={"description": "(Leftmost, Rightmost) pixels of the ROI."},
    ),
    pa.field(
        "height",
        pa.uint32(),
        nullable=False,
        metadata={"description": "Height pixel of the ROI."},
    ),
    pa.field(
        "width",
        pa.uint32(),
        nullable=False,
        metadata={"description": "Width pixel of the ROI."},
    ),
    pa.field(
        "do_rectify",
        pa.bool_(),
        nullable=True,
        metadata={
            "description": "False if the full image is captured (ROI not used) and True if a subwindow is captured (ROI used) (optional). False if Null"
        },
    ),
]

_DEPTHCAMERA_BASE_FIELDS = [
    pa.field(
        "x",
        pa.list_(pa.float32()),
        nullable=False,
        metadata={"description": "Horizontal position derived from depth"},
    ),
    pa.field(
        "y",
        pa.list_(pa.float32()),
        nullable=False,
        metadata={"description": "Vertical position derived from depth"},
    ),
    pa.field(
        "z",
        pa.list_(pa.float32()),
        nullable=False,
        metadata={
            "description": "Depth value directly (distance along optical axis in meter)"
        },
    ),
    pa.field(
        "intensity",
        pa.list_(pa.float32()),
        nullable=True,
        metadata={"description": "Signal amplitude/intensity."},
    ),
    pa.field(
        "rgb",
        pa.list_(pa.float32()),
        nullable=True,
        metadata={"description": "Packed RGB color value"},
    ),
]

_TOF_FIELDS = [
    pa.field(
        "noise",
        pa.list_(pa.float32()),
        nullable=True,
        metadata={"description": "Noise value per pixel."},
    ),
    pa.field(
        "grayscale",
        pa.list_(pa.float32()),
        nullable=True,
        metadata={"description": "Grayscale amplitude."},
    ),
]

_STEREO_FIELDS = [
    pa.field(
        "luma",
        pa.list_(pa.uint8()),
        nullable=True,
        metadata={
            "description": "Luminance of the corresponding pixel in the rectified image."
        },
    ),
    pa.field(
        "cost",
        pa.list_(pa.uint8()),
        nullable=True,
        metadata={
            "description": "Stereo matching cost (disparity confidence measure, 0 = high confidence)."
        },
    ),
]

_TRANSFORM_FIELDS = [
    pa.field(
        "translation",
        pa.struct(_COVARIANCE_FIELDS + _VECTOR_3D_FIELDS),
        nullable=False,
        metadata={"description": "3D translation vector"},
    ),
    pa.field(
        "rotation",
        pa.struct(_COVARIANCE_FIELDS + _VECTOR_4D_FIELDS),
        nullable=False,
        metadata={"description": "Quaternion representing rotation."},
    ),
    pa.field(
        "target_frame_id",
        pa.string(),
        nullable=True,
        metadata={"description": "Target frame identifier."},
    ),
]

# Base compiled structs
vector2d = pa.struct(_COVARIANCE_FIELDS + _VECTOR_2D_FIELDS)
vector3d = pa.struct(_COVARIANCE_FIELDS + _VECTOR_3D_FIELDS)
vector4d = pa.struct(_COVARIANCE_FIELDS + _VECTOR_4D_FIELDS)
quaternion = vector4d
point2d = vector2d
point3d = pa.struct(_COVARIANCE_FIELDS + _VECTOR_3D_FIELDS)
transform = pa.struct(_COVARIANCE_FIELDS + _TRANSFORM_FIELDS)
acceleration = pa.struct(_COVARIANCE_FIELDS + _ACCELERATION_FIELDS)
velocity = pa.struct(_COVARIANCE_FIELDS + _VELOCITY_FIELDS)
pose = pa.struct(_COVARIANCE_FIELDS + _POSE_FIELDS)
gps_status = pa.struct(_GPSSTATUS_FIELDS)
roi = pa.struct(_ROI_FIELDS)
rgbd = pa.struct(_DEPTHCAMERA_BASE_FIELDS)
tof_camera = pa.struct(_DEPTHCAMERA_BASE_FIELDS + _TOF_FIELDS)
stereo_camera = pa.struct(_DEPTHCAMERA_BASE_FIELDS + _STEREO_FIELDS)


# =============================================================================
# PARAMETERIZED TESTS
# =============================================================================
SCALAR_CASES = [
    (Integer8, pa.int8(), "8-bit Integer data"),
    (Integer16, pa.int16(), "16-bit Integer data"),
    (Integer32, pa.int32(), "32-bit Integer data"),
    (Integer64, pa.int64(), "64-bit Integer data"),
    (Unsigned8, pa.uint8(), "8-bit Unsigned data"),
    (Unsigned16, pa.uint16(), "16-bit Unsigned data"),
    (Unsigned32, pa.uint32(), "32-bit Unsigned data"),
    (Unsigned64, pa.uint64(), "64-bit Unsigned data"),
    (Floating16, pa.float16(), "16-bit Floating-point data"),
    (Floating32, pa.float32(), "32-bit Floating-point data"),
    (Floating64, pa.float64(), "64-bit Floating-point data"),
    (Boolean, pa.bool_(), "Boolean data"),
    (String, pa.string(), "String data"),
    (LargeString, pa.large_string(), "Large string data"),
]


@pytest.mark.parametrize("model, arrow_type, description", SCALAR_CASES)
def test_scalar_struct(model, arrow_type, description):
    expected = pa.struct(
        [
            pa.field(
                "data",
                arrow_type,
                nullable=False,
                metadata={"description": description},
            )
        ]
    )
    assert expected == model.__msco_pyarrow_struct__


@pytest.mark.parametrize(
    "model_cls, expected_struct",
    [
        (Vector2d, vector2d),
        (Vector3d, vector3d),
        (Vector4d, vector4d),
        (Point2d, point2d),
        (Point3d, point3d),
        (Quaternion, quaternion),
    ],
)
def test_composite_structs(model_cls, expected_struct):
    assert set(model_cls.__msco_pyarrow_struct__) == set(expected_struct)


@pytest.mark.parametrize(
    "model_class, expected_struct, use_set_comparison",
    [
        (ROI, roi, False),
        (GPSStatus, gps_status, False),
        (Pose, pose, True),
        (Velocity, velocity, True),
        (Acceleration, acceleration, True),
        (RGBDCamera, rgbd, True),
        (ToFCamera, tof_camera, True),
        (StereoCamera, stereo_camera, True),
        (Transform, transform, True),
    ],
)
def test_base_complex_structs(model_class, expected_struct, use_set_comparison):
    if use_set_comparison:
        assert set(expected_struct) == set(model_class.__msco_pyarrow_struct__)
    else:
        assert expected_struct == model_class.__msco_pyarrow_struct__


# =============================================================================
# Unique Structs
# =============================================================================
def test_transform_struct():
    pyarrow_struct = pa.struct(
        [
            pa.field(
                "translation",
                vector3d,
                nullable=False,
                metadata={"description": "3D translation vector"},
            ),
            pa.field(
                "rotation",
                quaternion,
                nullable=False,
                metadata={"description": "Quaternion representing rotation."},
            ),
            pa.field(
                "target_frame_id",
                pa.string(),
                nullable=True,
                metadata={"description": "Target frame identifier."},
            ),
        ]
        + _COVARIANCE_FIELDS
    )

    assert set(pyarrow_struct) == set(Transform.__msco_pyarrow_struct__)


def test_force_torque_struct():
    pyarrow_struct = pa.struct(
        _COVARIANCE_FIELDS
        + [
            pa.field(
                "force",
                vector3d,
                nullable=False,
                metadata={"description": "3D linear force vector"},
            ),
            pa.field(
                "torque",
                vector3d,
                nullable=False,
                metadata={"description": "3D torque vector"},
            ),
        ]
    )

    assert set(pyarrow_struct) == set(ForceTorque.__msco_pyarrow_struct__)


def test_motion_state_struct():
    pyarrow_struct = pa.struct(
        [
            pa.field(
                "pose",
                pose,
                nullable=False,
                metadata={
                    "description": "6D pose with optional time and covariance info."
                },
            ),
            pa.field(
                "velocity",
                velocity,
                nullable=False,
                metadata={
                    "description": "6D velocity with optional time and covariance info."
                },
            ),
            pa.field(
                "target_frame_id",
                pa.string(),
                nullable=False,
                metadata={"description": "Target frame identifier."},
            ),
            pa.field(
                "acceleration",
                acceleration,
                nullable=True,
                metadata={
                    "description": "6D acceleration with optional time and covariance info."
                },
            ),
        ]
        + _COVARIANCE_FIELDS
    )

    assert set(pyarrow_struct) == set(MotionState.__msco_pyarrow_struct__)


def test_temperature_struct():
    pyarrow_struct = pa.struct(
        [
            pa.field(
                "value",
                pa.float64(),
                nullable=False,
                metadata={"description": "Temperature value in Kelvin."},
            ),
        ]
        + _VARIANCE_FIELDS
    )

    assert set(pyarrow_struct) == set(Temperature.__msco_pyarrow_struct__)


def test_robot_joint_struct():
    pyarrow_struct = pa.struct(
        [
            pa.field(
                "names",
                pa.list_(pa.string()),
                nullable=False,
                metadata={"description": ("Names of the different robot joints")},
            ),
            pa.field(
                "positions",
                pa.list_(pa.float64()),
                nullable=False,
                metadata={
                    "description": (
                        "Positions ([rad] or [m]) of the different robot joints"
                    )
                },
            ),
            pa.field(
                "velocities",
                pa.list_(pa.float64()),
                nullable=False,
                metadata={
                    "description": (
                        "Velocities ([rad/s] or [m/s]) of the different robot joints"
                    )
                },
            ),
            pa.field(
                "efforts",
                pa.list_(pa.float64()),
                nullable=False,
                metadata={
                    "description": (
                        "Efforts ([N] or [N/m]) applied to the different robot joints"
                    )
                },
            ),
        ]
    )

    assert pyarrow_struct == RobotJoint.__msco_pyarrow_struct__


def test_range_struct():
    pyarrow_struct = pa.struct(
        [
            pa.field(
                "radiation_type",
                pa.uint8(),
                nullable=False,
                metadata={"description": "Which type of radiation the sensor used."},
            ),
            pa.field(
                "field_of_view",
                pa.float32(),
                nullable=False,
                metadata={
                    "description": "The arc angle, in radians, over which the distance reading is valid."
                },
            ),
            pa.field(
                "min_range",
                pa.float32(),
                nullable=False,
                metadata={
                    "description": "Minimum range value in meters. Fixed distance means that the minimum range"
                    "must be equal to the maximum range."
                },
            ),
            pa.field(
                "max_range",
                pa.float32(),
                nullable=False,
                metadata={
                    "description": "Maximum range value in meters. Fixed distance means that the minimum range"
                    "must be equal to the maximum range."
                },
            ),
            pa.field(
                "range",
                pa.float32(),
                nullable=False,
                metadata={"description": "Range value in meters."},
            ),
        ]
        + _VARIANCE_FIELDS
    )

    assert set(pyarrow_struct) == set(Range.__msco_pyarrow_struct__)


def test_pressure_struct():
    pyarrow_struct = pa.struct(
        [
            pa.field(
                "value",
                pa.float64(),
                nullable=False,
                metadata={
                    "description": "The absolute pressure reading from the sensor in Pascals."
                },
            ),
        ]
        + _VARIANCE_FIELDS
    )

    assert set(pyarrow_struct) == set(Pressure.__msco_pyarrow_struct__)


def test_magnetometer_struct():
    pyarrow_struct = pa.struct(
        [
            pa.field(
                "magnetic_field",
                Vector3d.__msco_pyarrow_struct__,
                nullable=False,
                metadata={
                    "description": "Magnetic field vector [mx, my, mz] in microTesla."
                },
            ),
        ]
    )

    assert pyarrow_struct == Magnetometer.__msco_pyarrow_struct__


def test_imu_struct():
    pyarrow_struct = pa.struct(
        [
            pa.field(
                "acceleration",
                vector3d,
                nullable=False,
                metadata={
                    "description": "Linear acceleration vector [ax, ay, az] in m/s^2."
                },
            ),
            pa.field(
                "angular_velocity",
                vector3d,
                nullable=False,
                metadata={
                    "description": "Angular velocity vector [wx, wy, wz] in rad/s."
                },
            ),
            pa.field(
                "orientation",
                quaternion,
                nullable=True,
                metadata={
                    "description": "Estimated orientation [qx, qy, qz, qw] (optional)."
                },
            ),
        ]
    )

    assert pyarrow_struct == IMU.__msco_pyarrow_struct__


def test_image_struct():
    pyarrow_struct = pa.struct(
        [
            pa.field(
                "data",
                pa.binary(),
                nullable=False,
                metadata={"description": "The flattened image memory buffer."},
            ),
            pa.field(
                "format",
                pa.string(),
                nullable=False,
                metadata={"description": "Container format (e.g., 'raw', 'png')."},
            ),
            pa.field(
                "width",
                pa.int32(),
                nullable=False,
                metadata={"description": "Image width in pixels."},
            ),
            pa.field(
                "height",
                pa.int32(),
                nullable=False,
                metadata={"description": "Image height in pixels."},
            ),
            pa.field(
                "stride",
                pa.int32(),
                nullable=False,
                metadata={"description": "Bytes per row. Essential for alignment."},
            ),
            pa.field(
                "encoding",
                pa.string(),
                nullable=False,
                metadata={"description": "Pixel format (e.g., 'bgr8', 'mono16')."},
            ),
            pa.field(
                "is_bigendian",
                pa.bool_(),
                nullable=True,
                metadata={
                    "description": "True if data is Big-Endian. Defaults to system endianness if null."
                },
            ),
        ]
    )

    assert pyarrow_struct == Image.__msco_pyarrow_struct__


def test_compressed_image_struct():
    pyarrow_struct = pa.struct(
        [
            pa.field(
                "data",
                pa.binary(),
                nullable=False,
                metadata={
                    "description": "The serialized (compressed) image data as bytes."
                },
            ),
            pa.field(
                "format",
                pa.string(),
                nullable=False,
                metadata={
                    "description": "The compression format (e.g., 'jpeg', 'png')."
                },
            ),
        ]
    )

    assert pyarrow_struct == CompressedImage.__msco_pyarrow_struct__


def test_gps_struct():
    pyarrow_struct = pa.struct(
        [
            pa.field(
                "position",
                point3d,
                nullable=False,
                metadata={"description": "Lat/Lon/Alt (WGS 84)."},
            ),
            pa.field(
                "velocity",
                vector3d,
                nullable=True,
                metadata={"description": "Velocity vector [North, East, Alt] m/s."},
            ),
            pa.field(
                "status",
                gps_status,
                nullable=True,
                metadata={"description": "Receiver status info."},
            ),
        ]
    )

    assert pyarrow_struct == GPS.__msco_pyarrow_struct__


def test_nmea_sentence_struct():
    pyarrow_struct = pa.struct(
        [
            pa.field(
                "sentence",
                pa.string(),
                nullable=False,
                metadata={"description": "Raw ASCII sentence."},
            ),
        ]
    )

    assert pyarrow_struct == NMEASentence.__msco_pyarrow_struct__


def test_camera_info_struct():
    pyarrow_struct = pa.struct(
        [
            pa.field(
                "height",
                pa.uint32(),
                nullable=False,
                metadata={
                    "description": "Height in pixels of the image with which the camera was calibrated."
                },
            ),
            pa.field(
                "width",
                pa.uint32(),
                nullable=False,
                metadata={
                    "description": "Width in pixels of the image with which the camera was calibrated."
                },
            ),
            pa.field(
                "distortion_model",
                pa.string(),
                nullable=False,
                metadata={
                    "description": "The distortion model used (e.g., 'plumb_bob', 'rational_polynomial')."
                },
            ),
            pa.field(
                "distortion_parameters",
                pa.list_(value_type=pa.float64()),
                nullable=False,
                metadata={
                    "description": "The distortion coefficients (k1, k2, t1, t2, k3...). Size depends on the model."
                },
            ),
            pa.field(
                "intrinsic_parameters",
                pa.list_(value_type=pa.float64(), list_size=9),
                nullable=False,
                metadata={
                    "description": "The 3x3 Intrinsic Matrix (K) flattened row-major. "
                    "Projects 3D points in the camera coordinate frame to 2D pixel coordinates."
                },
            ),
            pa.field(
                "rectification_parameters",
                pa.list_(value_type=pa.float64(), list_size=9),
                nullable=False,
                metadata={
                    "description": "The 3x3 Rectification Matrix (R) flattened row-major. "
                    "Used for stereo cameras to align the two image planes."
                },
            ),
            pa.field(
                "projection_parameters",
                pa.list_(value_type=pa.float64(), list_size=12),
                nullable=False,
                metadata={
                    "description": "The 3x4 Projection Matrix (P) flattened row-major. "
                    "Projects 3D world points directly into the rectified image pixel coordinates."
                },
            ),
            pa.field(
                "binning",
                Vector2d.__msco_pyarrow_struct__,
                nullable=True,
                metadata={
                    "description": "Hardware binning factor (x, y). If null, assumes (0, 0) (no binning)."
                },
            ),
            pa.field(
                "roi",
                ROI.__msco_pyarrow_struct__,
                nullable=True,
                metadata={
                    "description": "Region of Interest. Used if the image is a sub-crop of the full resolution."
                },
            ),
        ]
    )

    assert pyarrow_struct == CameraInfo.__msco_pyarrow_struct__


def test_radar_struct():
    pyarrow_struct = pa.struct(
        [
            pa.field(
                "x",
                pa.list_(pa.float32()),
                nullable=False,
                metadata={"description": "x coordinates in meters."},
            ),
            pa.field(
                "y",
                pa.list_(pa.float32()),
                nullable=False,
                metadata={"description": "y coordinates in meters."},
            ),
            pa.field(
                "z",
                pa.list_(pa.float32()),
                nullable=False,
                metadata={"description": "z coordinates in meters."},
            ),
            pa.field(
                "range",
                pa.list_(pa.float32()),
                nullable=True,
                metadata={"description": "radial distance in meters."},
            ),
            pa.field(
                "azimuth",
                pa.list_(pa.float32()),
                nullable=True,
                metadata={"description": "azimuth angle in radians."},
            ),
            pa.field(
                "elevation",
                pa.list_(pa.float32()),
                nullable=True,
                metadata={"description": "elevation angle in radians."},
            ),
            pa.field(
                "rcs",
                pa.list_(pa.float32()),
                nullable=True,
                metadata={"description": "radar cross section in dBm."},
            ),
            pa.field(
                "snr",
                pa.list_(pa.float32()),
                nullable=True,
                metadata={"description": "signal to noise ratio in dB."},
            ),
            pa.field(
                "doppler_velocity",
                pa.list_(pa.float32()),
                nullable=True,
                metadata={"description": "doppler velocity in m/s."},
            ),
            pa.field(
                "vx",
                pa.list_(pa.float32()),
                nullable=True,
                metadata={"description": "x velocity in m/s."},
            ),
            pa.field(
                "vy",
                pa.list_(pa.float32()),
                nullable=True,
                metadata={"description": "y velocity in m/s."},
            ),
            pa.field(
                "vx_comp",
                pa.list_(pa.float32()),
                nullable=True,
                metadata={"description": "x compensated velocity in m/s."},
            ),
            pa.field(
                "vy_comp",
                pa.list_(pa.float32()),
                nullable=True,
                metadata={"description": "y compensated velocity in m/s."},
            ),
            pa.field(
                "ax",
                pa.list_(pa.float32()),
                nullable=True,
                metadata={"description": "x acceleration in m/s^2."},
            ),
            pa.field(
                "ay",
                pa.list_(pa.float32()),
                nullable=True,
                metadata={"description": "y acceleration in m/s^2."},
            ),
            pa.field(
                "radial_speed",
                pa.list_(pa.float32()),
                nullable=True,
                metadata={"description": "radial speed in m/s."},
            ),
        ]
    )

    assert pyarrow_struct == Radar.__msco_pyarrow_struct__


def test_lidar_struct():
    pyarrow_struct = pa.struct(
        [
            pa.field(
                "x",
                pa.list_(pa.float32()),
                nullable=False,
                metadata={"description": "x coordinates in meters"},
            ),
            pa.field(
                "y",
                pa.list_(pa.float32()),
                nullable=False,
                metadata={"description": "y coordinates in meters"},
            ),
            pa.field(
                "z",
                pa.list_(pa.float32()),
                nullable=False,
                metadata={"description": "z coordinates in meters"},
            ),
            pa.field(
                "intensity",
                pa.list_(pa.float32()),
                nullable=True,
                metadata={"description": "strength of the returned signal"},
            ),
            pa.field(
                "reflectivity",
                pa.list_(pa.uint16()),
                nullable=True,
                metadata={"description": "Surface reflectivity"},
            ),
            pa.field(
                "beam_id",
                pa.list_(pa.uint16()),
                nullable=True,
                metadata={
                    "description": "beam index (ring, channel, line), identifies which laser fired the point"
                },
            ),
            pa.field(
                "range",
                pa.list_(pa.float32()),
                nullable=True,
                metadata={"description": "range in meters"},
            ),
            pa.field(
                "near_ir",
                pa.list_(pa.float32()),
                nullable=True,
                metadata={
                    "description": "near-infrared ambient light (noise, ambient)"
                },
            ),
            pa.field(
                "azimuth",
                pa.list_(pa.float32()),
                nullable=True,
                metadata={"description": "azimuth angle in radians"},
            ),
            pa.field(
                "elevation",
                pa.list_(pa.float32()),
                nullable=True,
                metadata={"description": "elevation angle in radians"},
            ),
            pa.field(
                "confidence",
                pa.list_(pa.uint8()),
                nullable=True,
                metadata={
                    "description": "per-point validity/confidence flags (tag, flags), manufacturer-specific bitmask"
                },
            ),
            pa.field(
                "return_type",
                pa.list_(pa.uint8()),
                nullable=True,
                metadata={
                    "description": "single/dual return classification, manufacturer-specific"
                },
            ),
            pa.field(
                "point_timestamp",
                pa.list_(pa.float64()),
                nullable=True,
                metadata={
                    "description": "per-point acquisition time offset from scan start"
                },
            ),
        ]
    )

    assert pyarrow_struct == Lidar.__msco_pyarrow_struct__


def test_laserscan_struct():
    pyarrow_struct = pa.struct(
        [
            pa.field(
                "angle_min",
                pa.float32(),
                nullable=False,
                metadata={"description": "start angle of the scan in rad."},
            ),
            pa.field(
                "angle_max",
                pa.float32(),
                nullable=False,
                metadata={"description": "end angle of the scan in rad."},
            ),
            pa.field(
                "angle_increment",
                pa.float32(),
                nullable=False,
                metadata={
                    "description": "angular distance between measurements in rad."
                },
            ),
            pa.field(
                "time_increment",
                pa.float32(),
                nullable=False,
                metadata={"description": "time between measurements in seconds."},
            ),
            pa.field(
                "scan_time",
                pa.float32(),
                nullable=False,
                metadata={"description": "time between scans in seconds."},
            ),
            pa.field(
                "range_min",
                pa.float32(),
                nullable=False,
                metadata={"description": "minimum range value in meters."},
            ),
            pa.field(
                "range_max",
                pa.float32(),
                nullable=False,
                metadata={"description": "maximum range value in meters."},
            ),
            pa.field(
                "ranges",
                pa.list_(pa.float32()),
                nullable=False,
                metadata={
                    "description": "range data in meters. Ranges need to be between range min and max otherwise discarded."
                },
            ),
            pa.field(
                "intensities",
                pa.list_(pa.float32()),
                nullable=True,
                metadata={"description": "intensity data."},
            ),
        ]
    )

    assert pyarrow_struct == LaserScan.__msco_pyarrow_struct__


def test_multiecholaserscan_struct():
    pyarrow_struct = pa.struct(
        [
            pa.field(
                "angle_min",
                pa.float32(),
                nullable=False,
                metadata={"description": "start angle of the scan in rad."},
            ),
            pa.field(
                "angle_max",
                pa.float32(),
                nullable=False,
                metadata={"description": "end angle of the scan in rad."},
            ),
            pa.field(
                "angle_increment",
                pa.float32(),
                nullable=False,
                metadata={
                    "description": "angular distance between measurements in rad."
                },
            ),
            pa.field(
                "time_increment",
                pa.float32(),
                nullable=False,
                metadata={"description": "time between measurements in seconds."},
            ),
            pa.field(
                "scan_time",
                pa.float32(),
                nullable=False,
                metadata={"description": "time between scans in seconds."},
            ),
            pa.field(
                "range_min",
                pa.float32(),
                nullable=False,
                metadata={"description": "minimum range value in meters."},
            ),
            pa.field(
                "range_max",
                pa.float32(),
                nullable=False,
                metadata={"description": "maximum range value in meters."},
            ),
            pa.field(
                "ranges",
                pa.list_(pa.list_(pa.float32())),
                nullable=False,
                metadata={
                    "description": "range data in meters. Ranges need to be between range min and max otherwise discarded."
                },
            ),
            pa.field(
                "intensities",
                pa.list_(pa.list_(pa.float32())),
                nullable=True,
                metadata={"description": "intensity data."},
            ),
        ]
    )

    assert pyarrow_struct == MultiEchoLaserScan.__msco_pyarrow_struct__


def test_battery_state_struct():
    pyarrow_struct = pa.struct(
        [
            pa.field("voltage", pa.float32(), nullable=False, metadata={"unit": "V"}),
            pa.field("temperature", pa.float32(), metadata={"unit": "C"}),
            pa.field("current", pa.float32(), metadata={"unit": "A"}),
            pa.field("charge", pa.float32(), metadata={"unit": "Ah"}),
            pa.field("capacity", pa.float32(), metadata={"unit": "Ah"}),
            pa.field("design_capacity", pa.float32(), metadata={"unit": "Ah"}),
            pa.field(
                "percentage", pa.float32(), nullable=False, metadata={"range": "0-1"}
            ),
            pa.field("power_supply_status", pa.uint8(), nullable=False),
            pa.field("power_supply_health", pa.uint8(), nullable=False),
            pa.field("power_supply_technology", pa.uint8(), nullable=False),
            pa.field("present", pa.bool_(), nullable=False),
            pa.field("location", pa.string(), nullable=False),
            pa.field("serial_number", pa.string(), nullable=False),
            pa.field("cell_voltage", pa.list_(pa.float32()), nullable=True),
            pa.field("cell_temperature", pa.list_(pa.float32()), nullable=True),
        ]
    )
    assert pyarrow_struct == BatteryState.__msco_pyarrow_struct__


def test_frame_transoform_struct():
    pyarrow_struct = pa.struct(
        [
            pa.field(
                "transforms",
                pa.list_(transform),
                nullable=False,
                metadata={"description": "List of coordinate frames transformations."},
            ),
        ]
    )

    assert pyarrow_struct == FrameTransform.__msco_pyarrow_struct__


def test_pointcloud2_struct():
    point_field_struct = pa.struct(
        [
            pa.field("name", pa.string(), nullable=False),
            pa.field("offset", pa.uint32(), nullable=False),
            pa.field("datatype", pa.int64(), nullable=False),
            pa.field("count", pa.uint32(), nullable=False),
        ]
    )

    pyarrow_struct = pa.struct(
        [
            pa.field("height", pa.uint32(), nullable=False),
            pa.field("width", pa.uint32(), nullable=False),
            pa.field("fields", pa.list_(point_field_struct), nullable=False),
            pa.field("is_bigendian", pa.bool_(), nullable=False),
            pa.field("point_step", pa.uint32(), nullable=False),
            pa.field("row_step", pa.uint32(), nullable=False),
            pa.field("data", pa.binary(), nullable=False),
            pa.field("is_dense", pa.bool_(), nullable=False),
        ]
    )

    assert pyarrow_struct == PointCloud2.__msco_pyarrow_struct__


# =============================================================================
# EXCEPTION TESTS
# =============================================================================
def test_not_supported_annotation():

    with pytest.raises(ValueError):

        class Test1(Serializable):
            test: Any

    with pytest.raises(NotImplementedError):

        class Test2(Serializable):
            test2: Union[int, float, str]


def test_no_MosaicoType():
    class Test1(Serializable):
        x: int

    class Test2(Serializable):
        x: MosaicoType.float16

    class Test3(Serializable):
        x: Annotated[int, pa.timestamp("us", tz="UTC")]

    class Test4(Serializable):
        x: Optional[MosaicoType.float32] = None

    class Test5(Serializable):
        x: MosaicoType.list_(str)

    class Test6(Serializable):
        x: MosaicoType.list_(Vector3d, list_size=3)

    class Test7(Serializable):
        x: MosaicoType.list_(Annotated[int, pa.timestamp("us", tz="UTC")])

    class Test8(Serializable):
        x: MosaicoType.annotate(int, pa.timestamp("us", tz="UTC"))

    pyarrow_struct_1 = pa.struct([pa.field("x", pa.int64(), nullable=False)])
    pyarrow_struct_2 = pa.struct([pa.field("x", pa.float16(), nullable=False)])
    pyarrow_struct_3 = pa.struct(
        [pa.field("x", pa.timestamp("us", tz="UTC"), nullable=False)]
    )
    pyarrow_struct_4 = pa.struct([pa.field("x", pa.float32(), nullable=True)])
    pyarrow_struct_5 = pa.struct([pa.field("x", pa.list_(pa.string()), nullable=False)])
    pyarrow_struct_6 = pa.struct(
        [pa.field("x", pa.list_(vector3d, list_size=3), nullable=False)]
    )
    pyarrow_struct_7 = pa.struct(
        [pa.field("x", pa.list_(pa.timestamp("us", tz="UTC")), nullable=False)]
    )

    assert Test1.__msco_pyarrow_struct__ == pyarrow_struct_1
    assert Test2.__msco_pyarrow_struct__ == pyarrow_struct_2
    assert Test3.__msco_pyarrow_struct__ == pyarrow_struct_3
    assert Test4.__msco_pyarrow_struct__ == pyarrow_struct_4
    assert Test5.__msco_pyarrow_struct__ == pyarrow_struct_5
    assert Test6.__msco_pyarrow_struct__ == pyarrow_struct_6
    assert Test7.__msco_pyarrow_struct__ == pyarrow_struct_7
    assert Test8.__msco_pyarrow_struct__ == pyarrow_struct_3
