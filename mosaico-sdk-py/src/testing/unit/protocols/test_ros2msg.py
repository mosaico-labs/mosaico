import pyarrow as pa
import pytest

from mosaicolabs.protocols import convert_ros2msg

# -----------------------------------------------------------------------------
# Message definitions
# -----------------------------------------------------------------------------

geometry_msg_vector3_msgdef = """
    # This represents a vector in free space.
    # It is only meant to represent a direction. Therefore, it does not
    # make sense to apply a translation to it (e.g., when applying a
    # generic rigid transformation to a Vector3, tf2 will only apply the
    # rotation). If you want your data to be translatable too, use the
    # geometry_msgs/Point message instead.

    float64 x
    float64 y
    float64 z
    """

geometry_msg_quaternion_msgdef = """
    # This represents an orientation in free space in quaternion form.

    float64 x 0
    float64 y 0
    float64 z 0
    float64 w 1
    """

geometry_msg_pose_msgdef = """
    # A representation of pose in free space, composed of position and orientation.

    Point position
    Quaternion orientation

    ================================================================================
    MSG: geometry_msgs/Point
    # This contains the position of a point in free space
    float64 x
    float64 y
    float64 z

    ================================================================================
    MSG: geometry_msgs/Quaternion
    # This represents an orientation in free space in quaternion form.

    float64 x 0
    float64 y 0
    float64 z 0
    float64 w 1
    """

geometry_msg_polygon_msgdef = """
    # A specification of a polygon where the first and last points are assumed to be connected

    Point32[] points

    ================================================================================
    MSG: geometry_msgs/Point32
    # This contains the position of a point in free space(with 32 bits of precision).

    float32 x
    float32 y
    float32 z
    """

geometry_msg_pose_with_covariance_msgdef = """
    # This represents a pose in free space with uncertainty.

    Pose pose

    # Row-major representation of the 6x6 covariance matrix
    # The orientation parameters use a fixed-axis representation.
    # In order, the parameters are:
    # (x, y, z, rotation about X axis, rotation about Y axis, rotation about Z axis)
    float64[36] covariance

    ================================================================================
    MSG: geometry_msgs/Pose
    Point position
    Quaternion orientation

    ================================================================================
    MSG: geometry_msgs/Point
    float64 x
    float64 y
    float64 z

    ================================================================================
    MSG: geometry_msgs/Quaternion
    float64 x
    float64 y
    float64 z
    float64 w
    """

primitives_msgdef = """
    bool flag
    byte raw
    char letter
    int8 tiny
    int16 small
    int32 medium
    int64 large
    uint8 utiny
    uint16 usmall
    uint32 umedium
    uint64 ularge
    float32 single
    float64 double
    string label
    string<=16 bounded_label
    """

primitive_lists_msgdef = """
    float32[] temperatures
    string[] names
    float64[9] intrinsics
    int32[5] recent_codes
    """

custom_type_lists_msgdef = """
    Point[] trajectory
    Point[3] triangle

    ================================================================================
    MSG: test_msgs/Point
    float64 x
    float64 y
    float64 z
    """

constants_msgdef = """
    uint8 STATUS_OK=0
    uint8 STATUS_ERROR=1
    uint8 status
    """

missing_nested_type_msgdef = """
    UndefinedType payload
    """

# -----------------------------------------------------------------------------
# Expected pyarrow schemas
# -----------------------------------------------------------------------------

vector3_pyarrow_struct = pa.struct(
    [
        pa.field("x", pa.float64()),
        pa.field("y", pa.float64()),
        pa.field("z", pa.float64()),
    ]
)

quaternion_pyarrow_struct = pa.struct(
    [
        pa.field("x", pa.float64()),
        pa.field("y", pa.float64()),
        pa.field("z", pa.float64()),
        pa.field("w", pa.float64()),
    ]
)

pose_pyarrow_struct = pa.struct(
    [
        pa.field("position", vector3_pyarrow_struct),
        pa.field("orientation", quaternion_pyarrow_struct),
    ]
)

point32_pyarrow_struct = pa.struct(
    [
        pa.field("x", pa.float32()),
        pa.field("y", pa.float32()),
        pa.field("z", pa.float32()),
    ]
)

polygon_pyarrow_struct = pa.struct(
    [
        pa.field("points", pa.list_(point32_pyarrow_struct)),
    ]
)

pose_with_covariance_pyarrow_struct = pa.struct(
    [
        pa.field("pose", pose_pyarrow_struct),
        pa.field("covariance", pa.list_(pa.float64(), 36)),
    ]
)

primitives_pyarrow_struct = pa.struct(
    [
        pa.field("flag", pa.bool_()),
        pa.field("raw", pa.uint8()),
        pa.field("letter", pa.uint8()),
        pa.field("tiny", pa.int8()),
        pa.field("small", pa.int16()),
        pa.field("medium", pa.int32()),
        pa.field("large", pa.int64()),
        pa.field("utiny", pa.uint8()),
        pa.field("usmall", pa.uint16()),
        pa.field("umedium", pa.uint32()),
        pa.field("ularge", pa.uint64()),
        pa.field("single", pa.float32()),
        pa.field("double", pa.float64()),
        pa.field("label", pa.string()),
        pa.field("bounded_label", pa.string()),
    ]
)

primitive_lists_pyarrow_struct = pa.struct(
    [
        pa.field("temperatures", pa.list_(pa.float32())),
        pa.field("names", pa.list_(pa.string())),
        pa.field("intrinsics", pa.list_(pa.float64(), 9)),
        pa.field("recent_codes", pa.list_(pa.int32(), 5)),
    ]
)

custom_type_lists_pyarrow_struct = pa.struct(
    [
        pa.field("trajectory", pa.list_(vector3_pyarrow_struct)),
        pa.field("triangle", pa.list_(vector3_pyarrow_struct, 3)),
    ]
)

# -----------------------------------------------------------------------------
# Scalar and nested message types
# -----------------------------------------------------------------------------


def test_flat_message_with_scalar_fields():
    assert vector3_pyarrow_struct == convert_ros2msg(
        geometry_msg_vector3_msgdef, "geometry_msgs/msg/Vector3"
    )


def test_scalar_fields_with_default_values():
    assert quaternion_pyarrow_struct == convert_ros2msg(
        geometry_msg_quaternion_msgdef, "geometry_msgs/msg/Quaternion"
    )


def test_nested_types():
    assert pose_pyarrow_struct == convert_ros2msg(
        geometry_msg_pose_msgdef, "geometry_msgs/msg/Pose"
    )


def test_all_primitive_type_mappings():
    assert primitives_pyarrow_struct == convert_ros2msg(
        primitives_msgdef, "test_msgs/msg/Primitives"
    )


# -----------------------------------------------------------------------------
# Lists, fixed-size and not fixed-size lists
# -----------------------------------------------------------------------------


def test_lists_of_primitive_types():
    struct = convert_ros2msg(primitive_lists_msgdef, "test_msgs/msg/PrimitiveLists")
    assert primitive_lists_pyarrow_struct == struct


def test_list_of_custom_types():
    assert polygon_pyarrow_struct == convert_ros2msg(
        geometry_msg_polygon_msgdef, "geometry_msgs/msg/Polygon"
    )


def test_lists_of_custom_types_fixed_and_variable():
    assert custom_type_lists_pyarrow_struct == convert_ros2msg(
        custom_type_lists_msgdef, "test_msgs/msg/CustomTypeLists"
    )


def test_nested_custom_type_with_fixed_size_list():
    assert pose_with_covariance_pyarrow_struct == convert_ros2msg(
        geometry_msg_pose_with_covariance_msgdef,
        "geometry_msgs/msg/PoseWithCovariance",
    )


# -----------------------------------------------------------------------------
# Edge cases
# -----------------------------------------------------------------------------


def test_constants_do_not_become_fields():
    struct = convert_ros2msg(constants_msgdef, "test_msgs/msg/Status")
    assert struct == pa.struct([pa.field("status", pa.uint8())])


def test_missing_nested_type_raises():
    with pytest.raises(KeyError, match="test_msgs/msg/UndefinedType"):
        convert_ros2msg(missing_nested_type_msgdef, "test_msgs/msg/Broken")
