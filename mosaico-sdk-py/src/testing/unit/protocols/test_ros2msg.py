import pyarrow as pa

from mosaicolabs.protocols import convert_ros2msg

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
    """


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
        pa.field(
            "position",
            vector3_pyarrow_struct,
        ),
        pa.field(
            "orientation",
            vector3_pyarrow_struct,
        ),
    ]
)


def test_schema_from_msgdef():
    assert vector3_pyarrow_struct == convert_ros2msg(geometry_msg_vector3_msgdef)
    assert quaternion_pyarrow_struct == convert_ros2msg(geometry_msg_quaternion_msgdef)
    # assert pose_pyarrow_struct == convert_ros2msg(geometry_msg_pose_msgdef) # This does not work yet
