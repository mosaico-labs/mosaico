# =========================================================================== #
# Converter 1: ROS 1 .msg  (encoding == "ros1msg")
# Converter 1: ROS 2 .msg  (encoding == "ros2msg")
# =========================================================================== #
from typing import Dict, cast

import pyarrow as pa
from rosbags.interfaces.typing import (
    Basename,
    Nodetype,
    Typesdict,
)
from rosbags.typesys import Stores, get_types_from_msg, get_typestore

_ROS_2_PYARROW_TYPE: Dict[str, pa.DataType] = {
    "bool": pa.bool_(),
    "byte": pa.uint8(),
    "char": pa.uint8(),
    "int8": pa.int8(),
    "int16": pa.int16(),
    "int32": pa.int32(),
    "int64": pa.int64(),
    "uint8": pa.uint8(),
    "uint16": pa.uint16(),
    "uint32": pa.uint32(),
    "uint64": pa.uint64(),
    "float32": pa.float32(),
    "float64": pa.float64(),
    "float128": pa.float64(),
    "string": pa.string(),
}


def _typesdict_to_schema(typesdict: Typesdict, msgtype: str) -> pa.StructType:
    """Recursively build a PyArrow struct type for a message type.

    Walks the field definitions for ``msgtype`` inside ``typesdict`` (as
    produced by ``rosbags.typesys.get_types_from_msg``) and translates each
    field into a PyArrow field, recursing into nested message types and
    list/array element types as needed.

    Args:
        typesdict (Typesdict): Mapping of fully-qualified ROS message type name to its
            ``(constants, fields)`` definition, as returned by
            ``get_types_from_msg``.
        msgtype (str): Fully-qualified name (e.g. ``"geometry_msgs/msg/Point"``) of
            the message type within ``typesdict`` to convert.

    Returns:
        pa.StructType: A ``pa.StructType`` mirroring the fields of ``msgtype``, with ROS
        constants omitted and nested/list types resolved recursively.

    Raises:
        TypeError: If a field's descriptor does not match the expected
            ``Basetype`` or list/sequence shape.
    """
    # Extract msgtype from Typesdict since we need to create the pyarrow struct of that particular part
    _, fields_def = typesdict[msgtype]
    pyarrow_fields: list[pa.Field] = []

    for field_name, field_desc in fields_def:
        # field_def example for simple types:   ("x"               , (Nodetype.BASE    , ("float64", 0)                 ))
        # field_def example for complex types:  ("pos"             , (Nodetype.NAME    , "geometry_msg/msg/Point"))
        # field_def example for sequence types: ('cell_temperature', (Nodetype.SEQUENCE, ((T.BASE, ('float32', 0)), 0) )) -> unknown size lists
        # field_def example for sequence types: ('k'               , (Nodetype.ARRAY   , ((T.BASE, ('float64', 0)), 9) )) -> fixed size lists
        node_type, content = field_desc

        if node_type is Nodetype.BASE:
            ros_type, _ = content

            ros_type = cast(Basename, ros_type)  # just for typechecker

            pyarrow_type = _ROS_2_PYARROW_TYPE[ros_type]
            pyarrow_fields.append(pa.field(field_name, pyarrow_type))

        elif node_type is Nodetype.NAME and isinstance(content, str):
            composed_field = pa.field(
                field_name, _typesdict_to_schema(typesdict, content)
            )

            pyarrow_fields.append(composed_field)

        elif node_type in (Nodetype.SEQUENCE, Nodetype.ARRAY):
            # content has the following value: Nodetype.SEQUENCE, ( (T.BASE, ('float32', 0)), 0 )
            # or the following value: Nodetype.SEQUENCE, ( (T.BASE, "msgtype"), 0 )
            list_content, list_size = (
                content  # list_type can be a simple or composed type
            )

            list_size = cast(int, list_size)

            item_node_type, item_content = list_content

            if item_node_type is Nodetype.BASE:  # simple type
                ros_type, _ = item_content
                list_field = _ROS_2_PYARROW_TYPE[ros_type]

            elif item_node_type is Nodetype.NAME and isinstance(
                item_content, str
            ):  # complex type
                list_field = pa.field(
                    field_name, _typesdict_to_schema(typesdict, item_content)
                )

            else:
                raise TypeError(
                    f"Cannot recognise node type {item_node_type} for field {field_name}"
                )

            pyarrow_list = pa.list_(list_field, list_size if list_size > 0 else -1)
            pyarrow_fields.append(pa.field(field_name, pyarrow_list))
        else:
            raise TypeError(
                f"Cannot recognise node type {node_type} for field {field_name}"
            )

    return pa.struct(pyarrow_fields)


def convert_ros2msg(msgdef: str, msgtype: str) -> pa.StructType:
    """Convert a ROS 2 ``.msg`` definition into an equivalent PyArrow struct type.

    The message definition may reference nested message types using the standard
    concatenated ``.msg`` format, where each nested definition is introduced
    by a ``MSG: <type>`` separator line, mirroring what
    ``rosbags.typesys.get_types_from_msg`` expects.

    Args:
        msgdef (str): The raw ``.msg`` text, optionally including concatenated
            definitions for any nested message types it references.
        msgtype (str): Fully-qualified name (e.g. ``"geometry_msgs/msg/Pose"``) of
            the top-level message type described by ``msgdef``.

    Returns:
        pa.StructType: A ``pa.StructType`` with one field per non-constant field of
            ``msgtype``, using PyArrow types mapped from the ROS primitives
            (see ``_ROS_2_PYARROW_TYPE``), with nested messages as nested structs
            and ROS arrays/sequences as PyArrow lists.

    Example:
        Given a nested message definition such as::

            geometry_msg_pose_msgdef = '''
            Point position
            Quaternion orientation

            ================================================================================
            MSG: geometry_msgs/Point
            float64 x
            float64 y
            float64 z

            ================================================================================
            MSG: geometry_msgs/Quaternion
            float64 x 0
            float64 y 0
            float64 z 0
            float64 w 1
            '''

        Calling::

            convert_ros2msg(geometry_msg_pose_msgdef, "geometry_msgs/msg/Pose")

        returns::

            pa.struct([
                pa.field("position", pa.struct([
                    pa.field("x", pa.float64()),
                    pa.field("y", pa.float64()),
                    pa.field("z", pa.float64()),
                ])),
                pa.field("orientation", pa.struct([
                    pa.field("x", pa.float64()),
                    pa.field("y", pa.float64()),
                    pa.field("z", pa.float64()),
                    pa.field("w", pa.float64()),
                ])),
            ])
    """
    msg_typesdict = get_types_from_msg(msgdef, msgtype)

    # This is necessary since builtin_interfaces containing Time and Duration were introduced in ROS2;
    # within ROS1, Time and Duration were built-in types and not part of a separate package. Therefore,
    # ROS1 msgdef do not contain Time and Duration message definitions leading to a typesdict that does
    # not hold Duration and Time as key attributes. However, composed types (like Header) still contain
    # Time and Duration data structures. Consequently, when looking for these structures composition within
    # typesdict they cannot be found, making the system crash. To solve this, Time and Duration definitions
    # need to be added to the typedict decuded from the input msgdef through the typestore coming from Stores.Empty
    typesdict = get_typestore(Stores.EMPTY).fielddefs | msg_typesdict

    return _typesdict_to_schema(typesdict, msgtype)
