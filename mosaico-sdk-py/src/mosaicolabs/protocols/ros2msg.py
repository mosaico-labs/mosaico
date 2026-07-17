# =========================================================================== #
# Converter 1: ROS 2 .msg  (encoding == "ros2msg")
# =========================================================================== #
from typing import Dict, TypeGuard, get_args

import pyarrow as pa
from rosbags.interfaces.typing import (
    BaseDesc,
    Basename,
    Basetype,
    NameDesc,
    Nodetype,
    Typesdict,
)
from rosbags.typesys import get_types_from_msg

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

_BASENAME_VALUES = frozenset(get_args(Basename))


def is_basetype(obj: object) -> TypeGuard[Basetype]:
    """Check whether ``obj`` matches rosbags' ``Basetype`` alias.

    ``Basetype`` is defined by rosbags as ``tuple[Basename, int]``, i.e. a
    ROS field descriptor made of the primitive type name (e.g. ``"float64"``)
    and a default-value marker.

    Args:
        obj: The object to check, typically the ``content`` half of a
            ``(Nodetype, content)`` field descriptor.

    Returns:
        ``True`` if ``obj`` is a 2-tuple whose first element is a known
        ``Basename`` and whose second element is an ``int``.
    """
    return (
        isinstance(obj, tuple)
        and len(obj) == 2
        and obj[0] in _BASENAME_VALUES
        and isinstance(obj[1], int)
    )


def is_list_sequence_type(obj: object) -> TypeGuard[tuple[BaseDesc | NameDesc, int]]:
    """Check whether ``obj`` matches rosbags' list/sequence descriptor shape.

    Rosbags represents both fixed-size arrays (``T[N]``) and variable-size
    sequences (``T[]``) as ``tuple[BaseDesc | NameDesc, int]``, where the
    trailing ``int`` is the array size (``0`` for unbounded sequences).

    Args:
        obj: The object to check, typically the ``content`` half of a
            ``(Nodetype.SEQUENCE | Nodetype.ARRAY, content)`` field
            descriptor.

    Returns:
        ``True`` if ``obj`` is a 2-tuple whose second element is an ``int``.
    """

    return isinstance(obj, tuple) and len(obj) == 2 and isinstance(obj[1], int)


def _typesdict_to_schema(types_dict: Typesdict, msgtype: str) -> pa.StructType:
    """Recursively build a PyArrow struct type for a message type.

    Walks the field definitions for ``msgtype`` inside ``types_dict`` (as
    produced by ``rosbags.typesys.get_types_from_msg``) and translates each
    field into a PyArrow field, recursing into nested message types and
    list/array element types as needed.

    Args:
        types_dict: Mapping of fully-qualified ROS message type name to its
            ``(constants, fields)`` definition, as returned by
            ``get_types_from_msg``.
        msgtype: Fully-qualified name (e.g. ``"geometry_msgs/msg/Point"``) of
            the message type within ``types_dict`` to convert.

    Returns:
        A ``pa.StructType`` mirroring the fields of ``msgtype``, with ROS
        constants omitted and nested/list types resolved recursively.

    Raises:
        TypeError: If a field's descriptor does not match the expected
            ``Basetype`` or list/sequence shape.
        RuntimeError: If a field has an unrecognised ``Nodetype``.
        KeyError: If a referenced nested message type is not present in
            ``types_dict``.
    """
    # Extract msgtype from Typesdict since we need to create the pyarrow struct of that particular part
    const_def, fields_def = types_dict[msgtype]
    pyarrow_fields: list[pa.field] = []

    for field_name, field_desc in fields_def:
        # field_def example for simple types:   ("x"               , (Nodetype.BASE    , ("float64", 0)                 ))
        # field_def example for complex types:  ("pos"             , (Nodetype.NAME    , "geometry_msg/msg/Point"))
        # field_def example for sequence types: ('cell_temperature', (Nodetype.SEQUENCE, ((T.BASE, ('float32', 0)), 0) )) -> unknown size lists
        # field_def example for sequence types: ('k'               , (Nodetype.ARRAY   , ((T.BASE, ('float64', 0)), 9) )) -> fixed size lists
        node_type, content = field_desc

        if node_type is Nodetype.BASE:
            if not is_basetype(content):
                raise TypeError(
                    f"Expected Basetype (Basename, int) for field {field_name!r}, got {content!r}"
                )

            ros_type, default_value = content

            pyarrow_type = _ROS_2_PYARROW_TYPE[ros_type]
            pyarrow_fields.append(pa.field(field_name, pyarrow_type))

        elif node_type is Nodetype.NAME and isinstance(content, str):
            composed_field = pa.field(
                field_name, _typesdict_to_schema(types_dict, content)
            )

            pyarrow_fields.append(composed_field)

        elif node_type in (Nodetype.SEQUENCE, Nodetype.ARRAY):
            if not is_list_sequence_type(content):
                raise TypeError(
                    f"Expected rosbag Array or Sequence Type for field {field_name!r}, got {content!r}"
                )

            # content has the following value: Nodetype.SEQUENCE, ( (T.BASE, ('float32', 0)), 0 )
            list_content, list_size = (
                content  # list_type can be a simple or composed type
            )

            _, list_item_content = list_content

            if isinstance(list_item_content, str):  # complex type
                list_field = pa.field(
                    field_name, _typesdict_to_schema(types_dict, list_item_content)
                )

            else:  # simple type
                ros_type, default_value = list_item_content
                list_field = _ROS_2_PYARROW_TYPE[ros_type]

            pyarrow_list = pa.list_(list_field, list_size if list_size > 0 else -1)
            pyarrow_fields.append(pa.field(field_name, pyarrow_list))
        else:
            raise RuntimeError(
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
        msgdef: The raw ``.msg`` text, optionally including concatenated
            definitions for any nested message types it references.
        msgtype: Fully-qualified name (e.g. ``"geometry_msgs/msg/Pose"``) of
            the top-level message type described by ``msgdef``.

    Returns:
        A ``pa.StructType`` with one field per non-constant field of
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
    types_dict = get_types_from_msg(msgdef, msgtype)
    return _typesdict_to_schema(types_dict, msgtype)
