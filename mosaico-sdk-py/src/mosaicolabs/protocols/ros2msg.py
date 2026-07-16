# =========================================================================== #
# Converter 1: ROS 2 .msg  (encoding == "ros2msg")
# =========================================================================== #
import re
from typing import Dict, List, Optional, Tuple

import pyarrow as pa

_ROS2_PRIMITIVES: Dict[str, pa.DataType] = {
    "bool": pa.bool_(),
    "byte": pa.uint8(),
    "char": pa.uint8(),
    "float32": pa.float32(),
    "float64": pa.float64(),
    "int8": pa.int8(),
    "uint8": pa.uint8(),
    "int16": pa.int16(),
    "uint16": pa.uint16(),
    "int32": pa.int32(),
    "uint32": pa.uint32(),
    "int64": pa.int64(),
    "uint64": pa.uint64(),
    "string": pa.string(),
    "wstring": pa.string(),
}

# array suffix:  type[]  type[N]  type[<=N]
_ARRAY_RE = re.compile(r"^(.*?)\[(<=)?(\d*)\]$")


def _parse_ros2_definition(text: str) -> Dict[str, List[Tuple[str, str]]]:
    """
    Parse the concatenated ROS 2 message definition.

    Returns {full_type_name: [(field_type, field_name), ...]}.
    The first block (before any '===' separator) is the root message and is
    stored under the key "".
    """
    blocks = re.split(r"^=+\s*$", text, flags=re.MULTILINE)
    types: Dict[str, List[Tuple[str, str]]] = {}

    for i, block in enumerate(blocks):
        lines = block.splitlines()
        name = "" if i == 0 else None
        fields: List[Tuple[str, str]] = []

        for raw in lines:
            line = raw.split("#", 1)[0].strip()  # strip comments
            if not line:
                continue
            if line.startswith("MSG:"):
                name = line[len("MSG:") :].strip()  # "pkg/msg/Type" or "pkg/Type"
                continue
            parts = line.split()
            if len(parts) < 2:
                continue
            ftype, rest = parts[0], parts[1]
            if "=" in rest or "=" in ftype:  # constant declaration -> skip
                continue
            fname = rest  # ignore trailing default value
            fields.append((ftype, fname))

        if name is not None:
            types[name] = fields
    return types


def _ros2_lookup(
    type_name: str, types: Dict[str, List[Tuple[str, str]]]
) -> Optional[str]:
    """Resolve a (possibly short) type reference to a registry key."""
    if type_name in types:
        return type_name
    base = type_name.split("/")[-1]  # 'Vector3'
    for key in types:
        if key.split("/")[-1] == base:
            return key
    return None


def _ros2_field_type(
    ftype: str, types: Dict[str, List[Tuple[str, str]]]
) -> pa.DataType:
    m = _ARRAY_RE.match(ftype)
    if m:
        elem_type, _bounded, size = m.group(1), m.group(2), m.group(3)
        elem = _ros2_field_type(elem_type, types)
        if size and not _bounded:  # fixed-size array type[N]
            return pa.list_(elem, int(size))
        return pa.list_(elem)  # dynamic / bounded
    # strip a trailing string bound like string<=10
    base = ftype.split("<=")[0]
    if base in _ROS2_PRIMITIVES:
        return _ROS2_PRIMITIVES[base]
    key = _ros2_lookup(base, types)
    if key is None:
        raise ValueError(f"ros2msg: unknown type reference {ftype!r}")
    return _ros2_struct(key, types)


def _ros2_struct(name: str, types: Dict[str, List[Tuple[str, str]]]) -> pa.StructType:
    fields = [
        pa.field(fname, _ros2_field_type(ftype, types)) for ftype, fname in types[name]
    ]
    return pa.struct(fields)


def convert_ros2msg(schema: str) -> pa.StructType:
    types = _parse_ros2_definition(schema)
    return _ros2_struct("", types)
