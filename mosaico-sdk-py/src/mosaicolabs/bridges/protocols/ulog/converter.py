"""
ULog schema converter: ULog `F` records -> PyArrow schemas.

The ULog counterpart of `protocols/mcap/` (`McapSchemaConverter` and its protobuf /
jsonschema / ros converters): it turns the native schema into the `pa.StructType` from
which `resolve_ontology_class` builds the Unmodeled ontology of the topic.

Example, with the real `sensor_accel` format of a PX4 log:

    F: sensor_accel:uint64_t timestamp;float x;...;uint8_t[3] clip_counter;uint8_t[5] _padding0;
    -> struct<x: float, ..., clip_counter: fixed_size_list<uint8>[3]>
       (top-level `timestamp` and `_padding0` dropped)
"""

from typing import Dict, List, Tuple

import pyarrow as pa

# Type mapping from the design guide: same sign and width as the ULog type
_PRIMITIVES: Dict[str, pa.DataType] = {
    "int8_t": pa.int8(),
    "uint8_t": pa.uint8(),
    "int16_t": pa.int16(),
    "uint16_t": pa.uint16(),
    "int32_t": pa.int32(),
    "uint32_t": pa.uint32(),
    "int64_t": pa.int64(),
    "uint64_t": pa.uint64(),
    "float": pa.float32(),
    "double": pa.float64(),
    "bool": pa.bool_(),
}
"""ULog primitive types and their PyArrow counterpart (`char` is handled apart, as a string)."""


class ULogSchemaConverter:
    """
    Converts ULog message formats (the `F` records, as parsed by pyulog into
    `ULog.message_formats`) into PyArrow schemas.

    ULog has a single schema encoding, so unlike `McapSchemaConverter` there is no
    registry: the converter is a set of pure classmethods.

    Mapping: integer, float and bool types keep sign and width, `char` and `char[N]`
    become `pa.string()`, `T[N]` becomes `pa.list_(T, N)`, a nested format becomes a
    `pa.struct` (recursively). `_padding*` fields are dropped everywhere, the top-level
    `timestamp` is dropped too, since it becomes the `Message.timestamp_ns` envelope.
    """

    @classmethod
    def fields(cls, formats: dict, name: str) -> List[Tuple[str, int, str]]:
        """Returns the `(type, array_size, name)` fields of format `name`, padding excluded."""
        return [f for f in formats[name].fields if not f[2].startswith("_padding")]

    @classmethod
    def _type(cls, formats: dict, ulog_type: str, array_size: int) -> pa.DataType:
        """Arrow type of one field. Recursive walk, like `McapSchemaConverter._convert`."""
        # char[N] is a C string: stored as text, not as a list of bytes
        if ulog_type == "char":
            return pa.string()
        if ulog_type in _PRIMITIVES:
            base = _PRIMITIVES[ulog_type]
        else:
            # A nested format (e.g. `telemetry_heartbeat`): convert its fields too.
            # Unlike the top level, a nested `timestamp` is a regular field and is kept.
            base = pa.struct(
                [
                    (n, cls._type(formats, t, s))
                    for t, s, n in cls.fields(formats, ulog_type)
                ]
            )
        # pyulog uses array_size == 0 for scalars; T[N] has always exactly N elements
        return pa.list_(base, array_size) if array_size else base

    @classmethod
    def to_pyarrow(cls, formats: dict, name: str) -> pa.StructType:
        """
        Builds the PyArrow payload schema of format `name`.

        Args:
            formats (dict): `ULog.message_formats`, mapping each format name to its definition.
            name (str): The format to convert.

        Returns:
            pa.StructType: The payload schema, without padding and top-level `timestamp`.

        Raises:
            KeyError: If a field has a type that is neither primitive nor a known format.
        """
        return pa.struct(
            [
                (n, cls._type(formats, t, s))
                for t, s, n in cls.fields(formats, name)
                if n != "timestamp"
            ]
        )

    # The two methods below keep the original schema for the future extraction
    # (Mosaico -> .ulg), like `stringify_schema_def` does for MCAP: they are stored
    # in the topic's `_ulog_` metadata.

    @classmethod
    def format_def(cls, formats: dict, name: str) -> str:
        """Rebuilds the original `F` definition of `name` (padding included), as pyulog writes it."""
        return f"{name}:" + "".join(
            f"{t}[{s}] {n};" if s else f"{t} {n};" for t, s, n in formats[name].fields
        )

    @classmethod
    def nested_formats(cls, formats: dict, name: str) -> Dict[str, str]:
        """Returns `{format: definition}` for every format nested (recursively) in `name`."""
        out: Dict[str, str] = {}
        todo = [name]
        while todo:
            for t, _, _ in formats[todo.pop()].fields:
                if t in formats and t not in out:
                    out[t] = cls.format_def(formats, t)
                    todo.append(t)
        return out
