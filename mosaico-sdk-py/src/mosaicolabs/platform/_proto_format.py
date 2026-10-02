"""
Conversions between `mosaico.v1.core.Format` (protobuf) and the public
`SerializationFormat` enum.
"""

from mosaicolabs.enum.serialization_format import SerializationFormat
from mosaicolabs.proto.v1 import core_pb2

_TO_PROTO = {
    SerializationFormat.Default: core_pb2.Format.FORMAT_DEFAULT,
    SerializationFormat.Ragged: core_pb2.Format.FORMAT_RAGGED,
    SerializationFormat.Image: core_pb2.Format.FORMAT_IMAGE,
}

_FROM_PROTO = {value: key for key, value in _TO_PROTO.items()}


def to_proto(fmt: SerializationFormat) -> "core_pb2.Format.ValueType":
    """Converts a `SerializationFormat` into its wire `core.Format` value."""
    return _TO_PROTO[fmt]


def from_proto(value: "core_pb2.Format.ValueType") -> SerializationFormat:
    """
    Converts a wire `core.Format` value into a `SerializationFormat`.

    Raises:
        ValueError: If `value` is `FORMAT_UNSPECIFIED` or otherwise unrecognized.
    """
    try:
        return _FROM_PROTO[value]
    except KeyError:
        raise ValueError(f"Unrecognized or unspecified serialization format: {value}")
