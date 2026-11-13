import pytest

from mosaicolabs.enum import SerializationFormat
from mosaicolabs.platform import _proto_format


@pytest.mark.parametrize("fmt", list(SerializationFormat))
def test_roundtrip(fmt):
    assert _proto_format.from_proto(_proto_format.to_proto(fmt)) == fmt


@pytest.mark.parametrize(
    "fmt, wire_value",
    [
        # Must match `Format` in `proto/mosaico/v1/core.proto`.
        (SerializationFormat.Default, 1),
        (SerializationFormat.Ragged, 2),
        (SerializationFormat.Image, 3),
    ],
)
def test_wire_value(fmt, wire_value):
    assert _proto_format.to_proto(fmt) == wire_value
