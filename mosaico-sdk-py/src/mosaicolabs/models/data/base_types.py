"""
Primitive Type Wrappers.

This module defines wrapper classes for standard primitive types (Integers, Floats,
Booleans, Strings).

In this SDK, a `Message` requires metadata (timestamps, sequence IDs via `HeaderMixin`)
and a specific serialization schema (`Serializable`) to be transmitted over the
data platform. These wrappers elevate standard Python types to first-class citizens of
the messaging system, allowing a user to send a simple `int` or `string` as a
timestamped, traceable message.
"""

from typing import Any
import pyarrow as pa

from ..serializable import Serializable
from ..mixins import HeaderMixin


class Integer8(Serializable, HeaderMixin):
    """Wrapper for a signed 8-bit integer."""

    __msco_pyarrow_struct__ = pa.struct(
        [
            pa.field(
                "data",
                pa.int8(),
                nullable=False,
                metadata={"description": "8-bit Integer data"},
            ),
        ]
    )
    data: int


class Integer16(Serializable, HeaderMixin):
    """Wrapper for a signed 16-bit integer."""

    __msco_pyarrow_struct__ = pa.struct(
        [
            pa.field(
                "data",
                pa.int16(),
                nullable=False,
                metadata={"description": "16-bit Integer data"},
            ),
        ]
    )
    data: int


class Integer32(Serializable, HeaderMixin):
    """Wrapper for a signed 32-bit integer."""

    __msco_pyarrow_struct__ = pa.struct(
        [
            pa.field(
                "data",
                pa.int32(),
                nullable=False,
                metadata={"description": "32-bit Integer data"},
            ),
        ]
    )
    data: int


class Integer64(Serializable, HeaderMixin):
    """Wrapper for a signed 64-bit integer."""

    __msco_pyarrow_struct__ = pa.struct(
        [
            pa.field(
                "data",
                pa.int64(),
                nullable=False,
                metadata={"description": "64-bit Integer data"},
            ),
        ]
    )
    data: int


class Unsigned8(Serializable, HeaderMixin):
    """Wrapper for an unsigned 8-bit integer."""

    __msco_pyarrow_struct__ = pa.struct(
        [
            pa.field(
                "data",
                pa.uint8(),
                nullable=False,
                metadata={"description": "8-bit Unsigned data"},
            ),
        ]
    )
    data: int

    def model_post_init(self, context: Any) -> None:
        """
        Validates that the input data is non-negative.

        Raises:
            ValueError: If data < 0.
        """
        super().model_post_init(context)
        if self.data < 0:
            raise ValueError("Integer must be unsigned")


class Unsigned16(Serializable, HeaderMixin):
    """Wrapper for an unsigned 16-bit integer."""

    __msco_pyarrow_struct__ = pa.struct(
        [
            pa.field(
                "data",
                pa.uint16(),
                nullable=False,
                metadata={"description": "16-bit Unsigned data"},
            ),
        ]
    )
    data: int

    def model_post_init(self, context: Any) -> None:
        super().model_post_init(context)
        if self.data < 0:
            raise ValueError("Integer must be unsigned")


class Unsigned32(Serializable, HeaderMixin):
    """Wrapper for an unsigned 32-bit integer."""

    __msco_pyarrow_struct__ = pa.struct(
        [
            pa.field(
                "data",
                pa.uint32(),
                nullable=False,
                metadata={"description": "32-bit Unsigned data"},
            ),
        ]
    )
    data: int

    def model_post_init(self, context: Any) -> None:
        super().model_post_init(context)
        if self.data < 0:
            raise ValueError("Integer must be unsigned")


class Unsigned64(Serializable, HeaderMixin):
    """Wrapper for an unsigned 64-bit integer."""

    __msco_pyarrow_struct__ = pa.struct(
        [
            pa.field(
                "data",
                pa.uint64(),
                nullable=False,
                metadata={"description": "64-bit Unsigned data"},
            ),
        ]
    )
    data: int

    def model_post_init(self, context: Any) -> None:
        super().model_post_init(context)
        if self.data < 0:
            raise ValueError("Integer must be unsigned")


class Floating16(Serializable, HeaderMixin):
    """Wrapper for a 16-bit floating point number (Half precision)."""

    __msco_pyarrow_struct__ = pa.struct(
        [
            pa.field(
                "data",
                pa.float16(),
                nullable=False,
                metadata={"description": "16-bit Floating-point data"},
            ),
        ]
    )
    data: float


class Floating32(Serializable, HeaderMixin):
    """Wrapper for a 32-bit floating point number (Single precision)."""

    __msco_pyarrow_struct__ = pa.struct(
        [
            pa.field(
                "data",
                pa.float32(),
                nullable=False,
                metadata={"description": "32-bit Floating-point data"},
            ),
        ]
    )
    data: float


class Floating64(Serializable, HeaderMixin):
    """Wrapper for a 64-bit floating point number (Double precision)."""

    __msco_pyarrow_struct__ = pa.struct(
        [
            pa.field(
                "data",
                pa.float64(),
                nullable=False,
                metadata={"description": "64-bit Floating-point data"},
            ),
        ]
    )
    data: float


class Boolean(Serializable, HeaderMixin):
    """Wrapper for a boolean value."""

    __msco_pyarrow_struct__ = pa.struct(
        [
            pa.field(
                "data",
                pa.bool_(),
                nullable=False,
                metadata={"description": "Boolean data"},
            ),
        ]
    )
    data: bool


class String(Serializable, HeaderMixin):
    """Wrapper for a standard UTF-8 string."""

    __msco_pyarrow_struct__ = pa.struct(
        [
            pa.field(
                "data",
                pa.string(),
                nullable=False,
                metadata={"description": "String data"},
            ),
        ]
    )
    data: str


class LargeString(Serializable, HeaderMixin):
    """
    Wrapper for a 'Large' UTF-8 string.
    Use this when the string data might exceed 2GB in size, requiring 64-bit offsets in Arrow.
    """

    __msco_pyarrow_struct__ = pa.struct(
        [
            pa.field(
                "data",
                pa.large_string(),
                nullable=False,
                metadata={"description": "Large string data"},
            ),
        ]
    )
    data: str
