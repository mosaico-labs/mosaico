"""
Header and Time Definitions.

This module defines the standard `Header` structure used to provide context (time, frame)
to ontology data. It includes a high-precision `Time` class to handle ROS-style
seconds/nanoseconds splitting, avoiding floating-point precision loss associated
with standard Python timestamps.
"""

from typing import Optional
import math
import time
from pydantic import field_validator
import pyarrow as pa
from datetime import datetime, timezone

from .base_model import BaseModel


class Time(BaseModel):
    """
    A high-precision time representation designed to prevent precision loss.

    The `Time` class splits a timestamp into a 64-bit integer for seconds and a 32-bit
    unsigned integer for nanoseconds. This dual-integer structure follows
    robotics standards (like ROS) to ensure temporal accuracy that standard 64-bit
    floating-point timestamps cannot maintain over long durations.


    Attributes:
        sec: Seconds since the epoch (Unix time).
        nanosec: Nanoseconds component within the current second, ranging from 0 to 999,999,999.
    """

    __msco_pyarrow_struct__ = pa.struct(
        [
            pa.field("sec", pa.int64()),
            pa.field("nanosec", pa.uint32()),
        ]
    )

    sec: int
    """Seconds since the epoch (Unix time)."""

    nanosec: int
    """Nanoseconds component within the current second, ranging from 0 to 999,999,999."""

    @field_validator("nanosec")
    @classmethod
    def validate_nanosec(cls, v: int) -> int:
        """Ensures nanoseconds are within the valid [0, 1e9) range."""
        if not (0 <= v < 1_000_000_000):
            raise ValueError(f"Nanoseconds must be in [0, 1e9). Got {v}")
        return v

    @classmethod
    def from_float(cls, ftime: float) -> "Time":
        """
        Factory method to create a Time object from a float (seconds since epoch).

        This method carefully handles floating-point artifacts by using rounding for
        the fractional part to ensure stable nanosecond conversion.

        Args:
            ftime: Total seconds since epoch (e.g., from `time.time()`).

        Returns:
            A normalized `Time` instance.
        """
        # Handle negative timestamps (although this is assumed a wrong behavior)
        # We must account for nanoseconds to be unsigned. This must be handled by borrowing from the seconds component.
        if ftime < 0:
            # e.g. -1.5 => sec = -2
            sec = math.floor(ftime)
            # Calculate remainder to reach the next second
            nanosec = int(round((ftime - sec) * 1e9))
        else:
            sec = int(ftime)
            frac_part = ftime - sec
            # Use round() to handle floating point artifacts (e.g., 0.999999 -> 1.0)
            nanosec = int(round(frac_part * 1e9))

        # Normalize if rounding pushed nanosec to 1 second
        if nanosec >= 1_000_000_000:
            sec += 1
            nanosec = 0

        return cls(sec=sec, nanosec=nanosec)

    @classmethod
    def from_milliseconds(cls, total_milliseconds: int) -> "Time":
        """
        Factory method to create a Time object from a total count of milliseconds.

        Args:
            total_milliseconds: Total time elapsed in milliseconds.

        Returns:
            A `Time` instance with split sec/nanosec components.
        """
        sec = total_milliseconds // 1_000
        nanosec = (total_milliseconds % 1_000) * 1_000_000
        return cls(sec=sec, nanosec=nanosec)

    @classmethod
    def from_nanoseconds(cls, total_nanoseconds: int) -> "Time":
        """
        Factory method to create a Time object from a total count of nanoseconds.

        Args:
            total_nanoseconds: Total time elapsed in nanoseconds.

        Returns:
            A `Time` instance with split sec/nanosec components.
        """
        sec = total_nanoseconds // 1_000_000_000
        nanosec = total_nanoseconds % 1_000_000_000
        return cls(sec=sec, nanosec=nanosec)

    @classmethod
    def from_datetime(cls, dt: datetime) -> "Time":
        """
        Factory method to create a Time object from a Python `datetime` instance.

        Args:
            dt: A standard Python `datetime` object.

        Returns:
            A `Time` instance reflecting the datetime's timestamp.
        """
        # Note: dt.timestamp() handles timezone conversion if aware
        timestamp = dt.timestamp()
        return cls.from_float(timestamp)

    @classmethod
    def now(cls) -> "Time":
        """Factory method that returns the current system UTC time in high precision."""
        return cls.from_float(time.time())

    def to_float(self) -> float:
        """
        Converts the high-precision time to a float.

        Warning: Precision Loss
            Converting to a 64-bit float may result in the loss of nanosecond
            precision due to mantissa limitations.
        """
        return float(self.sec) + float(self.nanosec) * 1e-9

    def to_nanoseconds(self) -> int:
        """
        Converts the time to a total integer of nanoseconds.

        This conversion preserves full precision.
        """
        return (self.sec * 1_000_000_000) + self.nanosec

    def to_milliseconds(self) -> int:
        """
        Converts the time to a total integer of milliseconds.

        This conversion preserves full precision.
        """
        return (self.sec * 1_000) + int(self.nanosec / 1_000_000)

    def to_datetime(self) -> datetime:
        """
        Converts the time to a Python UTC `datetime` object.

        Warning: Microsecond Limitation
            Python's `datetime` objects typically support microsecond precision;
            nanosecond data below that threshold will be truncated.
        """
        return datetime.fromtimestamp(self.to_float(), tz=timezone.utc)


class Header(BaseModel):
    """
    Standard metadata header used to provide context to ontology data.

    The `Header` structure provides spatial and temporal context, matching common
    industry standards for sensor data. It is typically injected
    into sensor models via the [`HeaderMixin`][mosaicolabs.models.mixins.HeaderMixin].


    Attributes:
        stamp: The high-precision [`Time`][mosaicolabs.models.header.Time] of data acquisition.
        frame_id: A string identifier for the coordinate frame (spatial context).
        seq: An optional sequence ID, primarily used for legacy tracking.

    Note: Nullable Fields
        In the underlying PyArrow schema, all header fields are explicitly marked as
        `nullable=True`. This ensures that empty headers are correctly
        deserialized as `None` rather than default-initialized objects.
    """

    # OPTIONALITY NOTE
    # All fields are explicitly set to `nullable=True`. This prevents Parquet V2
    # readers from incorrectly deserializing a `None` Header field in a class
    # as a default-initialized object (e.g., getting Header(0, ...) instead of None).
    __msco_pyarrow_struct__ = pa.struct(
        [
            pa.field(
                "seq",
                pa.uint32(),
                nullable=True,
                metadata={"description": "Sequence ID. Legacy field."},
            ),
            pa.field(
                "stamp",
                Time.__msco_pyarrow_struct__,
                nullable=True,
                metadata={"description": "Time of data acquisition."},
            ),
            pa.field(
                "frame_id",
                pa.string(),
                nullable=True,
                metadata={"description": "Coordinate frame ID."},
            ),
        ]
    )

    stamp: Time
    """The high-precision [`Time`][mosaicolabs.models.header.Time] of data acquisition."""

    frame_id: Optional[str] = None
    """A string identifier for the coordinate frame (spatial context)."""

    seq: Optional[int] = None
    """An optional sequence ID, primarily used for legacy tracking."""
