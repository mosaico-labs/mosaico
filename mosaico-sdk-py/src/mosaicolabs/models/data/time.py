"""
Time Definitions.

This module defines the standard `Time` and `Duration` classes used to provide temporal
context to ontology data. Both classes use a high-precision seconds/nanoseconds split
based on the ROS convention.

`_TemporalBase` is a private base class that holds the common fields, validators, and
conversion logic shared by `Time` and `Duration`.
"""

import math
import time
from datetime import datetime, timezone
from typing import Type

from pydantic import field_validator
from typing_extensions import Self

from ..core import BaseModel, MosaicoField, MosaicoType, Serializable


class _TemporalBase(BaseModel):
    """Private base class providing shared seconds/nanoseconds fields and temporal conversion logic."""

    seconds: MosaicoType.int64 = MosaicoField(description="Time in seconds.")
    """Seconds since the epoch (Unix time) or since process start (clock time)."""

    nanoseconds: MosaicoType.uint64 = MosaicoField(description="Time in nanoseconds.")
    """Nanoseconds component within the current second, ranging from 0 to 999,999,999."""

    @field_validator("nanoseconds")
    @classmethod
    def validate_nanosec(cls, v: int) -> int:
        """Ensures nanoseconds are within the valid [0, 1e9) range."""
        if not (0 <= v < 1_000_000_000):
            raise ValueError(f"Nanoseconds must be in [0, 1e9). Got {v}")
        return v

    @classmethod
    def from_float(cls, ftime: float) -> Self:
        """
        Factory method to create an instance from a float (seconds since epoch).

        This method carefully handles floating-point artifacts by using rounding for
        the fractional part to ensure stable nanosecond conversion.

        Args:
            ftime: Total seconds since epoch (e.g., from `time.time()`).

        Returns:
            A normalized instance.
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

        return cls(seconds=sec, nanoseconds=nanosec)

    @classmethod
    def from_milliseconds(cls, total_milliseconds: int) -> Self:
        """
        Factory method to create an instance from a total count of milliseconds.

        Args:
            total_milliseconds: Total time elapsed in milliseconds.

        Returns:
            An instance with split sec/nanosec components.
        """
        sec = total_milliseconds // 1_000
        nanosec = (total_milliseconds % 1_000) * 1_000_000
        return cls(seconds=sec, nanoseconds=nanosec)

    @classmethod
    def from_nanoseconds(cls, total_nanoseconds: int) -> Self:
        """
        Factory method to create an instance from a total count of nanoseconds.

        Args:
            total_nanoseconds: Total time elapsed in nanoseconds.

        Returns:
            An instance with split sec/nanosec components.
        """
        sec = total_nanoseconds // 1_000_000_000
        nanosec = total_nanoseconds % 1_000_000_000
        return cls(seconds=sec, nanoseconds=nanosec)

    def to_float(self) -> float:
        """
        Converts the high-precision time to a float.

        Warning: Precision Loss
            Converting to a 64-bit float may result in the loss of nanosecond
            precision due to mantissa limitations.
        """
        return float(self.seconds) + float(self.nanoseconds) * 1e-9

    def to_nanoseconds(self) -> int:
        """
        Converts the time to a total integer of nanoseconds.

        This conversion preserves full precision.
        """
        return (self.seconds * 1_000_000_000) + self.nanoseconds

    def to_milliseconds(self) -> int:
        """
        Converts the time to a total integer of milliseconds.

        This conversion preserves full precision.
        """
        return (self.seconds * 1_000) + int(self.nanoseconds / 1_000_000)


class Time(_TemporalBase, Serializable):
    """
    A high-precision time representation.

    The `Time` class splits a timestamp into a 64-bit integer for seconds and a 32-bit
    unsigned integer for nanoseconds.

    Attributes:
        seconds: Seconds passed since the epoch (Unix time) or process start (clock time).
        nanoseconds: Nanoseconds component within the current second, ranging from 0 to 999,999,999.

    ### Querying with the **`.Q` Proxy**
    This class fields are queryable when constructing a [`QueryOntologyCatalog`][mosaicolabs.models.query.builders.QueryOntologyCatalog]
    via the **`.Q` proxy**. Check the fields documentation for detailed description.

    | Field Access Path | Queryable Type | Supported Operators |
    | :--- | :--- | :--- |
    | `Time.Q.seconds` | `Numeric` | `.eq()`, `.lt()`, `.gt()`, `.leq()`, `.geq()`, `.in_()`, `.between()` |
    | `Time.Q.nanoseconds` | `Numeric` | `.eq()`, `.lt()`, `.gt()`, `.leq()`, `.geq()`, `.in_()`, `.between()` |

    Example:
    ```python
    from mosaicolabs import MosaicoClient, Time, QueryOntologyCatalog

    with MosaicoClient.connect("localhost", 6726) as client:
        # Filter Time with time seconds-component AND time nanoseconds-component
        qresponse = client.query(
            QueryOntologyCatalog(Time.Q.seconds.lt(20.0))
                .with_expression(Time.Q.nanoseconds.gt(100000))
        )

        # Inspect the response
        if qresponse is not None:
            # Results are automatically grouped by Sequence for easier data management
            for item in qresponse:
                print(f"Sequence: {item.sequence.name}")
                print(f"Topics: {[topic.name for topic in item.topics]}")

        # FIXME: Add here example for timestamp exytraction and clustering
    ```

    """

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
    def now(cls: Type[Self]) -> "Time":
        """Factory method that returns the current system UTC time in high precision."""
        return cls.from_float(time.time())

    def to_datetime(self) -> datetime:
        """
        Converts the time to a Python UTC `datetime` object.

        Warning: Microsecond Limitation
            Python's `datetime` objects typically support microsecond precision;
            nanosecond data below that threshold will be truncated.
        """
        return datetime.fromtimestamp(self.to_float(), tz=timezone.utc)


class Duration(_TemporalBase, Serializable):
    """
    A high-precision duration representation.

    The `Duration` class represents a span of time split into a 64-bit integer
    for seconds and a 32-bit unsigned integer for nanoseconds. Both components must be

    Attributes:
        seconds: seconds component of the duration.
        nanoseconds: Nanoseconds component within the current second, ranging from 0 to 999,999,999.

    ### Querying with the **`.Q` Proxy**
    This class fields are queryable when constructing a [`QueryOntologyCatalog`][mosaicolabs.models.query.builders.QueryOntologyCatalog]
    via the **`.Q` proxy**. Check the fields documentation for detailed description.

    | Field Access Path | Queryable Type | Supported Operators |
    | :--- | :--- | :--- |
    | `Duration.Q.seconds` | `Numeric` | `.eq()`, `.lt()`, `.gt()`, `.leq()`, `.geq()`, `.in_()`, `.between()` |
    | `Duration.Q.nanoseconds` | `Numeric` | `.eq()`, `.lt()`, `.gt()`, `.leq()`, `.geq()`, `.in_()`, `.between()` |

    Example:
    ```python
    from mosaicolabs import MosaicoClient, Duration, QueryOntologyCatalog

    with MosaicoClient.connect("localhost", 6726) as client:
        # Filter Duration with seconds-component AND nanoseconds-component
        qresponse = client.query(
            QueryOntologyCatalog(Duration.Q.seconds.lt(20.0))
                .with_expression(Duration.Q.nanoseconds.gt(100000))
        )

        # Inspect the response
        if qresponse is not None:
            for item in qresponse:
                print(f"Sequence: {item.sequence.name}")
                print(f"Topics: {[topic.name for topic in item.topics]}")
    ```

    """
