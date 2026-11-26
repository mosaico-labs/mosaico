"""
Helpers of the LeRobot reader and writer: the conversion of the LeRobot timestamps.
"""

import pyarrow as pa
import pyarrow.compute as pc


def _to_timestamp_ns(timestamp: pa.Array) -> pa.Array:
    """Converts LeRobot timestamps (float seconds) into int64 nanoseconds, rounded."""

    timestamp_ns = pc.round(pc.multiply(timestamp, 1e9)).cast(pa.int64())

    return timestamp_ns


def _from_timestamp_ns(timestamp_ns: pa.Array) -> pa.Array:
    """Converts int64 nanoseconds back into LeRobot timestamps (float64 seconds): the opposite
    of `_to_timestamp_ns`."""

    return pc.divide(timestamp_ns.cast(pa.float64()), 1e9)
