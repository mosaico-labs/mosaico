"""
Helpers of the LeRobot reader: the conversion of the LeRobot timestamps.
"""

import pyarrow as pa
import pyarrow.compute as pc


def _to_timestamp_ns(timestamp: pa.Array) -> pa.Array:
    """Converts LeRobot timestamps (float seconds) into int64 nanoseconds, rounded."""

    timestamp_ns = pc.round(pc.multiply(timestamp, 1e9)).cast(pa.int64())

    return timestamp_ns
