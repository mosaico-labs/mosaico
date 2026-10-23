"""
Helpers of the LeRobot reader: the conversion of the LeRobot timestamps and the measure of the
batches sent to the server.
"""

import pyarrow as pa
import pyarrow.compute as pc
import pyarrow.ipc as pa_ipc


def _to_timestamp_ns(timestamp: pa.Array) -> pa.Array:
    """Converts LeRobot timestamps (float seconds) into int64 nanoseconds, rounded."""

    timestamp_ns = pc.round(pc.multiply(timestamp, 1e9)).cast(pa.int64())

    return timestamp_ns


def _max_rows_within(batch: pa.RecordBatch, max_size: int) -> int:
    """
    Returns the largest number of leading rows of `batch` within `max_size` bytes, measured as
    `TopicWriter.push_batch` does (Arrow IPC size). `batch` itself must exceed `max_size`.

    The size never decreases when rows are added, so the number is found by bisection: `lo`
    rows are known to fit and `hi` rows are known not to, and the range between them is halved
    until they are adjacent. Returns 0 if not even the first row fits.
    """
    lo, hi = 0, batch.num_rows
    while hi - lo > 1:
        mid = (lo + hi) // 2
        if pa_ipc.get_record_batch_size(batch.slice(0, mid)) <= max_size:
            lo = mid
        else:
            hi = mid
    return lo
