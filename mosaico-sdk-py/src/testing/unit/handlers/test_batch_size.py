"""
Tests for batch size calculation in topic_write_state.

Validates that the optimized get_record_batch_size API provides
accurate size estimation within Flight transmission safety margins.
"""

import pyarrow as pa
import pyarrow.ipc as pa_ipc

from mosaicolabs.comm.connection import PYARROW_OUT_OF_RANGE_BYTES


def test_batch_size_respects_flight_limits():
    """
    Verify that schema overhead does not exceed the 10% safety margin.

    The get_record_batch_size function returns only the batch size,
    excluding schema overhead. This test verifies that the difference
    between estimated and actual size (with schema) is covered by the
    10% safety margin used in _TopicWriteState.
    """
    batch = pa.RecordBatch.from_pydict({"data": [b"x" * 100000]})

    # Estimated size (native API, excludes schema)
    estimated = pa_ipc.get_record_batch_size(batch)

    # Actual size (full serialization with schema)
    sink = pa.BufferOutputStream()
    writer = pa.ipc.new_stream(sink, batch.schema)
    writer.write_batch(batch)
    writer.close()
    actual = sink.getvalue().size

    # Schema overhead must be less than the safety margin
    overhead = actual - estimated
    safety_margin = PYARROW_OUT_OF_RANGE_BYTES * 0.1

    assert overhead < safety_margin, (
        f"Schema overhead ({overhead} bytes) exceeds safety margin ({safety_margin} bytes)"
    )
