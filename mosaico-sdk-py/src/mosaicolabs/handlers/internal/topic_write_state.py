"""
Internal Write State Module.

This module handles the low-level data buffering, serialization, and transmission
logic for a single topic. It implements the remote transmission pipeline,
while optimizing the throughput and preventing memory exhaustion.
"""

import time
from collections import defaultdict
from typing import List, Optional

import pyarrow as pa
import pyarrow.flight as fl
import pyarrow.ipc as pa_ipc

from mosaicolabs.logging_config import get_logger
from mosaicolabs.models.core import Message

# Set the hierarchical logger
logger = get_logger(__name__)


def _encode_messages(objs: list[Message]):
    """Helper to pivot a list of Message objects into a columnar dictionary."""
    result = defaultdict(list)
    for obj in objs:
        for k, v in obj._encode().items():
            result[k].append(v)
    return dict(result)


class _TopicWriteState:
    """
    Manages the write buffer and async dispatch for a single topic.

    **Architecture:**
    1.  **Buffering**: Accumulates `Message` objects in `_current_data_batch`,
        flushing whenever adding the next record would exceed `max_batch_size_bytes`.
        A single record that alone exceeds `max_batch_size_bytes` cannot be split
        any further, so it is dropped instead - see `_push_by_bytes_size()`.
    2.  **Sync Dispatch**.
    """

    def __init__(
        self,
        topic_name: str,
        data_schema: pa.StructType,
        writer: Optional[fl.FlightStreamWriter],
        max_batch_size_bytes: int,
    ):
        """
        Initializes the write state.

        Args:
            topic_name (str): Topic name.
            data_schema (pa.StructType): The pyarrow schema of the ontology payload
                writing to this topic, used to build the combined message+payload
                schema for each flushed `RecordBatch`.
            writer (Optional[fl.FlightStreamWriter]): Active Flight stream writer.
            max_batch_size_bytes (Optional[int]): flush threshold for byte mode.
        """
        if writer is None:
            raise ValueError("Cannot initialize _TopicState: 'writer' is None.")

        self.topic_name: str = topic_name
        self.writer: Optional[fl.FlightStreamWriter] = writer
        self.data_schema: pa.StructType = data_schema
        self.max_batch_size_bytes = max_batch_size_bytes

        # --- Buffering State ---
        self._current_data_batch: List[Message] = []
        self._current_batch_size_bytes: int = 0

        self._written_records = 0
        self._pushed_records = 0
        self._oversized_records = 0

    def _get_record_batch(self, msgs: List[Message]) -> pa.RecordBatch:
        """
        [CPU Bound] Converts Python objects to Arrow RecordBatch.
        """
        if self.writer is None:
            raise ValueError("Writer is None")

        return pa.RecordBatch.from_pydict(
            _encode_messages(msgs),
            schema=Message._get_schema(self.data_schema),
        )

    def _get_serialized_size(self, batch: pa.RecordBatch) -> int:
        """
        Calculates exact serialized size of a RecordBatch in Arrow IPC format.

        Uses PyArrow's native C++ implementation for optimal performance.
        This method is significantly faster than full serialization and provides
        accurate size for Flight transmission limits.

        Note: Returns batch size only (excludes schema message overhead).
        """
        return pa_ipc.get_record_batch_size(batch)

    def _push_by_bytes_size(self, msg: Message) -> bool:
        """
        Buffer logic for Byte-Mode topics (e.g., Images).

        1. Serializes the *single* new record to check its size.
        2. If adding it exceeds `max_batch_size_bytes`, flushes current buffer.
        3. Adds record to new buffer.
        """
        assert self.writer is not None

        # Measure size of the new message
        single_record_batch = self._get_record_batch([msg])
        single_record_size = self._get_serialized_size(single_record_batch)

        # The record cannot be split any further: it is dropped, and the caller
        # (TopicWriter.push()) is responsible for surfacing this to the user
        # (status/last_error) and reporting it to the server as a notification.
        if single_record_size > self.max_batch_size_bytes:
            logger.error(
                f"Single record size ({single_record_size} bytes) exceeds gRPC limit "
                f"({self.max_batch_size_bytes} bytes) for topic '{self.topic_name}'. "
                "Record will be skipped."
            )
            return False

        # Check Buffer Threshold
        projected_size = self._current_batch_size_bytes + single_record_size

        if projected_size > self.max_batch_size_bytes:
            # Flush existing data, without the last message
            if self._current_data_batch:
                self._write_current_batch()
            # Now reset the buffer and add the last message
            self._current_data_batch = [msg]
            self._current_batch_size_bytes = single_record_size
        else:
            # Keep appending until limit is reached
            self._current_data_batch.append(msg)
            self._current_batch_size_bytes += single_record_size

        return True

    def push_record(self, msg: Message) -> bool:
        """
        Adds a record to the buffer.

        Automatically delegates to `_push_by_bytes_size`
        based on the ontology type defined in the message.

        Args:
            msg (Message): The message to push in the buffer for later sending.

        Returns:
            bool: True if the record was buffered for transmission. False if it
                was rejected because, on its own, it exceeds `max_batch_size_bytes`
                and therefore cannot be split any further.

        Raises:
            ValueError: If the writer is None.
        """
        if self.writer is None:
            raise ValueError("write() called on uninitialized state.")

        pushed = self._push_by_bytes_size(msg)

        if pushed:
            self._pushed_records += 1
        else:
            self._oversized_records += 1

        return pushed

    def _submit_write_task(self, msgs_to_write: List[Message]):
        """
        Dispatches the write operation.
        """
        if self.writer is None:
            logger.error(
                f"Cannot write batch for topic '{self.topic_name}'. Writer is None."
            )
            return

        try:
            # Serialization (CPU)
            batch = self._get_record_batch(msgs_to_write)
            # Transmission (IO)
            tstart = time.time()
            self.writer.write(batch)
            logger.debug(
                f"'writer.write' MB {self._get_serialized_size(batch) / (1024 * 1024)}, time: {time.time() - tstart}"
            )
            self._written_records += len(msgs_to_write)
        except Exception as e:
            raise Exception(f"Write failed for topic '{self.topic_name}': '{e}'")

    def _write_current_batch(self):
        """
        Flushes buffer: synchronously writes the buffered records and resets buffer.
        """
        if self.writer is None:
            raise ValueError("Writer is None")

        if self._current_data_batch:
            records = self._current_data_batch

            # Reset immediately
            self._current_data_batch = []
            self._current_batch_size_bytes = 0

            self._submit_write_task(records)

    def close(self, with_error: bool = False):
        """
        Finalizes the topic stream.

        1. Flushes remaining buffer (unless error).
        2. Waits for pending tasks.
        3. Closes Flight writer.
        """
        if self.writer is not None:
            try:
                if not with_error:
                    # Flush any data remaining in the buffer
                    self._write_current_batch()

                self.writer.done_writing()
                logger.info(
                    f"Topic '{self.topic_name}' finished. "
                    f"Pushed: {self._pushed_records}, Written: {self._written_records}, "
                    f"Dropped (oversized): {self._oversized_records}"
                )
            finally:
                self.writer.close()
                self.writer = None
