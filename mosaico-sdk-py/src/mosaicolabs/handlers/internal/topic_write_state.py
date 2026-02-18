"""
Internal Write State Module.

This module handles the low-level data buffering, serialization, and transmission
logic for a single topic. It implements an asynchronous pipeline with backpressure
to optimize throughput while preventing memory exhaustion.
"""

from collections import defaultdict
from enum import Enum
from mosaicolabs.models.message import Message
import pyarrow.flight as fl
import pyarrow as pa
import pyarrow.ipc as pa_ipc
from typing import List, Optional

from concurrent.futures import ThreadPoolExecutor, Future, wait
from threading import BoundedSemaphore, Lock

from mosaicolabs.models import Serializable
from mosaicolabs.enum import SerializationFormat
from mosaicolabs.logging_config import get_logger
from ...comm.connection import PYARROW_OUT_OF_RANGE_BYTES


# Set the hierarchical logger
logger = get_logger(__name__)


class _UploadMode(Enum):
    """Determines the buffering strategy: by accumulated byte size or by record count."""

    Bytes = "bytes"
    Count = "count"


# Map ontology format types to their optimal upload strategy
_SERIALIZATION_FORMAT_TO_UPLOAD_MODE = {
    SerializationFormat.Image: _UploadMode.Bytes,  # Heavy data -> Limit by bytes
    SerializationFormat.Default: _UploadMode.Bytes,  # Light data -> Limit by count
    SerializationFormat.Ragged: _UploadMode.Bytes,
}


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
    1.  **Buffering**: Accumulates `Message` objects in `_current_data_batch`.
        -   Uses **Bytes Mode** for heavy data (Images) to respect Flight chunk limits.
        -   Uses **Count Mode** for light data (IMU, Odometry) for efficiency.
    2.  **Async Dispatch**: Offloads serialization and network I/O to a `ThreadPoolExecutor`.
    3.  **Backpressure**: Uses a `BoundedSemaphore` to limit the number of pending
        async tasks. If the network is slower than the data producer, `push_record()`
        will eventually block, naturally throttling the application.
    """

    def __init__(
        self,
        topic_name: str,
        ontology_tag: str,
        writer: Optional[fl.FlightStreamWriter],
        executor: Optional[ThreadPoolExecutor] = None,
        max_batch_size_bytes: Optional[int] = None,
        max_batch_size_records: Optional[int] = None,
    ):
        """
        Initializes the write state.

        Args:
            topic_name (str): Topic name.
            ontology_tag (str): Data ontology tag for schema resolution.
            writer (Optional[fl.FlightStreamWriter]): Active Flight stream writer.
            executor (Optional[ThreadPoolExecutor]): Executor for async operations.
            max_batch_size_bytes (Optional[int]): flush threshold for byte mode.
            max_batch_size_records (Optional[int]): flush threshold for count mode.
        """
        if writer is None:
            raise ValueError("Cannot initialize _TopicState: 'writer' is None.")

        # Safety Check: Ensure configured limit is within PyArrow's hard limit (4MB usually)
        if (
            max_batch_size_bytes is not None
            and max_batch_size_bytes > PYARROW_OUT_OF_RANGE_BYTES * 0.9
        ):
            raise ValueError(
                f"'max_batch_size_bytes' must be strictly less than 90% of max allowable limit {PYARROW_OUT_OF_RANGE_BYTES}."
            )

        self.topic_name: str = topic_name
        self.writer: Optional[fl.FlightStreamWriter] = writer
        self.ontology_tag: str = ontology_tag
        self.executor: Optional[ThreadPoolExecutor] = executor
        self.max_batch_size_bytes = max_batch_size_bytes
        self.max_batch_size_records = max_batch_size_records

        # Resolve Ontology Class for serialization schema
        self.ontology_type = Serializable._get_class_type(ontology_tag)
        if self.ontology_type is None:
            raise RuntimeError(
                f"Ontology class for tag '{ontology_tag}' not registered in Message."
            )

        if self.max_batch_size_bytes is None or self.max_batch_size_records is None:
            raise RuntimeError(
                "'max_batch_size_bytes' AND 'max_batch_size_records' must be provided."
            )

        # --- Buffering State ---
        self._current_data_batch: List[Message] = []
        self._current_batch_size_bytes: int = 0

        # --- Async & Backpressure State ---
        self._pending_writes: List[Future] = []
        self._pending_writes_lock = Lock()
        self._written_records = 0
        self._pushed_records = 0

        # Backpressure Settings:
        # Allow max 3 pending batches. The 4th attempt will block the main thread.
        self.max_pending_batches = 3
        # Use BoundedSemaphore to manage shared I/O writing channel.
        # Waiting (blocking) mechanism when length of pending batches is more than limit
        self._pending_sem = BoundedSemaphore(self.max_pending_batches)

    def _get_record_batch(self, msgs: List[Message]) -> pa.RecordBatch:
        """
        [CPU Bound] Converts Python objects to Arrow RecordBatch.
        Runs in worker thread during async mode.
        """
        if self.writer is None:
            raise ValueError("Writer is None")
        assert self.ontology_type is not None

        return pa.RecordBatch.from_pydict(
            _encode_messages(msgs),
            schema=Message._get_schema(self.ontology_type),
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

    def _push_by_bytes_size(self, msg: Message):
        """
        Buffer logic for Byte-Mode topics (e.g., Images).

        1. Serializes the *single* new record to check its size.
        2. If adding it exceeds `max_batch_size_bytes`, flushes current buffer.
        3. Adds record to new buffer.
        """
        assert self.writer is not None
        assert self.max_batch_size_bytes is not None

        # Measure size of the new message
        single_record_batch = self._get_record_batch([msg])
        single_record_size = self._get_serialized_size(single_record_batch)

        # TODO: Try finding solutions for the case in which the single record
        # is beyond pyarrow transmission limits! Log for now.
        if single_record_size > PYARROW_OUT_OF_RANGE_BYTES:
            logger.error(
                f"Single record size ({single_record_size} bytes) exceeds PyArrow limit "
                f"({PYARROW_OUT_OF_RANGE_BYTES} bytes) for topic '{self.topic_name}'. "
                "Record will be skipped."
            )
            return

        # Check Buffer Threshold
        projected_size = self._current_batch_size_bytes + single_record_size

        if projected_size > self.max_batch_size_bytes:
            # Flush existing data
            if self._current_data_batch:
                self._write_current_batch()

            # Handle edge case: Single record > Preferred batch size
            # It will be added as a batch of 1.

            self._current_data_batch = [msg]
            self._current_batch_size_bytes = single_record_size
        else:
            self._current_data_batch.append(msg)
            self._current_batch_size_bytes += single_record_size

    def _push_by_count(self, msg: Message):
        """
        Buffer logic for Count-Mode topics.

        Simply counts records and flushes when `max_batch_size_records` is reached.
        """
        assert self.writer is not None
        assert self.max_batch_size_records is not None

        self._current_data_batch.append(msg)

        if len(self._current_data_batch) >= self.max_batch_size_records:
            self._write_current_batch()

    def push_record(self, msg: Message):
        """
        Adds a record to the buffer.

        Automatically delegates to `_push_by_bytes_size` or `_push_by_count`
        based on the ontology type defined in the message.
        """
        if self.writer is None:
            raise ValueError("write() called on uninitialized state.")

        mode = _SERIALIZATION_FORMAT_TO_UPLOAD_MODE.get(
            msg.data.__serialization_format__
        )

        if mode == _UploadMode.Bytes:
            self._push_by_bytes_size(msg)
        else:
            self._push_by_count(msg)

        self._pushed_records += 1

    def _submit_write_task(self, msgs_to_write: List[Message]):
        """
        Dispatches the write operation to the executor.

        **Backpressure Logic:**
        Calls `self._pending_sem.acquire()`. If 3 tasks are already pending,
        this call BLOCKS, pausing the main thread until a worker finishes.
        """
        if self.writer is None:
            logger.error(
                f"Cannot write batch for topic '{self.topic_name}'. Writer is None."
            )
            return

        # Worker Function
        def full_write_task(records, topic_name, sem: Optional[BoundedSemaphore]):
            try:
                # Serialization (CPU)
                batch = self._get_record_batch(records)
                # Transmission (IO)
                assert self.writer is not None
                self.writer.write(batch)
            except Exception as e:
                logger.error(f"Async write failed for topic '{topic_name}': '{e}'")
            finally:
                # Release Semaphore (Unblock main thread, if blocked)
                if sem:
                    sem.release()

        if self.executor is not None:
            # Backpressure Gate
            # Attempt to acquire a slot. If the queue is full (max_pending_batches reached),
            # this call blocks the main thread, effectively throttling the data producer.
            self._pending_sem.acquire()

            future = self.executor.submit(
                full_write_task, msgs_to_write, self.topic_name, self._pending_sem
            )

            # Resource Management
            # Define a callback to automatically remove the future from the tracking list
            # once execution completes, preventing infinite list growth.
            def cleanup_callback(fut):
                with self._pending_writes_lock:
                    try:
                        self._pending_writes.remove(fut)
                    except ValueError:
                        pass  # Future already removed

            future.add_done_callback(cleanup_callback)

            with self._pending_writes_lock:
                self._pending_writes.append(future)

        else:
            # Sync Path: Run immediately on main thread
            full_write_task(msgs_to_write, self.topic_name, None)

        self._written_records += len(msgs_to_write)

    def _write_current_batch(self):
        """
        Flushes buffer: transfers data ownership to async task and resets buffer.
        """
        if self.writer is None:
            raise ValueError("Writer is None")

        if self._current_data_batch:
            records = self._current_data_batch

            # Reset immediately
            self._current_data_batch = []
            self._current_batch_size_bytes = 0

            self._submit_write_task(records)

    def _wait_for_pending_writes(self):
        """
        Blocks until all async tasks are complete.
        """
        if self.writer is not None and self.executor is not None:
            logger.info(
                f"Waiting for pending writes termination, for topic '{self.topic_name}'..."
            )

            with self._pending_writes_lock:
                futures = list(self._pending_writes)

            if futures:
                wait(futures)

            # Check for silent failures
            for f in futures:
                if f.exception():
                    logger.error(f"Async write error: '{f.exception()}'")

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
                    self._wait_for_pending_writes()

                self.writer.done_writing()
                logger.info(
                    f"Topic '{self.topic_name}' finished. "
                    f"Pushed: {self._pushed_records}, Written: {self._written_records}"
                )
            finally:
                self.writer.close()
                self.writer = None
