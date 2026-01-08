"""
Internal Read State Module.

This module defines `_TopicReadState`, a helper class that encapsulates the state
of an active PyArrow Flight stream reading session. It manages the underlying
`FlightStreamReader`, buffers `RecordBatches`, and provides row-by-row iteration
capabilities essential for the k-way merge logic used in `SequenceDataStreamer`.
"""

import pyarrow.flight as fl
from typing import Iterator, List, Optional, Type

from mosaicolabs.models import Serializable
from mosaicolabs.logging import get_logger

# Set the hierarchical logger
logger = get_logger(__name__)


class _TopicReadState:
    """
    Manages the reading state for a single ontology topic.

    **Key Responsibilities:**
    1.  **Batch Management**: Reads chunks (`FlightStreamChunk`) from the Flight stream.
    2.  **Row Iteration**: Converts columnar batches into a row-wise iterator.
    3.  **Peeking**: Maintains a "peek buffer" (`peeked_row`, `peeked_timestamp`) to allow
        the `SequenceDataStreamer` to inspect the next available timestamp without
        consuming the data, enabling time-ordered merging of multiple streams.
    """

    def __init__(
        self,
        topic_name: str,
        ontology_tag: str,
        reader: Optional[fl.FlightStreamReader],
    ):
        """
        Initializes the read state.

        Args:
            topic_name (str): The name of the topic.
            ontology_tag (str): The identifier for the ontology data type.
            reader (Optional[fl.FlightStreamReader]): The active stream reader.

        Raises:
            ValueError: If `reader` is None or if the schema lacks a 'timestamp' column.
        """
        if reader is None:
            raise ValueError("Cannot initialize _TopicState: 'reader' is None.")

        self.topic_name: str = topic_name
        self.reader: Optional[fl.FlightStreamReader] = reader
        self.ontology_tag: str = ontology_tag

        # Writer-specific fields (unused in reader context but kept for structure alignment)
        self.ontology_type: Optional[Type[Serializable]] = None
        self.field_names: Optional[List[str]] = None

        # --- Schema Validation & Setup ---
        self.column_names: List[str] = []
        self.timestamp_index: int = -1

        self.column_names = reader.schema.names
        try:
            self.timestamp_index = self.column_names.index("timestamp_ns")
        except ValueError as e:
            raise ValueError(
                f"Topic '{topic_name}' schema is missing the required 'timestamp_ns' column."
            ) from e

        # --- Buffering & Iteration State ---
        self.current_batch: Optional[fl.FlightStreamChunk] = None

        # Iterator yields tuples of python objects: (value_col1, value_col2, ...)
        self.row_iterator: Optional[Iterator] = None

        # Peek Buffer: Stores the next row to be consumed
        self.peeked_row: Optional[tuple] = None

        # Sentinel value: 'inf' indicates stream is empty or not yet started
        self.peeked_timestamp: float = float("inf")

    def _advance_to_next_batch(self) -> bool:
        """
        Loads the next `RecordBatch` from the stream and resets the row iterator.

        Returns:
            bool: True if a new batch was loaded; False if the stream is exhausted.
        """
        if self.reader is None:
            return False

        try:
            # Fetch next chunk from Flight
            self.current_batch = self.reader.read_chunk()
            current_batch = self.current_batch

            if current_batch.data is None or current_batch.data.num_rows == 0:
                self.row_iterator = None
                return False

            # Efficiently transpose columnar data to row iterator
            # columns = [col_array_1, col_array_2, ...]
            columns = [
                current_batch.data.column(i)
                for i in range(current_batch.data.num_columns)
            ]
            self.row_iterator = iter(zip(*columns))
            return True

        except StopIteration:
            # Normal end of stream
            self.row_iterator = None
            return False
        except Exception:
            # Unexpected error
            self.row_iterator = None
            raise

    def peek_next_row(self) -> bool:
        """
        Populates `self.peeked_row` with the next available data point.

        This method handles the transition between batches automatically. If the
        current batch iterator is exhausted, it calls `_advance_to_next_batch()`
        recursively until data is found or the stream ends.

        Returns:
            bool: True if a row is available; False if the stream is fully exhausted.
        """
        if self.reader is None:
            return False

        while True:
            # Ensure we have an active batch iterator
            if self.row_iterator is None:
                if not self._advance_to_next_batch():
                    # End of Stream reached
                    self.peeked_row = None
                    self.peeked_timestamp = float("inf")
                    return False

            try:
                assert self.row_iterator is not None

                # Get next row from current batch
                row_values = next(self.row_iterator)

                # Extract timestamp for sorting logic
                timestamp_ns = row_values[self.timestamp_index].as_py()

                # Update state
                self.peeked_row = row_values
                self.peeked_timestamp = timestamp_ns
                return True

            except StopIteration:
                # Current batch finished; loop back to try loading the next batch
                if self._advance_to_next_batch():
                    continue
                else:
                    # Stream finished after last batch
                    self.peeked_row = None
                    self.peeked_timestamp = float("inf")
                    return False
            except Exception:
                self.peeked_row = None
                self.peeked_timestamp = float("inf")
                raise

    def close(self, with_error: bool = False):
        """
        Cancels the stream reader and releases resources.
        """
        if self.reader is not None:
            try:
                self.reader.cancel()
            except Exception as e:
                logger.warning(f"Error canceling FlightStreamReader: '{e}'")
            finally:
                self.reader = None
