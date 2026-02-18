"""
Sequence Reading Module.

This module provides the `SequenceDataStreamer`, which reads an entire sequence
by merging multiple topic streams into a single, time-ordered iterator.
"""

import json
from mosaicolabs.models.message import Message
import pyarrow.flight as fl
from typing import Any, List, Optional, Dict

from .endpoints import TopicParsingError, TopicResourceManifest
from .internal.topic_read_state import _TopicReadState
from .topic_reader import TopicDataStreamer
from ..logging_config import get_logger

# Set the hierarchical logger
logger = get_logger(__name__)


class SequenceDataStreamer:
    """
    A unified, time-ordered iterator for reading multi-topic sequences.

    The `SequenceDataStreamer` performs a **K-Way Merge** across multiple topic streams to
    provide a single, coherent chronological view of an entire sequence.
    This is essential when topics have different recording rates or asynchronous sampling
    times.

    ### Key Capabilities
    * **Temporal Slicing**: Supports server-side filtering to stream data within specific
        time windows (t >= start and t < end).
    * **Peek-Ahead**: Provides the `next_timestamp()` method, allowing the system
        to inspect chronological order without consuming the record—a core requirement
        for the K-way merge sorting algorithm.

    ### The Merge Algorithm
    This class manages multiple internal [`TopicDataStreamer`][mosaicolabs.handlers.TopicDataStreamer]
    instances. On every iteration, it:

    1. **Peeks** at the next available timestamp from every active topic stream.
    2. **Selects** the topic currently holding the lowest absolute timestamp.
    3. **Yields** that specific record and advances only the "winning" topic stream.

    Tip: Obtaining a Streamer
        Do not instantiate this class directly. Use the
        [`SequenceHandler.get_data_streamer()`][mosaicolabs.handlers.SequenceHandler.get_data_streamer]
        method to obtain a configured instance.
    """

    def __init__(
        self,
        *,
        sequence_name: str,
        client: fl.FlightClient,
        topic_readers: Dict[str, TopicDataStreamer],
    ):
        """
        Internal constructor for SequenceDataStreamer.

        **Do not call this directly.** Internal library modules should use the
        `SequenceDataStreamer._connect()` factory.

        Example:
            ```python
            from mosaicolabs import MosaicoClient

            with MosaicoClient.connect("localhost", 6726) as client:
                # Use a Handler to inspect the catalog
                seq_handler = client.sequence_handler("mission_alpha")
                if seq_handler:
                    # Start a Unified Stream (K-Way Merge) for multi-sensor replay
                    streamer = seq_handler.get_data_streamer(
                        topics=["/gps", "/imu"], # Optionally filter topics
                        # Optionally set the time window to extract
                        start_timestamp_ns=1738508778000000000,
                        end_timestamp_ns=1738509618000000000
                    )

                    # Start timed data-stream
                    for topic, msg in streamer:
                        # Do some processing...

                    # Once done, close the resources, topic handler and related reading channels (recommended).
                    seq_handler.close()
            ```

        Args:
            sequence_name: The name of the sequence being streamed.
            client: The active FlightClient for remote operations.
            topic_readers: A dictionary mapping topic names to their respective
                [`TopicDataStreamer`][mosaicolabs.handlers.TopicDataStreamer] instances.
        """
        self._name: str = sequence_name
        """The name of the handled sequence data stream"""
        self._fl_client: fl.FlightClient = client
        "The client for remote operations"
        self._topic_readers: Dict[str, TopicDataStreamer] = topic_readers
        """The spawned topic data stream readers"""
        self._winning_rdstate: Optional[_TopicReadState] = None
        """The current topic datastream state corresponding to the last extracted measurement"""
        self._in_iter: bool = False
        """Tag for assessing if the data streamer is used in a loop"""
        self._is_open: bool = True
        """Tag for assessing the internal streamer status"""

    @classmethod
    def _connect(
        cls,
        sequence_name: str,
        topics: List[str],
        start_timestamp_ns: Optional[int],
        end_timestamp_ns: Optional[int],
        client: fl.FlightClient,
    ) -> "SequenceDataStreamer":
        """
        Internal factory method to initialize the Sequence reader merger.

        This method queries the server for the sequence's resource endpoints and
        establishes individual data streams for each requested topic.

        Important: **Do not call this directly**
            Users must use  the
            [`SequenceHandler.get_data_streamer()`][mosaicolabs.handlers.SequenceHandler.get_data_streamer]
            method to obtain a configured instance.

        Args:
            sequence_name: The sequence to read.
            topics: A whitelist of topic names to include in the stream.
                Other topics in the sequence will be ignored.
            start_timestamp_ns: Optional inclusive lower bound for temporal slicing.
            end_timestamp_ns: Optional exclusive upper bound for temporal slicing.
            client: An established PyArrow Flight connection.

        Returns:
            SequenceDataStreamer: An initialized merger ready for iteration.

        Raises:
            ConnectionError: If the server-side flight descriptor cannot be retrieved.
            RuntimeError: If no valid topic handlers could be opened for the sequence.
        """
        try:
            flight_info = cls._get_flight_info(
                sequence_name=sequence_name,
                start_timestamp_ns=start_timestamp_ns,
                end_timestamp_ns=end_timestamp_ns,
                client=client,
            )
        except Exception as e:
            raise ConnectionError(
                f"Server error (get_flight_info) while asking for Sequence descriptor, {e}"
            )

        topic_readers: Dict[str, TopicDataStreamer] = {}

        # Extract the Topics resource manifests data and their tickets
        for ep in flight_info.endpoints:
            try:
                topic_resrc_mdata = TopicResourceManifest.from_flight_endpoint(ep)
            except TopicParsingError as e:
                logger.error(f"Skipping invalid topic endpoint, err: '{e}'")
                continue
            # Skip topics with no data
            if (
                topic_resrc_mdata.timestamp_ns_min is None
                or topic_resrc_mdata.timestamp_ns_max is None
            ):
                continue
            # If not in the selected topics
            if topics and topic_resrc_mdata.topic_name not in topics:
                continue
            treader = TopicDataStreamer._connect_from_ticket(
                client=client,
                topic_name=topic_resrc_mdata.topic_name,
                ticket=ep.ticket,
            )
            # Cache the topic reader instance
            topic_readers[treader.name()] = treader

        if not topic_readers:
            raise RuntimeError(
                f"Unable to open TopicDataStreamer handlers for sequence '{sequence_name}'"
            )

        return cls(
            sequence_name=sequence_name,
            client=client,
            topic_readers=topic_readers,
        )

    def _validate_status_open(self):
        if not self._is_open:
            raise ValueError(f"Reader closed for sequence {self._name}")

    def _validate_status_not_in_iter(self):
        if self._in_iter:
            raise RuntimeError(
                "Cannot switch to batch provider mode: row-by-row iteration has already started. "
                "You must decide between streaming (loops) or batch processing (analytics) "
                "at the beginning of the session."
            )

    # --- Iterator Protocol Implementation ---

    def __iter__(self) -> "SequenceDataStreamer":
        """
        Initializes the K-Way merge iterator.

        This pre-loads the first row of every topic stream to prepare the initial
        comparison step.
        """
        self._validate_status_open()
        for treader in self._topic_readers.values():
            if treader._rdstate.peeked_row is None:
                treader._rdstate.peek_next_row()
        self._in_iter = True
        return self

    def next_timestamp(self) -> Optional[int]:
        """
        Peeks at the timestamp of the next chronological measurement without
        consuming the record.

        Returns:
            The minimum timestamp (nanoseconds) found across all active topics,
                or `None` if all streams are exhausted.

        Example:
            ```python
            from mosaicolabs import MosaicoClient

            with MosaicoClient.connect("localhost", 6726) as client:
                # Use a Handler to inspect the catalog
                seq_handler = client.sequence_handler("mission_alpha")
                if seq_handler:
                    # Start a Unified Stream (K-Way Merge) for multi-sensor replay
                    streamer = seq_handler.get_data_streamer(
                        topics=["/gps", "/imu"], # Optionally filter topics
                        # Optionally set the time window to extract
                        start_timestamp_ns=1738508778000000000,
                        end_timestamp_ns=1738509618000000000
                    )

                    # Peek at the start time (without consuming data)
                    print(f"Recording starts at: {streamer.next_timestamp()}")

                    # Do some processing...

                    # Once done, close the resources, topic handler and related reading channels (recommended).
                    seq_handler.close()
            ```

        """
        self._validate_status_open()
        self._in_iter = (
            True  # Safety: ensures direct next_timestamp() calls also lock the state
        )
        min_tstamp: float = float("inf")

        for treader in self._topic_readers.values():
            if treader._rdstate.peeked_row is None:
                treader._rdstate.peek_next_row()

            # Compare current topic's next timestamp against global min
            if treader._rdstate.peeked_timestamp < min_tstamp:
                min_tstamp = treader._rdstate.peeked_timestamp

        if min_tstamp == float("inf"):
            return None

        return int(min_tstamp)

    def __next__(self) -> tuple[str, Message]:
        """
        Executes the merge step to return the next chronological record.

        Returns:
            tuple[str, Message]: A tuple containing the `topic_name` and the
                reconstructed [`Message`][mosaicolabs.models.Message] object.

        Raises:
            StopIteration: If all underlying topic streams are exhausted.
        """
        self._validate_status_open()
        min_tstamp: float = float("inf")
        topic_min_tstamp: Optional[str] = None
        self._winning_rdstate = None

        # Identify the "Winner" (Topic with lowest timestamp)
        for topic_name, treader in self._topic_readers.items():
            if treader._rdstate.peeked_row is None:
                treader._rdstate.peek_next_row()

            if treader._rdstate.peeked_timestamp < min_tstamp:
                min_tstamp = treader._rdstate.peeked_timestamp
                topic_min_tstamp = topic_name

        # Check termination condition
        if topic_min_tstamp is None or min_tstamp == float("inf"):
            raise StopIteration

        # Retrieve data from Winner
        self._winning_rdstate = self._topic_readers[topic_min_tstamp]._rdstate
        assert self._winning_rdstate.peeked_row is not None

        row_values = self._winning_rdstate.peeked_row
        row_dict = {
            name: value.as_py()
            for name, value in zip(self._winning_rdstate.column_names, row_values)
        }

        # Advance the Winner's stream
        self._winning_rdstate.peek_next_row()

        return self._winning_rdstate.topic_name, Message._create(
            self._winning_rdstate.ontology_tag, **row_dict
        )

    @staticmethod
    def _get_flight_info(
        sequence_name: str,
        start_timestamp_ns: Optional[int],
        end_timestamp_ns: Optional[int],
        client: fl.FlightClient,
    ) -> fl.FlightInfo:
        """Performs the get_flight_info call. Raises if flight function does"""
        cmd_dict: dict[str, Any] = {"resource_locator": sequence_name}
        if start_timestamp_ns is not None:
            cmd_dict.update({"timestamp_ns_start": start_timestamp_ns})
        if end_timestamp_ns is not None:
            cmd_dict.update({"timestamp_ns_end": end_timestamp_ns})

        descriptor = fl.FlightDescriptor.for_command(json.dumps(cmd_dict))
        return client.get_flight_info(descriptor)

    def _as_batch_provider(self) -> Dict[str, "TopicDataStreamer"]:
        """
        Transitions the streamer to 'Batch Provider' mode for analytical modules.

        This internal helper is designed to facilitate high-performance data extraction
        (e.g., by MosaicoFrameExtractor). It ensures the stream is in a 'clean' state
        (not partially consumed) before returning the internal topic readers, avoiding
        inconsistent data states between row-based and batch-based processing.

        Returns:
            Dict[str, TopicDataStreamer]: A mapping of topic names to their internal streamers.

        Raises:
            RuntimeError: If row-by-row iteration has already commenced.
        """
        self._validate_status_open()
        self._validate_status_not_in_iter()

        return self._topic_readers

    def close(self):
        """
        Gracefully terminates all underlying topic streams and releases allocated resources.

        This method iterates through all active [`TopicDataStreamer`][mosaicolabs.handlers.TopicDataStreamer]
        instances, ensuring that each remote connection is closed and local memory buffers
        are cleared.

        Note: Automatic Cleanup
            In standard workflows, you do not need to call this manually. This function is
            **automatically invoked** by the [`SequenceHandler.close()`][mosaicolabs.handlers.SequenceHandler.close]
            method, which in turn is triggered by the `__exit__` logic of the parent
            [`SequenceHandler`][mosaicolabs.handlers.SequenceHandler] when used within a
            `with` context.


        """
        self._is_open = False
        for treader in self._topic_readers.values():
            try:
                treader.close()
            except Exception as e:
                logger.warning(f"Error closing state '{treader.name()}': '{e}'")

        logger.info(f"SequenceReader for '{self._name}' closed.")
