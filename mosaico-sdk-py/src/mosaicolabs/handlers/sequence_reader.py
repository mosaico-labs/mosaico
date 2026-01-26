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
    Reads a multi-topic sequence as a unified stream.

    **Algorithm (K-Way Merge):**
    This class maintains a list of `TopicDataStreamer` instances (one per topic).
    On every iteration, it:
    1. Peeks at the next timestamp of every active topic.
    2. Selects the topic with the lowest timestamp.
    3. Yields that record and advances that specific topic's stream.

    This ensures that data is yielded in correct chronological order, even if
    the topics were recorded at different rates.
    """

    def __init__(
        self,
        *,
        sequence_name: str,
        client: fl.FlightClient,
        topic_readers: Dict[str, TopicDataStreamer],
    ):
        """
        Internal constructor.
        Users can retrieve an instance by using 'get_data_streamer()` from a SequenceHandler instance instead.
        Internal library modules will call the 'connect()' function.
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

    @classmethod
    def connect(
        cls,
        sequence_name: str,
        topics: List[str],
        start_timestamp_ns: Optional[int],
        end_timestamp_ns: Optional[int],
        client: fl.FlightClient,
    ):
        """
        Factory method to initialize the Sequence reader.

        Queries the server for all endpoints associated with the sequence and
        opens a `TopicDataStreamer` for each one.

        Args:
            sequence_name (str): The sequence to read.
            topics (list[str]): Retrieve the streams from these topics only; ignore the other.
            client (fl.FlightClient): Connected client.

        Returns:
            SequenceDataStreamer: The initialized merger.
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
            treader = TopicDataStreamer.connect_from_ticket(
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

    # --- Iterator Protocol Implementation ---

    def __iter__(self) -> "SequenceDataStreamer":
        """
        Initializes the K-Way merge by pre-loading (peeking) the first row
        of every topic.
        """
        for treader in self._topic_readers.values():
            if treader._rdstate.peeked_row is None:
                treader._rdstate.peek_next_row()
        self._in_iter = True
        return self

    def next(self) -> Optional[tuple[str, Message]]:
        """
        Returns the next time-ordered record or None if finished.
        """
        self._in_iter = True  # Safety: ensures direct next() calls also lock the state
        try:
            return self.__next__()
        except StopIteration:
            return None

    def next_timestamp(self) -> Optional[float]:
        """
        Peeks at the timestamp of the next time-ordered ontology measurement
        across all topics without consuming the element (non-destructive peek).

        Returns:
            The minimum timestamp (float) found across all active topics, or None
            if all streams are exhausted.
        """
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

        return min_tstamp

    def __next__(self) -> tuple[str, Message]:
        """
        Executes the merge step to return the next chronological record.

        Returns:
            tuple[str, Message]: A tuple containing (topic_name, message_object).

        Raises:
            StopIteration: If all underlying topic streams are exhausted.
        """
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

        return self._winning_rdstate.topic_name, Message.create(
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
        # Safety check: if _winning_rdstate is set, it means next() has been called at least once.
        if self._in_iter:
            raise RuntimeError(
                "Cannot switch to batch provider mode: row-by-row iteration has already started. "
                "You must decide between streaming (loops) or batch processing (analytics) "
                "at the beginning of the session."
            )

        return self._topic_readers

    def close(self):
        """Closes all underlying topic streams."""
        for treader in self._topic_readers.values():
            try:
                treader.close()
            except Exception as e:
                logger.warning(f"Error closing state '{treader.name()}': '{e}'")
        logger.info(f"SequenceReader for '{self._name}' closed.")
