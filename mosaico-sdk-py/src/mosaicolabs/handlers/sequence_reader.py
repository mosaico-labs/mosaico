"""
Sequence Reading Module.

This module provides the `SequenceDataStreamer`, which reads an entire sequence
by merging multiple topic streams into a single, time-ordered iterator.
"""

import json
from mosaicolabs.models.message import Message
import pyarrow.flight as fl
from typing import List, Optional, Dict

from .internal.topic_read_state import _TopicReadState
from .topic_reader import TopicDataStreamer
from .helpers import _parse_ep_ticket
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

    _name: str
    _fl_client: fl.FlightClient
    _topic_readers: Dict[str, TopicDataStreamer] = {}
    _winning_rdstate: Optional[_TopicReadState] = None

    def __init__(
        self,
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
        self._fl_client = client
        self._topic_readers = topic_readers

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
        descriptor = fl.FlightDescriptor.for_command(
            json.dumps(
                {
                    "resource_locator": sequence_name,
                    # TODO: is ok for server to receive 'null'?
                    "timestamp_ns_start": start_timestamp_ns,
                    "timestamp_ns_end": end_timestamp_ns,
                }
            )
        )
        try:
            flight_info = client.get_flight_info(descriptor)
        except Exception as e:
            raise ConnectionError(
                f"Server error while asking for Sequence descriptor, {e}"
            )

        topic_readers: Dict[str, TopicDataStreamer] = {}

        # Create a reader for each endpoint (topic)
        for ep in flight_info.endpoints:
            if len(ep.locations) != 1:
                continue
            ep_ticket_data = _parse_ep_ticket(ep.locations[0].uri)
            if ep_ticket_data is None:
                logger.error(
                    f"Skipping endpoint with invalid ticket format: '{ep.locations[0].uri}'"
                )
                continue
            if topics and ep_ticket_data[1] not in topics:
                continue
            treader = TopicDataStreamer.connect(
                client=client, topic_name=ep_ticket_data[1], ticket=ep.ticket
            )
            topic_readers[treader.name()] = treader

        if not topic_readers:
            raise RuntimeError(
                f"Unable to open TopicDataStreamer handlers for sequence '{sequence_name}'"
            )

        return cls(sequence_name, client, topic_readers)

    # --- Iterator Protocol Implementation ---

    def __iter__(self) -> "SequenceDataStreamer":
        """
        Initializes the K-Way merge by pre-loading (peeking) the first row
        of every topic.
        """
        for treader in self._topic_readers.values():
            if treader._rdstate.peeked_row is None:
                treader._rdstate.peek_next_row()
        return self

    def next(self) -> Optional[tuple[str, Message]]:
        """
        Returns the next time-ordered record or None if finished.
        """
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

    def close(self):
        """Closes all underlying topic streams."""
        for treader in self._topic_readers.values():
            try:
                treader.close()
            except Exception as e:
                logger.warning(f"Error closing state '{treader.name()}': '{e}'")
        logger.info(f"SequenceReader for '{self._name}' closed.")
