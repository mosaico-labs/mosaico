"""
Topic Reading Module.

This module provides the `TopicDataStreamer`, an iterator that reads ontology records
from a single topic via the Flight `DoGet` protocol.
"""

from mosaicolabs.models.message import Message
import pyarrow.flight as fl
from typing import Optional

from .internal.topic_read_state import _TopicReadState

from ..comm.metadata import TopicMetadata, _decode_metadata
from ..logging_config import get_logger

# Set the hierarchical logger
logger = get_logger(__name__)


class TopicDataStreamer:
    """
    Streams data from a single topic.

    This class wraps the PyArrow Flight reader. It fetches `RecordBatches` from the server
    and yields individual `Message` objects. It also provides a `next_timestamp` method
    to allow peek-ahead capabilities (used by sequence-level merging).
    """

    _fl_client: fl.FlightClient
    _rdstate: _TopicReadState

    def __init__(self, client: fl.FlightClient, state: _TopicReadState):
        """
        Internal constructor.
        Users can retrieve an instance by using 'get_data_streamer()` from a TopicHandler instance instead.
        Internal library modules will call the 'connect()' function.
        """
        self._fl_client = client
        self._rdstate = state

    @classmethod
    def connect(
        cls, client: fl.FlightClient, topic_name: str, ticket: fl.Ticket
    ) -> "TopicDataStreamer":
        """
        Factory method to initialize a streamer.

        Args:
            client (fl.FlightClient): Connected Flight client.
            ticket (fl.Ticket): The opaque ticket (from `get_flight_info`) representing the data stream.

        Returns:
            TopicDataStreamer: An initialized reader.
        """
        # Initialize the Flight stream (DoGet)
        reader = client.do_get(ticket)

        # Decode metadata to determine how to deserialize the data
        topic_mdata = TopicMetadata.from_dict(_decode_metadata(reader.schema.metadata))
        ontology_tag = topic_mdata.properties.ontology_tag

        rdstate = _TopicReadState(
            topic_name=topic_name,
            reader=reader,
            ontology_tag=ontology_tag,
        )
        return TopicDataStreamer(client=client, state=rdstate)

    def name(self) -> str:
        """Returns the topic name."""
        return self._rdstate.topic_name

    def next(self) -> Optional[Message]:
        """
        Returns the next message or None if finished (Non-raising equivalent of __next__).
        """
        try:
            return self.__next__()
        except StopIteration:
            return None

    def next_timestamp(self) -> Optional[float]:
        """
        Peeks at the timestamp of the next record without consuming it.

        This is used by `SequenceDataStreamer` to perform k-way merge sorting.

        Returns:
            Optional[float]: The next timestamp, or None if stream is empty.
        """
        if self._rdstate.peeked_row is None:
            # Load the next row into the buffer
            if not self._rdstate.peek_next_row():
                return None

        # Check for end-of-stream sentinel
        if self._rdstate.peeked_timestamp == float("inf"):
            return None

        return self._rdstate.peeked_timestamp

    def __iter__(self) -> "TopicDataStreamer":
        """Returns self as iterator."""
        return self

    def __next__(self) -> Message:
        """
        Iterates the stream to return the next Message.

        Raises:
            StopIteration: When the stream is exhausted.
        """
        # Ensure a row is available in the peek buffer
        if self._rdstate.peeked_row is None:
            if not self._rdstate.peek_next_row():
                raise StopIteration

        assert self._rdstate.peeked_row is not None
        row_values = self._rdstate.peeked_row

        # Convert Arrow values to Python types
        row_dict = {
            name: value.as_py()
            for name, value in zip(self._rdstate.column_names, row_values)
        }

        # Advance the buffer immediately *after* extracting the data
        self._rdstate.peek_next_row()

        return Message.create(self._rdstate.ontology_tag, **row_dict)

    def close(self):
        """Closes the underlying Flight stream."""
        try:
            self._rdstate.close()
        except Exception as e:
            logger.warning(f"Error closing state '{self._rdstate.topic_name}': '{e}'")
        logger.info(f"TopicReader for '{self._rdstate.topic_name}' closed.")
