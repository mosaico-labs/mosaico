"""
Topic Reading Module.

This module provides the `TopicDataStreamer`, an iterator that reads ontology records
from a single topic via the Flight `DoGet` protocol.
"""

import json
from mosaicolabs.handlers.endpoints import TopicParsingError, TopicResourceManifest
from mosaicolabs.models.message import Message
import pyarrow.flight as fl
import pyarrow as pa
from typing import Any, Optional

from .internal.topic_read_state import _TopicReadState

from ..comm.metadata import TopicMetadata, _decode_metadata
from ..helpers.helpers import pack_topic_resource_name
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

    def __init__(
        self,
        *,
        client: fl.FlightClient,
        state: _TopicReadState,
    ):
        """
        Internal constructor.
        Users can retrieve an instance by using 'get_data_streamer()` from a TopicHandler instance instead.
        Internal library modules will call the 'connect()' function.
        """
        self._fl_client: fl.FlightClient = client
        """The FlightClient used for remote operations."""
        self._rdstate: _TopicReadState = state
        """The actual reader object"""

    @classmethod
    def connect_from_ticket(
        cls,
        client: fl.FlightClient,
        topic_name: str,
        ticket: fl.Ticket,
    ) -> "TopicDataStreamer":
        """
        Factory method to initialize a streamer, starting from a flight Ticket

        Args:
            client (fl.FlightClient): Connected Flight client.
            topic_name (str): The name of the topic.
            ticket (fl.Ticket): The opaque ticket (from `get_flight_info`) representing the data stream.

        Returns:
            TopicDataStreamer: An initialized reader.

        Raises:
            ConnectionError: if 'do_get' fails
        """
        # Initialize the Flight stream (DoGet)
        try:
            reader = client.do_get(ticket)
        except Exception as e:
            raise ConnectionError(
                f"Server error (do_get) while asking for Topic data reader, '{e}'"
            )

        # Decode metadata to determine how to deserialize the data
        topic_mdata = TopicMetadata.from_dict(_decode_metadata(reader.schema.metadata))
        ontology_tag = topic_mdata.properties.ontology_tag

        rdstate = _TopicReadState(
            topic_name=topic_name,
            reader=reader,
            ontology_tag=ontology_tag,
        )
        return cls(
            client=client,
            state=rdstate,
        )

    @classmethod
    def connect(
        cls,
        topic_name: str,
        sequence_name: str,
        client: fl.FlightClient,
        start_timestamp_ns: Optional[int],
        end_timestamp_ns: Optional[int],
    ):
        """
        Factory method to initialize a streamer, via endpoint.

        Args:
            client (fl.FlightClient): Connected Flight client.
            topic_name (str): The name of the topic.
            sequence_name (str): The name of the parent sequence.
            start_timestamp_ns (Optional[int]): The **inclusive** lower bound for the time window (in nanoseconds).
                The stream will begin from the message with the timestamp **greater than or equal to** this value.
            end_timestamp_ns (Optional[int]): The **exclusive** upper bound for the time window (in nanoseconds).
                The stream will stop at the message with the timestamp **strictly lower than** this value.

        Returns:
            TopicDataStreamer: An initialized reader.

        Raises:
            ConnectionError: if 'get_flight_info' or 'do_get' fail
        """

        # Get FlightInfo (here we need just the Endpoints)
        try:
            flight_info = cls._get_flight_info(
                sequence_name=sequence_name,
                topic_name=topic_name,
                start_timestamp_ns=start_timestamp_ns,
                end_timestamp_ns=end_timestamp_ns,
                client=client,
            )
        except Exception as e:
            raise ConnectionError(
                f"Server error (get_flight_info) while asking for Topic descriptor (in TopicDataStreamer), {e}"
            )
        for ep in flight_info.endpoints:
            try:
                topic_resrc_mdata = TopicResourceManifest.from_flight_endpoint(ep)
            except TopicParsingError as e:
                logger.error(f"Skipping invalid topic endpoint, err: '{e}'")
                continue
            if topic_resrc_mdata.topic_name == topic_name:
                return cls.connect_from_ticket(
                    client=client,
                    topic_name=topic_name,
                    ticket=ep.ticket,
                )

        raise ValueError("Unable to init TopicDataStreamer")

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

    def _fetch_next_batch(self) -> Optional[pa.RecordBatch]:
        """
        Retrieves the next raw RecordBatch from the underlying stream.

        This is a library-internal bridge designed for high-performance
        batch processing. It bypasses the standard row-by-row iteration
        to provide direct access to columnar data.

        Returns:
            Optional[pa.RecordBatch]: The next available Arrow RecordBatch,
                or None if the stream is exhausted.

        Note:
            Calling this method advances the internal stream state and
            will interfere with the standard iteration (`next()`) if
            used concurrently.
        """
        return self._rdstate.fetch_next_batch()

    @staticmethod
    def _get_flight_info(
        sequence_name: str,
        topic_name: str,
        start_timestamp_ns: Optional[int],
        end_timestamp_ns: Optional[int],
        client: fl.FlightClient,
    ) -> fl.FlightInfo:
        """Performs the get_flight_info call. Raises if flight function does"""
        topic_resrc_name = pack_topic_resource_name(sequence_name, topic_name)
        cmd_dict: dict[str, Any] = {"resource_locator": topic_resrc_name}
        if start_timestamp_ns is not None:
            cmd_dict.update({"timestamp_ns_start": start_timestamp_ns})
        if end_timestamp_ns is not None:
            cmd_dict.update({"timestamp_ns_end": end_timestamp_ns})

        descriptor = fl.FlightDescriptor.for_command(json.dumps(cmd_dict))

        # Get FlightInfo
        return client.get_flight_info(descriptor)
