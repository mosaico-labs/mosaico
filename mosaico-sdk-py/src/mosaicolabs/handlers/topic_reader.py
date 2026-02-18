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
    An iterator that streams ontology records from a single topic.

    The `TopicDataStreamer` wraps a PyArrow Flight `DoGet` stream to fetch `RecordBatches`
    from the server and reconstruct individual [`Message`][mosaicolabs.models.Message] objects.
    It is designed for efficient row-by-row iteration while providing peek-ahead
    capabilities for time-synchronized merging.

    ### Key Capabilities
    * **Temporal Slicing**: Supports server-side filtering to stream data within specific
        time windows (t >= start and t < end).
    * **Peek-Ahead**: Provides the `next_timestamp()` method, allowing the system
        to inspect chronological order without consuming the record—a core requirement
        for the K-way merge sorting performed by the [`SequenceDataStreamer`][mosaicolabs.handlers.SequenceDataStreamer].

    Important: Obtaining a Streamer
        Users should typically not instantiate this class directly.
        The recommended way to obtain a streamer is via the
        [`TopicHandler.get_data_streamer()`][mosaicolabs.handlers.TopicHandler.get_data_streamer]
        method.
    """

    def __init__(
        self,
        *,
        client: fl.FlightClient,
        state: _TopicReadState,
    ):
        """
        Internal constructor for TopicDataStreamer.

        **Do not call this directly.** Internal library modules should use the
        `TopicDataStreamer._connect()` or `TopicDataStreamer._connect_from_ticket()` factory methods instead.

        Example:
            ```python
            from mosaicolabs import MosaicoClient, IMU

            with MosaicoClient.connect("localhost", 6726) as client:
                # Retrieve the topic handler using (e.g.) MosaicoClient
                top_handler = client.topic_handler("mission_alpha", "/front/imu")
                if top_handler:
                    imu_stream = top_handler.get_data_streamer(
                        # Optionally set the time window to extract
                        start_timestamp_ns=1738508778000000000,
                        end_timestamp_ns=1738509618000000000
                    )

                    # Peek at the start time (without consuming data)
                    print(f"Recording starts at: {streamer.next_timestamp()}")

                    # Direct, low-overhead loop
                    for imu_msg in imu_stream:
                        # Do some processing...

                    # Once done, close the reading channel (recommended)
                    top_handler.close()

            ```

        Args:
            client: The active FlightClient used for remote operations.
            state: The internal state object managing the Arrow reader and peek buffers.
        """
        self._fl_client: fl.FlightClient = client
        """The FlightClient used for remote operations."""
        self._rdstate: _TopicReadState = state
        """The actual reader object"""
        self._is_open: bool = True
        """Tag for assessing the internal streamer status"""

    @classmethod
    def _connect_from_ticket(
        cls,
        client: fl.FlightClient,
        topic_name: str,
        ticket: fl.Ticket,
    ) -> "TopicDataStreamer":
        """
        Factory method to initialize a streamer using a pre-existing Flight Ticket.

        This is the primary entry point for internal handlers that have already
        performed resource discovery via `get_flight_info`.

        Important: **Do not call this directly**
            Users must use  the
            [`TopicHandler.get_data_streamer()`][mosaicolabs.handlers.TopicHandler.get_data_streamer]
            method to obtain a configured instance.

        Args:
            client: An established PyArrow Flight connection.
            topic_name: The name of the topic to read.
            ticket: The opaque authorization ticket representing the specific data stream.

        Returns:
            An initialized `TopicDataStreamer` ready for iteration.

        Raises:
            ConnectionError: If the server fails to open the `do_get` stream.
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
    def _connect(
        cls,
        topic_name: str,
        sequence_name: str,
        client: fl.FlightClient,
        start_timestamp_ns: Optional[int],
        end_timestamp_ns: Optional[int],
    ) -> "TopicDataStreamer":
        """
        Factory method to initialize a streamer via an endpoint with optional temporal slicing.

        This method performs its own resource discovery to identify the correct
        endpoint and ticket before opening the data stream.

        Important: **Do not call this directly**
            Users must use  the
            [`TopicHandler.get_data_streamer()`][mosaicolabs.handlers.TopicHandler.get_data_streamer]
            method to obtain a configured instance.

        Args:
            topic_name: The name of the topic to read.
            sequence_name: The name of the parent sequence.
            client: An established PyArrow Flight connection.
            start_timestamp_ns: The **inclusive** lower bound (t >= start) in nanoseconds.
            end_timestamp_ns: The **exclusive** upper bound (t < end) in nanoseconds.

        Returns:
            An initialized `TopicDataStreamer`.

        Raises:
            ConnectionError: If `get_flight_info` or `do_get` fail on the server.
            ValueError: If the topic cannot be found within the specified sequence.
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
                return cls._connect_from_ticket(
                    client=client,
                    topic_name=topic_name,
                    ticket=ep.ticket,
                )

        raise ValueError("Unable to init TopicDataStreamer")

    def _validate_status_open(self):
        if not self._is_open:
            raise ValueError(f"Reader closed for topic {self.name()}")

    def name(self) -> str:
        """
        The name of the topic associated with this streamer.

        Returns:
            The name of the topic.
        """
        return self._rdstate.topic_name

    def next_timestamp(self) -> Optional[int]:
        """
        Peeks at the timestamp of the next record without consuming it.

        Returns:
            The next timestamp in nanoseconds, or `None` if the stream is empty.

        Raises:
            ValueError: if the data streamer instance has been closed.

        Example:
            ```python
            from mosaicolabs import MosaicoClient, IMU

            with MosaicoClient.connect("localhost", 6726) as client:
                # Retrieve the topic handler using (e.g.) MosaicoClient
                top_handler = client.topic_handler("mission_alpha", "/front/imu")
                if top_handler:
                    imu_stream = top_handler.get_data_streamer(
                        # Optionally set the time window to extract
                        start_timestamp_ns=1738508778000000000,
                        end_timestamp_ns=1738509618000000000
                    )

                    # Peek at the start time (without consuming data)
                    print(f"Recording starts at: {streamer.next_timestamp()}")

                    # Do some processing...

                    # Once done, close the reading channel (recommended)
                    top_handler.close()
            ```
        """
        self._validate_status_open()

        if self._rdstate.peeked_row is None:
            # Load the next row into the buffer
            if not self._rdstate.peek_next_row():
                return None

        # Check for end-of-stream sentinel
        if self._rdstate.peeked_timestamp == float("inf"):
            return None

        return int(self._rdstate.peeked_timestamp)

    @property
    def ontology_tag(self) -> str:
        """
        The ontology tag associated with this streamer.

        Returns:
            The ontology tag.
        """
        return self._rdstate.ontology_tag

    def __iter__(self) -> "TopicDataStreamer":
        """Returns self as iterator."""
        self._validate_status_open()
        return self

    def __next__(self) -> Message:
        """
        Iterates the stream to return the next chronological message.

        Returns:
            Message: The reconstructed message object.

        Raises:
            StopIteration: When the data stream is exhausted.
        """
        self._validate_status_open()
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

        return Message._create(self._rdstate.ontology_tag, **row_dict)

    def close(self):
        """
        Gracefully terminates the underlying Apache Arrow Flight stream and releases buffers.

        Note: Automatic Lifecycle Management
            In most production workflows, manual invocation is not required. This method is
            **automatically called** by the parent [`TopicHandler.close()`][mosaicolabs.handlers.TopicHandler.close].
            If the handler is managed within a `with` context, the SDK ensures a top-down cleanup
            of the handler and its associated streamers upon exit.

        Example:
            ```python
            # Manual resource management (if not using 'with' block)
            streamer = topic_handler.get_data_streamer()
            try:
                for meas in streamer:
                    process_robot_data(meas)
            finally:
                streamer.close()
            ```
        """
        self._is_open = False
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
        self._validate_status_open()
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
