"""
Topic Handling Module.

This module provides the `TopicHandler`, which serves as a client-side handle
for an *existing* topic on the server. It allows users to inspect metadata
and create readers (`TopicDataStreamer`).
"""

import datetime
import json
import pyarrow.flight as fl
from typing import Any, Dict, Optional, Tuple

from .endpoints import TopicParsingError, TopicResourceManifest
from .topic_reader import TopicDataStreamer

from ..comm.metadata import TopicMetadata, _decode_metadata
from ..comm.do_action import _do_action, _DoActionResponseSysInfo
from ..enum import FlightAction
from ..helpers import (
    pack_topic_resource_name,
    sanitize_topic_name,
    sanitize_sequence_name,
)
from ..models.platform import Topic
from ..logging_config import get_logger

# Set the hierarchical logger
logger = get_logger(__name__)


class TopicHandler:
    """
    Represents an existing topic on the Mosaico platform.

    The `TopicHandler` provides a client-side interface for interacting with an individual
    data stream (topic). It allows users to inspect static metadata and system diagnostics (via the [`Topic`][mosaicolabs.models.platform.Topic] model),
    and access the raw message stream through a dedicated [`TopicDataStreamer`][mosaicolabs.handlers.TopicDataStreamer].

    Important: Obtaining a Handler
        Direct instantiation of this class is discouraged. Use the
        [`MosaicoClient.topic_handler()`][mosaicolabs.comm.MosaicoClient.topic_handler]
        factory method to retrieve an initialized handler.

    """

    def __init__(
        self,
        *,
        client: fl.FlightClient,
        topic_model: Topic,
        ticket: fl.Ticket,
        timestamp_ns_min: Optional[int],
        timestamp_ns_max: Optional[int],
    ):
        """
        Internal constructor for TopicHandler.

        **Do not call this directly.** Users should retrieve instances via
        [`MosaicoClient.topic_handler()`][mosaicolabs.comm.MosaicoClient.topic_handler],
        or by using the [`get_topic_handler()`][mosaicolabs.handlers.SequenceHandler.get_topic_handler] method from the
        [`SequenceHandler`][mosaicolabs.handlers.SequenceHandler] instance of the parent senquence.
        Internal modules should use the `TopicHandler._connect()` factory.

        Args:
            client: The active FlightClient for remote operations.
            topic_model: The underlying metadata and system info model for the topic.
            ticket: The remote resource ticket used for data retrieval.
            timestamp_ns_min: The lowest timestamp (in ns) available in this topic.
            timestamp_ns_max: The highest timestamp (in ns) available in this topic.
        """
        self._fl_client: fl.FlightClient = client
        """The FlightClient used for remote operations."""
        self._topic: Topic = topic_model
        """The topic metadata model"""
        self._fl_ticket: fl.Ticket = ticket
        """The FlightTicket of the remote resource corresponding to this topic"""
        self._data_streamer_instance: Optional[TopicDataStreamer] = None
        """The instance of the spawned data streamer handler"""
        self._timestamp_ns_min: Optional[int] = timestamp_ns_min
        """Lowest timestamp [ns] in the sequence (among all the topics)"""
        self._timestamp_ns_max: Optional[int] = timestamp_ns_max
        """Highest timestamp [ns] in the sequence (among all the topics)"""

    @classmethod
    def _connect(
        cls,
        sequence_name: str,
        topic_name: str,
        client: fl.FlightClient,
    ) -> Optional["TopicHandler"]:
        """
        Internal factory method to initialize a TopicHandler from the server.
        This method fetches flight descriptors and system information (size, creation dates,
        etc.) to fully populate the `Topic` data model.


        Important: **Do not call this directly**
            Users should retrieve instances via
            [`MosaicoClient.topic_handler()`][mosaicolabs.comm.MosaicoClient.topic_handler],
            or by using the [`get_topic_handler()`][mosaicolabs.handlers.SequenceHandler.get_topic_handler] method from the
            [`SequenceHandler`][mosaicolabs.handlers.SequenceHandler] instance of the parent senquence.


        Args:
            sequence_name: Name of the parent sequence.
            topic_name: Name of the topic.
            client: An established PyArrow Flight connection.

        Returns:
            TopicHandler: An initialized handler instance, or `None` if the
                resource cannot be found or initialized.
        """
        # Get FlightInfo (Metadata + Endpoints)
        try:
            flight_info, _stzd_sequence_name, _stzd_topic_name = cls._get_flight_info(
                sequence_name=sequence_name,
                topic_name=topic_name,
                client=client,
            )
        except Exception as e:
            logger.error(
                f"Server error (get_flight_info) while asking for Topic descriptor (in TopicHandler), '{e}'"
            )
            return None

        topic_metadata = TopicMetadata.from_dict(
            _decode_metadata(flight_info.schema.metadata)
        )

        # Extract the Topic resource manifest data and the ticket
        ticket: Optional[fl.Ticket] = None
        topic_resrc_mdata: Optional[TopicResourceManifest] = None
        for ep in flight_info.endpoints:
            try:
                topic_resrc_mdata = TopicResourceManifest.from_flight_endpoint(ep)
            except TopicParsingError as e:
                logger.error(f"Skipping invalid topic endpoint, err: '{e}'")
                continue
            # here the topic name is sanitized
            if topic_resrc_mdata.topic_name == _stzd_topic_name:
                ticket = ep.ticket
                break

        if ticket is None or topic_resrc_mdata is None:
            logger.error(
                f"Unable to init handler for topic '{topic_name}' in sequence '{sequence_name}'"
            )
            return None

        # Get System Info (Size, dates, etc.)
        # TODO: This data can be sent via the manifest also (in the flight endpoint). Backend agrees too
        ACTION = FlightAction.TOPIC_SYSTEM_INFO
        act_resp = _do_action(
            client=client,
            action=ACTION,
            payload={
                "name": pack_topic_resource_name(_stzd_sequence_name, _stzd_topic_name)
            },
            expected_type=_DoActionResponseSysInfo,
        )

        if act_resp is None:
            logger.error(f"Action '{ACTION}' returned no response.")
            return None

        # Build Model
        topic_model = Topic._from_flight_info(
            sequence_name=_stzd_sequence_name,
            name=_stzd_topic_name,
            metadata=topic_metadata,
            sys_info=act_resp,
        )

        # Get the 'min'/'max' timestamps, as we are at a topic-level
        return cls(
            client=client,
            topic_model=topic_model,
            ticket=ticket,
            timestamp_ns_min=topic_resrc_mdata.timestamp_ns_min,
            timestamp_ns_max=topic_resrc_mdata.timestamp_ns_max,
        )

    # -------------------- Public methods --------------------
    @property
    def name(self) -> str:
        """
        The relative name of the topic (e.g., "/front_cam/image_raw").

        Returns:
            The relative name of the topic.
        """
        return self._topic.name

    @property
    def sequence_name(self) -> str:
        """
        The name of the parent sequence containing this topic.

        Returns:
            The name of the parent sequence.
        """
        return self._topic.sequence_name

    @property
    def user_metadata(self) -> Dict[str, Any]:
        """
        The user-defined metadata dictionary associated with this topic.

        Returns:
            The user-defined metadata dictionary.
        """
        return self._topic.user_metadata

    @property
    def created_datetime(self) -> datetime.datetime:
        """
        The UTC timestamp indicating when the entity was created on the server.

        Returns:
            The UTC timestamp indicating when the entity was created on the server.
        """
        return self._topic._created_datetime

    @property
    def is_locked(self) -> bool:
        """
        Indicates if the resource is currently locked.

        A locked state typically occurs during active writing or maintenance operations,
        preventing deletion or structural modifications.

        Returns:
            True if the resource is currently locked, False otherwise.
        """
        return self._topic._is_locked

    @property
    def chunks_number(self) -> Optional[int]:
        """
        The number of physical data chunks stored for this topic.

        Returns:
            The number of physical data chunks stored for this topic, or `None` if the server did not provide detailed storage statistics.
        """
        return self._topic._chunks_number

    @property
    def ontology_tag(self) -> str:
        """
        The ontology type identifier (e.g., 'imu', 'gnss').

        This corresponds to the `__ontology_tag__` defined in the
        [`Serializable`][mosaicolabs.models.Serializable] class registry.

        Returns:
            The ontology type identifier.
        """
        return self._topic._ontology_tag

    @property
    def serialization_format(self) -> str:
        """
        The format used to serialize the topic data (e.g., 'arrow', 'image').

        This corresponds to the [`SerializationFormat`][mosaicolabs.enum.SerializationFormat] enum.

        Returns:
            The serialization format.
        """
        return self._topic._serialization_format

    @property
    def total_size_bytes(self) -> int:
        """
        The total physical storage footprint of the entity on the server in bytes.

        Returns:
            The total physical storage footprint of the entity on the server in bytes.
        """
        return self._topic._total_size_bytes

    @property
    def timestamp_ns_min(self) -> Optional[int]:
        """
        The lowest timestamp (nanoseconds) recorded in this topic.

        Returns:
            The lowest timestamp (nanoseconds) recorded in this topic, or `None` if the topic is empty or timestamps are unavailable.
        """
        return self._timestamp_ns_min

    @property
    def timestamp_ns_max(self) -> Optional[int]:
        """
        The highest timestamp (nanoseconds) recorded in this topic.

        Returns:
            The highest timestamp (nanoseconds) recorded in this topic, or `None` if the topic is empty or timestamps are unavailable.
        """
        return self._timestamp_ns_max

    def get_data_streamer(
        self,
        start_timestamp_ns: Optional[int] = None,
        end_timestamp_ns: Optional[int] = None,
    ) -> TopicDataStreamer:
        """
        Opens a high-performance reading channel for iterating over this topic's data.

        ### Stream Lifecycle Policy: Single-Active-Streamer
        To optimize resource utilization and prevent backend socket exhaustion, this handler
        maintains at most **one active stream** at a time.

        Args:
            start_timestamp_ns: The **inclusive** lower bound (t >= start) in nanoseconds.
                The stream begins at the first message with a timestamp >= this value.
            end_timestamp_ns: The **exclusive** upper bound (t < end) in nanoseconds.
                The stream terminates before reaching any message with a timestamp >= this value.

        Returns:
            TopicDataStreamer: A chronological iterator for the requested data window.

        Raises:
            ValueError: If the topic contains no data or the handler is in an invalid state.

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
                        process_sample(imu_msg.get_data(IMU)) # Some custom process function

                    # Once done, close the reading channel (recommended)
                    top_handler.close()

            ```

        Important:
            Every call to `get_data_streamer()` will automatically invoke
            `close()` on any previously spawned `TopicDataStreamer` instance and its associated
            Apache Arrow Flight channel before initializing the new stream.

            Example:
                ```python
                top_handler = client.topic_handler("mission_alpha", "/front/imu")

                # Opens first stream
                streamer_v1 = top_handler.get_data_streamer(start_timestamp_ns=T1)

                # Calling this again automatically CLOSES streamer_v1 and opens a new channel
                streamer_v2 = top_handler.get_data_streamer(start_timestamp_ns=T2)

                # Using `streamer_v1` will raise a ValueError
                for msg in streamer_v1 # raises here!
                    pass
                ```

        """
        if self._fl_ticket is None:
            raise ValueError(
                f"Unable to get a TopicDataStreamer for topic '{self._topic.name}': invalid TopicHandler!"
            )

        self._validate_timestamps_info()

        if self._data_streamer_instance is not None:
            self._data_streamer_instance.close()
            self._data_streamer_instance = None

        if start_timestamp_ns is not None or end_timestamp_ns is not None:
            # Spawn via connection (calls get_flight_info)
            self._data_streamer_instance = TopicDataStreamer._connect(
                client=self._fl_client,
                topic_name=self.name,
                sequence_name=self._topic.sequence_name,
                start_timestamp_ns=start_timestamp_ns,
                end_timestamp_ns=end_timestamp_ns,
            )
        else:
            # Spawn via ticket (calls do_get straight)
            self._data_streamer_instance = TopicDataStreamer._connect_from_ticket(
                client=self._fl_client,
                topic_name=self.name,
                ticket=self._fl_ticket,
            )

        return self._data_streamer_instance

    def close(self):
        """
        Terminates the active data streamer associated with this topic and releases
        allocated system resources.

        In the Mosaico architecture, a `TopicHandler` acts as a factory for
        `TopicDataStreamers`. Calling `close()` ensures that any background data
        fetching, buffering, or network sockets held by an active streamer are
        immediately shut down.

        Note:
            - If no streamer has been spawned (via `get_data_streamer`), this
              method performs no operation and returns safely.
            - Explicitly closing handlers is a best practice when iterating through
              large datasets to prevent resource accumulation.

        Example:
            ```python
            from mosaicolabs import MosaicoClient

            with MosaicoClient.connect("localhost", 6726) as client:
                # Access a specific sensor topic (e.g., IMU)
                top_handler = client.topic_handler("mission_alpha", "/front/imu")

                if top_handler:
                    # Initialize a high-performance data stream
                    imu_stream = top_handler.get_data_streamer(
                        start_timestamp_ns=1738508778000000000,
                        end_timestamp_ns=1738509618000000000
                    )

                    # Consume data for ML training or analysis
                    # for msg in imu_stream: ...

                    # Release the streaming channel and backend resources
                    top_handler.close()
            ```
        """
        if self._data_streamer_instance is not None:
            self._data_streamer_instance.close()
        self._data_streamer_instance = None

    @staticmethod
    def _get_flight_info(
        sequence_name: str,
        topic_name: str,
        client: fl.FlightClient,
    ) -> Tuple[fl.FlightInfo, str, str]:
        """Performs the get_flight_info call. Raises if flight function does"""
        _stzd_sequence_name = sanitize_sequence_name(sequence_name)
        _stzd_topic_name = sanitize_topic_name(topic_name)

        topic_resrc_name = pack_topic_resource_name(
            _stzd_sequence_name, _stzd_topic_name
        )
        descriptor = fl.FlightDescriptor.for_command(
            json.dumps(
                {
                    "resource_locator": topic_resrc_name,
                }
            )
        )

        # Get FlightInfo (Metadata + Endpoints)
        return client.get_flight_info(descriptor), _stzd_sequence_name, _stzd_topic_name

    def _validate_timestamps_info(self):
        if self._timestamp_ns_min is None or self._timestamp_ns_max is None:
            raise ValueError(
                f"Unable to get the data-stream for topic {self.name}. "
                "The topic might contain no data or could not derive 'min' and 'max' timestamps."
            )
