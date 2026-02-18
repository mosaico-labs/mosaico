"""
Sequence Handling Module.

This module provides the `SequenceHandler`, which serves as a client-side handle
for an *existing* sequence. It allows users to inspect metadata, list topics,
and access reading interfaces (`SequenceDataStreamer`).
"""

import datetime
import json
import pyarrow.flight as fl
from typing import Dict, Any, List, Optional, Tuple

from .endpoints import TopicParsingError, TopicResourceManifest
from .sequence_reader import SequenceDataStreamer
from .topic_handler import TopicHandler
from ..comm.metadata import SequenceMetadata, _decode_metadata
from ..comm.do_action import _do_action, _DoActionResponseSysInfo
from ..enum import FlightAction
from ..models.platform import Sequence
from ..helpers import sanitize_sequence_name
from ..logging_config import get_logger

# Set the hierarchical logger
logger = get_logger(__name__)


class SequenceHandler:
    """
    Represents a client-side handle for an existing Sequence on the Mosaico platform.

    The `SequenceHandler` acts as a primary container for inspecting sequence-level metadata,
    listing available topics, and accessing data reading interfaces like the
    `SequenceDataStreamer`.

    Important: Obtaining a Handler
        Users should not instantiate this class directly. The recommended way to
        obtain a handler is via the [`MosaicoClient.sequence_handler()`][mosaicolabs.comm.MosaicoClient.sequence_handler]
        factory method.
    """

    # -------------------- Constructor --------------------
    def __init__(
        self,
        *,
        sequence_model: Sequence,
        client: fl.FlightClient,
        timestamp_ns_min: Optional[int],
        timestamp_ns_max: Optional[int],
    ):
        """
        Internal constructor for SequenceHandler.

        **Do not call this directly.** Users should retrieve instances via
        [`MosaicoClient.sequence_handler()`][mosaicolabs.comm.MosaicoClient.sequence_handler],
        while internal modules should use the `SequenceHandler._connect()` factory.

        Args:
            sequence_model: The underlying metadata and system info model for the sequence.
            client: The active FlightClient for remote operations.
            timestamp_ns_min: The lowest timestamp (in ns) available in this sequence.
            timestamp_ns_max: The highest timestamp (in ns) available in this sequence.
        """
        self._fl_client: fl.FlightClient = client
        """The FlightClient used for remote operations."""
        self._topic_handler_instances: Dict[str, TopicHandler] = {}
        """The cache of the spawned topic handlers instances"""
        self._data_streamer_instance: Optional[SequenceDataStreamer] = None
        """The spawned sequence data streamer instance"""
        self._sequence: Sequence = sequence_model
        """The sequence metadata model"""
        self._timestamp_ns_min: Optional[int] = timestamp_ns_min
        """Lowest timestamp [ns] in the sequence (among all the topics)"""
        self._timestamp_ns_max: Optional[int] = timestamp_ns_max
        """Highest timestamp [ns] in the sequence (among all the topics)"""

    @classmethod
    def _connect(
        cls, sequence_name: str, client: fl.FlightClient
    ) -> Optional["SequenceHandler"]:
        """
        Internal factory method to create a handler.
        Queries the server to build the `Sequence` model and discover all
        contained topics.

        Important: **Do not call this directly**
            Users can retrieve an instance by using [`MosaicoClient.sequence_handler()`][mosaicolabs.comm.MosaicoClient.sequence_handler] instead.

        Args:
            sequence_name (str): Name of the sequence.
            client (fl.FlightClient): Connected client.

        Returns:
            SequenceHandler: Initialized handler or None if error occurs
        """

        # Get FlightInfo
        try:
            flight_info, _stzd_sequence_name = cls._get_flight_info(
                client=client, sequence_name=sequence_name
            )
        except Exception as e:
            logger.error(
                f"Server error (get_flight_info) while asking for Sequence descriptor, '{e}'"
            )
            return None

        seq_metadata = SequenceMetadata.from_dict(
            _decode_metadata(flight_info.schema.metadata)
        )

        # Extract the Topics resource manifests data
        stopics = []
        tstamps_ns_min = []
        tstamps_ns_max = []
        for ep in flight_info.endpoints:
            try:
                topic_resrc_mdata = TopicResourceManifest.from_flight_endpoint(ep)
            except TopicParsingError as e:
                logger.error(f"Skipping invalid topic endpoint, err: '{e}'")
                continue
            stopics.append(topic_resrc_mdata.topic_name)
            # Collect the 'min'/'max' timestamps, as we are at a sequence-level
            if (
                topic_resrc_mdata.timestamp_ns_min is not None
                and topic_resrc_mdata.timestamp_ns_max is not None
            ):
                tstamps_ns_min.append(topic_resrc_mdata.timestamp_ns_min)
                tstamps_ns_max.append(topic_resrc_mdata.timestamp_ns_max)

        # Get System Info
        ACTION = FlightAction.SEQUENCE_SYSTEM_INFO
        act_resp = _do_action(
            client=client,
            action=ACTION,
            payload={"name": _stzd_sequence_name},
            expected_type=_DoActionResponseSysInfo,
        )

        if act_resp is None:
            logger.error(f"Action '{ACTION}' returned no response.")
            return None

        sequence_model = Sequence._from_flight_info(
            name=_stzd_sequence_name,
            metadata=seq_metadata,
            sys_info=act_resp,
            topics=stopics,
        )

        return cls(
            sequence_model=sequence_model,
            client=client,
            timestamp_ns_min=min(tstamps_ns_min) if tstamps_ns_min else None,
            timestamp_ns_max=max(tstamps_ns_max) if tstamps_ns_max else None,
        )

    # -------------------- Public methods --------------------
    @property
    def name(self) -> str:
        """
        The unique name of the sequence.

        Returns:
            The unique name of the sequence.
        """
        return self._sequence._name

    @property
    def topics(self) -> List[str]:
        """
        The list of topic names (data channels) available within this sequence.

        Returns:
            The list of topic names (data channels) available within this sequence.
        """
        return self._sequence._topics

    @property
    def user_metadata(self) -> Dict[str, Any]:
        """
        The user-defined metadata dictionary associated with this sequence.

        Returns:
            The user-defined metadata dictionary associated with this sequence.
        """
        return self._sequence.user_metadata

    @property
    def created_datetime(self) -> datetime.datetime:
        """
        The UTC timestamp indicating when the entity was created on the server.

        Returns:
            The UTC timestamp indicating when the entity was created on the server.
        """
        return self._sequence._created_datetime

    @property
    def is_locked(self) -> bool:
        """
        Indicates if the resource is currently locked.

        A locked state typically occurs during active writing or maintenance operations,
        preventing deletion or structural modifications.

        Returns:
            The lock status of the sequence.
        """
        return self._sequence._is_locked

    @property
    def total_size_bytes(self) -> int:
        """
        The total physical storage footprint of the entity on the server in bytes.

        Returns:
            The total physical storage footprint of the entity on the server in bytes.
        """
        return self._sequence._total_size_bytes

    @property
    def timestamp_ns_min(self) -> Optional[int]:
        """
        The lowest timestamp (nanoseconds) recorded in the sequence across all topics.

        Returns:
            The lowest timestamp (nanoseconds) recorded in the sequence across all topics, or `None` if the sequence contains no data or the timestamps could not be derived.
        """
        return self._timestamp_ns_min

    @property
    def timestamp_ns_max(self) -> Optional[int]:
        """
        The highest timestamp (nanoseconds) recorded in the sequence across all topics.

        Returns:
            The highest timestamp (nanoseconds) recorded in the sequence across all topics, or `None` if the sequence contains no data or the timestamps could not be derived.
        """
        return self._timestamp_ns_max

    def get_data_streamer(
        self,
        topics: List[str] = [],
        start_timestamp_ns: Optional[int] = None,
        end_timestamp_ns: Optional[int] = None,
    ) -> SequenceDataStreamer:
        """
        Opens a reading channel for iterating over the sequence data.

        The returned [`SequenceDataStreamer`][mosaicolabs.handlers.SequenceDataStreamer] performs a K-way merge sort to provide
        a single, time-synchronized chronological stream of messages from
        multiple topics.


        Args:
            topics: A subset of topic names to stream. If empty, all topics
                in the sequence are streamed.
            start_timestamp_ns: The **inclusive** lower bound (t >= start) for the time window in nanoseconds.
                The stream starts at the first message with a timestamp greater than or equal to this value.
            end_timestamp_ns: The **exclusive** upper bound (t < end) for the time window in nanoseconds.
                The stream stops at the first message with a timestamp strictly less than this value.

        Returns:
            A `SequenceDataStreamer` iterator yielding `(topic_name, message)` tuples.

        Raises:
            ValueError: If the provided topic names do not exist or if the
                sequence contains no data.

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

                    # Start timed data-stream
                    for topic, msg in streamer:
                        print(f"[{topic}] at {msg.timestamp_ns}: {type(msg.data).__name__}")

                    # Once done, close the resources, topic handler and related reading channels (recommended).
                    seq_handler.close()
            ```

        Important:
            Every call to `get_data_streamer()` will automatically invoke
            `close()` on any previously spawned `SequenceDataStreamer` instance and its associated
            Apache Arrow Flight channels before initializing the new stream.

            Example:
                ```python
                seq_handler = client.sequence_handler("mission_alpha")

                # Opens first stream
                streamer_v1 = seq_handler.get_data_streamer(start_timestamp_ns=T1)

                # Calling this again automatically CLOSES streamer_v1 and opens a new channel
                streamer_v2 = seq_handler.get_data_streamer(start_timestamp_ns=T2)

                # Using `streamer_v1` will raise a ValueError
                for topic, msg in streamer_v1 # raises here!
                    pass
                ```
        """
        if topics and any([t not in self.topics for t in topics]):
            raise ValueError(
                f"Invalid input topic names {topics}. Available topics in sequence '{self.name}':\n{self.topics}"
            )

        self._validate_timestamps_info()

        if self._data_streamer_instance is not None:
            self._data_streamer_instance.close()
            self._data_streamer_instance = None

        self._data_streamer_instance = SequenceDataStreamer._connect(
            self._sequence.name,
            topics,
            start_timestamp_ns,
            end_timestamp_ns,
            self._fl_client,
        )
        return self._data_streamer_instance

    def get_topic_handler(
        self, topic_name: str, force_new_instance: bool = False
    ) -> TopicHandler:
        """
        Get a specific [`TopicHandler`][mosaicolabs.handlers.TopicHandler] for a child topic.

        Args:
            topic_name: The relative name of the topic (e.g., "/camera/front").
            force_new_instance: If `True`, bypasses the internal cache and
                recreates the handler.

        Returns:
            A `TopicHandler` dedicated to the specified topic.

        Raises:
            ValueError: If the topic is not available in this sequence or
                an internal connection error occurs.

        Example:
            ```python
            import sys
            from mosaicolabs import MosaicoClient

            with MosaicoClient.connect("localhost", 6726) as client:
                seq_handler = client.sequence_handler("mission_alpha")
                if seq_handler:
                    # Use a Handler to inspect the catalog
                    top_handler = seq_handler.get_topic_handler("/front/imu")
                    if top_handler:
                        print(f"Sequence: {top_handler.sequence_name}")
                        print(f"\t|Topic: {top_handler.sequence_name}:{top_handler.name}")
                        print(f"\t|User metadata: {top_handler.user_metadata}")
                        print(f"\t|Timestamp span: {top_handler.timestamp_ns_min} - {top_handler.timestamp_ns_max}")
                        print(f"\t|Created {top_handler.created_datetime}")
                        print(f"\t|Size (MB) {top_handler.total_size_bytes/(1024*1024)}")

                    # Once done, close the resources, topic handler and related reading channels (recommended).
                    seq_handler.close()
            ```
        """
        if topic_name not in self._sequence.topics:
            raise ValueError(
                f"Topic '{topic_name}' not available in sequence '{self._sequence.name}'"
            )

        th = self._topic_handler_instances.get(topic_name)

        if force_new_instance and th is not None:
            th.close()
            th = None

        if th is None:
            th = TopicHandler._connect(
                sequence_name=self._sequence.name,
                topic_name=topic_name,
                client=self._fl_client,
            )
            if not th:
                raise ValueError(
                    f"Internal Error: unable to connect a TopicHandler for topic '{topic_name}' in sequence '{self.name}'"
                )
            self._topic_handler_instances[topic_name] = th

        return th

    def close(self):
        """
        Gracefully closes all cached topic handlers and active data streamers.

        This method should be called to release network and memory resources
        when the handler is no longer needed.

        Example:
            ```python
            from mosaicolabs import MosaicoClient

            with MosaicoClient.connect("localhost", 6726) as client:
                # Use a Handler to inspect the catalog
                seq_handler = client.sequence_handler("mission_alpha")
                if seq_handler:
                    # Perform operations
                    # ...

                    # Once done, close the resources, topic handler and related reading channels (recommended).
                    seq_handler.close()
            ```
        """
        for _, th in self._topic_handler_instances.items():
            th.close()
        self._topic_handler_instances.clear()

        if self._data_streamer_instance is not None:
            self._data_streamer_instance.close()
            self._data_streamer_instance = None

    @staticmethod
    def _get_flight_info(
        client: fl.FlightClient, sequence_name: str
    ) -> Tuple[fl.FlightInfo, str]:
        """Performs the get_flight_info call. Raises if flight function does"""
        _stzd_sequence_name = sanitize_sequence_name(sequence_name)

        descriptor = fl.FlightDescriptor.for_command(
            json.dumps(
                {
                    "resource_locator": _stzd_sequence_name,
                }
            )
        )
        # Get FlightInfo
        flight_info = client.get_flight_info(descriptor)
        return flight_info, _stzd_sequence_name

    def _validate_timestamps_info(self):
        if self._timestamp_ns_min is None or self._timestamp_ns_max is None:
            raise ValueError(
                f"Unable to get the data-stream for sequence {self.name}. "
                "The sequence might contain no data or could not derive 'min' and 'max' timestamps."
            )
