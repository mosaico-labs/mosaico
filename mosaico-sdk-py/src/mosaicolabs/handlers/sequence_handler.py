"""
Sequence Handling Module.

This module provides the `SequenceHandler`, which serves as a client-side handle
for an *existing* sequence. It allows users to inspect metadata, list topics,
and access reading interfaces (`SequenceDataStreamer`).
"""

import json
from typing import Any, Callable, Dict, List, Optional, Tuple, Union

import pyarrow.flight as fl

from mosaicolabs.platform.resource_info import TopicResourceInfo

from ..comm.connection import (
    DEFAULT_MAX_BATCH_BYTES,
    DEFAULT_MAX_BATCH_SIZE_RECORDS,
    _ConnectionPool,
)
from ..comm.executor_pool import _ExecutorPool
from ..enum import OnErrorPolicy, SessionLevelErrorPolicy
from ..helpers import sanitize_sequence_name
from ..logging_config import get_logger
from ..models.platform import Sequence, Session
from ..platform.metadata import SequenceMetadata, _decode_schema_metadata
from ..platform.resource_manifests import (
    SequenceResourceManifest,
    TopicManifestError,
)
from .config import SessionWriterConfig
from .sequence_reader import SequenceDataStreamer
from .sequence_updater import SequenceUpdater
from .topic_handler import TopicHandler

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
        connection_pool_allocator: Callable[[], _ConnectionPool],
        executor_pool_allocator: Callable[[], _ExecutorPool],
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
        self._connection_pool_allocator: Callable[[], _ConnectionPool] = (
            connection_pool_allocator
        )
        """Allocator for connection pools"""
        self._executor_pool_allocator: Callable[[], _ExecutorPool] = (
            executor_pool_allocator
        )
        """Allocator for executor pools"""

    @classmethod
    def _connect(
        cls,
        sequence_name: str,
        client: fl.FlightClient,
        connection_pool_allocator: Callable[[], _ConnectionPool],
        executor_pool_allocator: Callable[[], _ExecutorPool],
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
            connection_pool_allocator (Callable): the callback for allocationg a connection pool.
            executor_pool_allocator (Callable): the callback for allocationg an executor pool.

        Returns:
            SequenceHandler: Initialized handler or None if error occurs
        """

        model_tuple = SequenceHandler._get_platform_resource_data(
            sequence_name=sequence_name, client=client
        )
        if model_tuple is None:
            return None

        sequence_model, tstamp_ns_min, tstamp_ns_max = model_tuple

        return cls(
            sequence_model=sequence_model,
            client=client,
            connection_pool_allocator=connection_pool_allocator,
            executor_pool_allocator=executor_pool_allocator,
            timestamp_ns_min=tstamp_ns_min,
            timestamp_ns_max=tstamp_ns_max,
        )

    @staticmethod
    def _get_platform_resource_data(
        sequence_name: str,
        client: fl.FlightClient,
    ) -> Optional[Tuple]:
        """
        Internal static method to retrieve sequence-related remote info.
        Queries the server to build the `Sequence` model and discover all
        contained topics.

        Args:
            sequence_name (str): Name of the sequence.
            client (fl.FlightClient): Connected client.

        Returns:
            Optional tuple containing:
                - the sequence model (`Sequence`),
                - the min sequence timestamp
                - the max sequence timestamp
        """

        # Get FlightInfo
        try:
            flight_info, _stzd_sequence_name = SequenceHandler._get_flight_info(
                client=client, sequence_name=sequence_name
            )
        except Exception as e:
            logger.error(
                f"Server error (get_flight_info) while asking for Sequence descriptor, '{e}'"
            )
            return None

        # Retrieve the Sequence metadata
        seq_metadata = SequenceMetadata._from_decoded_schema_metadata(
            _decode_schema_metadata(flight_info.schema.metadata)
        )

        seq_manifest = SequenceResourceManifest._from_app_metadata(
            flight_info.app_metadata
        )

        # Extract the Topics resource manifests data
        tstamps_ns_min = []
        tstamps_ns_max = []
        total_size_bytes = 0
        for ep in flight_info.endpoints:
            try:
                topic_resrc_info = TopicResourceInfo._from_flight_endpoint(ep)
            except TopicManifestError as e:
                logger.error(f"Skipping invalid topic endpoint, err: '{e}'")
                continue
            # Collect the 'min'/'max' timestamps, as we are at a sequence-level
            if (
                topic_resrc_info.timestamp_ns_min is not None
                and topic_resrc_info.timestamp_ns_max is not None
            ):
                tstamps_ns_min.append(topic_resrc_info.timestamp_ns_min)
                tstamps_ns_max.append(topic_resrc_info.timestamp_ns_max)

            total_size_bytes += topic_resrc_info.total_size_bytes

        sequence_model = Sequence._from_resource_info(
            name=_stzd_sequence_name,
            platform_metadata=seq_metadata,
            resrc_manifest=seq_manifest,
            total_size_bytes=total_size_bytes,
        )

        return (
            sequence_model,
            min(tstamps_ns_min) if tstamps_ns_min else None,
            max(tstamps_ns_max) if tstamps_ns_max else None,
        )

    def _reload(self) -> bool:
        """
        Reloads the sequence handler with the latest data from the server.

        Returns:
            bool: True if the reload was successful, False otherwise.
        """
        model_tuple = SequenceHandler._get_platform_resource_data(
            sequence_name=self.name, client=self._fl_client
        )
        if model_tuple is None:
            return False

        sequence_model, tstamp_ns_min, tstamp_ns_max = model_tuple

        self._sequence = sequence_model
        self._timestamp_ns_min = tstamp_ns_min
        self._timestamp_ns_max = tstamp_ns_max

        return True

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
        return self._sequence.topics

    @property
    def sessions(self) -> List[Session]:
        """
        The list of all the writing sessions that produced this sequence (upon creation or updates).

        Returns:
            A list of [`Session`][mosaicolabs.models.platform.Session] instances
        """
        return self._sequence.sessions

    @property
    def user_metadata(self) -> Dict[str, Any]:
        """
        The user-defined metadata dictionary associated with this sequence.

        Returns:
            The ucreated_timestampdata dictionary associated with this sequence.
        """
        return self._sequence.user_metadata

    @property
    def created_timestamp(self) -> int:
        """
        The UTC timestamp indicating when the entity was created on the server.

        Returns:
            The UTC creation timestamp.
        """
        return self._sequence.created_timestamp

    @property
    def updated_timestamps(self) -> List[int]:
        """
        The list of UTC timestamps indicating when the entity was updated on the server.

        Returns:
            The list of UTC update timestamps.
        """
        return self._sequence.updated_timestamps

    @property
    def total_size_bytes(self) -> int:
        """
        The total physical storage footprint of the entity on the server in bytes.

        Returns:
            The total physical storage in bytes.
        """
        return self._sequence.total_size_bytes

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

    def update(
        self,
        on_error: Union[
            SessionLevelErrorPolicy, OnErrorPolicy
        ] = SessionLevelErrorPolicy.Report,
        max_batch_size_bytes: Optional[int] = None,
        max_batch_size_records: Optional[int] = None,
    ) -> SequenceUpdater:
        """
        Update the sequence on the platform and returns a [`SequenceUpdater`][mosaicolabs.handlers.SequenceUpdater] for ingestion.

        Important:
            The function **must** be called inside a with context, otherwise a
            RuntimeError is raised.

        Args:
            on_error (SessionLevelErrorPolicy | OnErrorPolicy): Behavior on write failure. Defaults to
                [`SessionLevelErrorPolicy.Report`][mosaicolabs.enum.SessionLevelErrorPolicy.Report].

                Deprecated:
                    [`OnErrorPolicy`][mosaicolabs.enum.OnErrorPolicy] is deprecated since v0.3.0; use
                    [`SessionLevelErrorPolicy`][mosaicolabs.enum.SessionLevelErrorPolicy] instead.
                    It will be removed in v0.4.0.

            max_batch_size_bytes (Optional[int]): Max bytes per Arrow batch.
            max_batch_size_records (Optional[int]): Max records per Arrow batch.

        Returns:
            SequenceUpdater: An initialized updater instance.

        Raises:
            RuntimeError: If the method is called outside a `with` context.
            Exception: If any error occurs during sequence injection.

        Example:
            ```python
            from mosaicolabs import MosaicoClient, SessionLevelErrorPolicy

            # Open the connection with the Mosaico Client
            with MosaicoClient.connect("localhost", 6726) as client:
                # Get the handler for the sequence
                seq_handler = client.sequence_handler("mission_log_042")
                # Update the sequence
                with seq_handler.update(
                    on_error = SessionLevelErrorPolicy.Delete
                    ) as seq_updater:
                        # Start creating topics and pushing data
                        # (1)!

                # Exiting the block automatically flushes all topic buffers, finalizes the sequence on the server
                # and closes all connections and pools
            ```

            1. See also:
                * [`SequenceUpdater.topic_create()`][mosaicolabs.handlers.SequenceUpdater.topic_create]
                * [`TopicWriter.push()`][mosaicolabs.handlers.TopicWriter.push]
        """

        # Use defaults if specific batch sizes aren't provided
        max_batch_size_bytes = (
            max_batch_size_bytes
            if max_batch_size_bytes is not None
            else DEFAULT_MAX_BATCH_BYTES
        )
        max_batch_size_records = (
            max_batch_size_records
            if max_batch_size_records is not None
            else DEFAULT_MAX_BATCH_SIZE_RECORDS
        )

        if isinstance(on_error, OnErrorPolicy):
            on_error = SessionLevelErrorPolicy(on_error.value)

        return SequenceUpdater(
            sequence_name=self._sequence.name,
            client=self._fl_client,
            connection_pool=self._connection_pool_allocator(),
            executor_pool=self._executor_pool_allocator(),
            config=SessionWriterConfig(
                on_error=on_error,
                max_batch_size_bytes=max_batch_size_bytes,
                max_batch_size_records=max_batch_size_records,
            ),
        )

    def reload(self) -> bool:
        """
        Reloads the handler's data from the server.
        Use this method when you need to retrieve the latest sequence information,
        e.g. after a [sequence update][mosaicolabs.handlers.SequenceHandler.update].

        Note:
            This method does not close any active topic handlers or data streamers.
            The function does not affect actual sequence data-streams. Therefore,
            it is safe to call this method multiple times without closing any active resources.

        Returns:
            bool: True if the reload was successful, False otherwise.

        Example:
            ```python
            from mosaicolabs import MosaicoClient

            with MosaicoClient.connect("localhost", 6726) as client:
                # Use a Handler to inspect the catalog
                seq_handler = client.sequence_handler("mission_alpha")
                if seq_handler:
                    # Perform operations, typically updating the sequence on the server
                    # ...
                    # (1)!

                    # Refresh the handler's data from the server
                    if not seq_handler.reload():
                        print("Failed to reload sequence handler")
            ```

            1. See also: [`SequenceUpdater`][mosaicolabs.handlers.SequenceUpdater]
        """
        return self._reload()

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
