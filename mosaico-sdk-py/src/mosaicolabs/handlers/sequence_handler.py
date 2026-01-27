"""
Sequence Handling Module.

This module provides the `SequenceHandler`, which serves as a client-side handle
for an *existing* sequence. It allows users to inspect metadata, list topics,
and access reading interfaces (`SequenceDataStreamer`).
"""

import json
import pyarrow.flight as fl
from typing import Dict, Any, List, Optional, Tuple, Type

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
    Represents an existing Sequence on the Mosaico platform.

    Acts as a container for accessing the sequence's metadata and its
    child topics.

    User intending getting an instance of this class, must use 'MosaicoClient.sequence_handler()' factory.
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
        Internal constructor.
        Users can retrieve an instance by using 'MosaicoClient.sequence_handler()` instead.
        Internal library modules will call the 'connect()' function.
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
    def connect(
        cls, sequence_name: str, client: fl.FlightClient
    ) -> Optional["SequenceHandler"]:
        """
        Factory method to create a handler.

        Queries the server to build the `Sequence` model and discover all
        contained topics.

        Args:
            sequence_name (str): Name of the sequence.
            client (fl.FlightClient): Connected client.

        Returns:
            SequenceHandler: Initialized handler.
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

        sequence_model = Sequence.from_flight_info(
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

    # --- Context Manager ---
    def __enter__(self) -> "SequenceHandler":
        """Returns the SequenceHandler instance for use in a 'with' statement."""
        return self

    def __exit__(
        self,
        exc_type: Optional[Type[BaseException]],
        exc_val: Optional[BaseException],
        exc_tb: Optional[Any],
    ) -> None:
        """Context manager exit for SequenceHandler."""
        try:
            self.close()
        except Exception as e:
            logger.error(
                f"Error releasing resources allocated from SequenceHandler '{self._sequence.name}'.\nInner err: '{e}'"
            )

    # -------------------- Public methods --------------------
    @property
    def topics(self):
        """Returns the list of topic names in the sequence."""
        return self._sequence.topics

    @property
    def user_metadata(self):
        """Returns the user dictionary for the sequence."""
        return self._sequence.user_metadata

    @property
    def name(self):
        """Returns the sequence name."""
        return self._sequence.name

    @property
    def sequence_info(self) -> Sequence:
        """Returns the full Sequence model."""
        return self._sequence

    @property
    def timestamp_ns_min(self) -> Optional[int]:
        """
        Return the lowest timestamp in nanoseconds, among all the topics.
        Returns optional to manage the degenerate case of topics with no data.
        """
        return self._timestamp_ns_min

    @property
    def timestamp_ns_max(self) -> Optional[int]:
        """
        Return the highest timestamp in nanoseconds, among all the topics.
        Returns optional to manage the degenerate case of topics with no data.
        """
        return self._timestamp_ns_max

    def get_data_streamer(
        self,
        topics: List[str] = [],
        start_timestamp_ns: Optional[int] = None,
        end_timestamp_ns: Optional[int] = None,
    ) -> SequenceDataStreamer:
        """
        Opens a reading channel and returns a `SequenceDataStreamer` for iterating over the sequence data.

        The streamer allows for time-synchronized playback of multiple topics and supports temporal slicing
        to retrieve data within a specific time window.

        Args:
            topics (List[str], optional): A list of specific topic names to filter the stream.
                If empty, the behavior depends on the implementation (typically streams all available topics).
            start_timestamp_ns (int, optional): The **inclusive** lower bound for the time window (in nanoseconds).
                The stream will begin from the message with the timestamp **greater than or equal to** this value.
            end_timestamp_ns (int, optional): The **exclusive** upper bound for the time window (in nanoseconds).
                The stream will stop at the message with the timestamp **strictly lower than** this value.

        Returns:
            SequenceDataStreamer: An iterator yielding time-ordered messages from the requested topics.
        """
        if topics and any([t not in self.topics for t in topics]):
            raise ValueError(
                f"Invalid input topic names {topics}. Available topics in sequence '{self.name}':\n{self.topics}"
            )

        self._validate_timestamps_info()

        if self._data_streamer_instance is not None:
            self._data_streamer_instance.close()
            self._data_streamer_instance = None

        self._data_streamer_instance = SequenceDataStreamer.connect(
            self._sequence.name,
            topics,
            start_timestamp_ns,
            end_timestamp_ns,
            self._fl_client,
        )
        return self._data_streamer_instance

    def get_topic_handler(
        self, topic_name: str, force_new_instance=False
    ) -> TopicHandler:
        """
        Get a specific `TopicHandler` for a child topic.

        Args:
            topic_name (str): Name of the child topic (without the parent sequence name).
            force_new_instance (bool): If True, recreates the handler.

        Returns:
            TopicHandler: The handler

        Raises:
            ValueError: If topic doesn't exist.
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
            th = TopicHandler.connect(
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
        """Closes all cached topic handlers and streamers."""
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
