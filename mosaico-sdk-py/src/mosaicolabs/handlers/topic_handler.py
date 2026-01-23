"""
Topic Handling Module.

This module provides the `TopicHandler`, which serves as a client-side handle
for an *existing* topic on the server. It allows users to inspect metadata
and create readers (`TopicDataStreamer`).
"""

import json
import pyarrow.flight as fl
from typing import Any, Optional, Tuple, Type

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

    Provides access to:
    1. Static metadata (from `Topic` model).
    2. Streaming data reading (via `get_data_streamer`).

    User intending getting an instance of this class, must use 'MosaicoClient.topic_handler()' factory.
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
        Internal constructor.
        Users can retrieve an instance by using 'MosaicoClient.topic_handler()` instead.
        Internal library modules will call the 'connect()' function.
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
    def connect(
        cls,
        sequence_name: str,
        topic_name: str,
        client: fl.FlightClient,
    ) -> Optional["TopicHandler"]:
        """
        Factory method to create a handler.

        Fetches flight info and system info from the server to populate the Topic model.

        Args:
            sequence_name (str): Parent sequence.
            topic_name (str): Topic name.
            client (fl.FlightClient): Connected client.

        Returns:
            TopicHandler: Initialized handler.
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
        topic_model = Topic.from_flight_info(
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

    # --- Context Manager ---
    def __enter__(self) -> "TopicHandler":
        """Returns the TopicHandler instance for use in a 'with' statement."""
        return self

    def __exit__(
        self,
        exc_type: Optional[Type[BaseException]],
        exc_val: Optional[BaseException],
        exc_tb: Optional[Any],
    ) -> None:
        """Context manager exit for TopicHandler."""
        try:
            self.close()
        except Exception as e:
            logger.exception(
                f"Error releasing resources allocated from TopicHandler '{self._topic.name}'.\nInner err: '{e}'"
            )

    @property
    def user_metadata(self):
        """Returns the user dictionary associated with the topic."""
        return self._topic.user_metadata

    @property
    def topic_info(self) -> Topic:
        """Returns the Topic data model."""
        return self._topic

    @property
    def name(self):
        """Returns the topic name."""
        return self._topic.name

    @property
    def timestamp_ns_min(self):
        """Return the lowest timestamp in nanoseconds, for this topic"""
        return self._timestamp_ns_min

    @property
    def timestamp_ns_max(self):
        """Return the highest timestamp in nanoseconds, for this topic"""
        return self._timestamp_ns_max

    def get_data_streamer(
        self,
        start_timestamp_ns: Optional[int] = None,
        end_timestamp_ns: Optional[int] = None,
    ) -> TopicDataStreamer:
        """
        Opens a reading channel and returns a `TopicDataStreamer` for iterating over this topic's data.

        The streamer supports temporal slicing to retrieve data within a specific time window.

        Args:
            start_timestamp_ns (int, optional): The **inclusive** lower bound for the time window (in nanoseconds).
                The stream will begin from the message with the timestamp **greater than or equal to** this value.
            end_timestamp_ns (int, optional): The **exclusive** upper bound for the time window (in nanoseconds).
                The stream will stop at the message with the timestamp **strictly lower than** this value.

        Returns:
            TopicDataStreamer: An iterator yielding time-ordered messages from this topic.

        Raises:
            ValueError: If the TopicHandler internal state is invalid or the topic has no data to stream.
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
            self._data_streamer_instance = TopicDataStreamer.connect(
                client=self._fl_client,
                topic_name=self.name,
                sequence_name=self._topic.sequence_name,
                start_timestamp_ns=start_timestamp_ns,
                end_timestamp_ns=end_timestamp_ns,
            )
        else:
            # Spawn via ticket (calls do_get straight)
            self._data_streamer_instance = TopicDataStreamer.connect_from_ticket(
                client=self._fl_client,
                topic_name=self.name,
                ticket=self._fl_ticket,
            )

        return self._data_streamer_instance

    def close(self):
        """Closes the data streamer if active."""
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
