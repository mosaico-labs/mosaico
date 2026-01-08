"""
Topic Handling Module.

This module provides the `TopicHandler`, which serves as a client-side handle
for an *existing* topic on the server. It allows users to inspect metadata
and create readers (`TopicDataStreamer`).
"""

import json
import pyarrow.flight as fl
from typing import Any, Optional, Type

from .helpers import _parse_ep_ticket
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
from ..logging import get_logger

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

    # -------------------- Class attributes --------------------
    _topic: Topic
    _fl_client: fl.FlightClient
    _fl_ticket: fl.Ticket
    _data_streamer_instance: Optional[TopicDataStreamer]

    def __init__(self, client: fl.FlightClient, topic_model: Topic, ticket: fl.Ticket):
        """
        Internal constructor.
        Users can retrieve an instance by using 'MosaicoClient.topic_handler()` instead.
        Internal library modules will call the 'connect()' function.
        """
        self._fl_client = client
        self._topic = topic_model
        self._fl_ticket = ticket
        self._data_streamer_instance = None

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
        try:
            flight_info = client.get_flight_info(descriptor)
        except Exception as e:
            logger.error(f"Server error while asking for Topic descriptor, '{e}'")
            return None

        topic_metadata = TopicMetadata.from_dict(
            _decode_metadata(flight_info.schema.metadata)
        )

        # Extract the Ticket for this specific topic
        ticket: Optional[fl.Ticket] = None
        ep_ticket_data = None
        for ep in flight_info.endpoints:
            ep_ticket_data = _parse_ep_ticket(ep.ticket)
            # here the topic name is sanitized
            if ep_ticket_data and ep_ticket_data[1] == _stzd_topic_name:
                ticket = ep.ticket
                break

        if ticket is None:
            logger.error(
                f"Unable to init handler for topic '{topic_name}' in sequence '{sequence_name}'"
            )
            return None

        # Get System Info (Size, dates, etc.)
        ACTION = FlightAction.TOPIC_SYSTEM_INFO
        act_resp = _do_action(
            client=client,
            action=ACTION,
            payload={"name": topic_resrc_name},
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

        return cls(client, topic_model, ticket)

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

    def get_data_streamer(self, force_new_instance=False) -> TopicDataStreamer:
        """
        Creates or retrieves a `TopicDataStreamer` to read data.

        Args:
            force_new_instance (bool): If True, creates a fresh reader even if one exists.

        Returns:
            TopicDataStreamer: The reader object.

        Raises:
            ValueError: if TopicHandler internal is not valid
        """
        if self._fl_ticket is None:
            raise ValueError(
                f"Unable to get a TopicDataStreamer for topic '{self._topic.name}': invalid TopicHandler!"
            )

        if force_new_instance and self._data_streamer_instance is not None:
            self._data_streamer_instance.close()
            self._data_streamer_instance = None

        if self._data_streamer_instance is None:
            self._data_streamer_instance = TopicDataStreamer.connect(
                self._fl_client, self._fl_ticket
            )
        return self._data_streamer_instance

    def close(self):
        """Closes the data streamer if active."""
        if self._data_streamer_instance is not None:
            self._data_streamer_instance.close()
        self._data_streamer_instance = None
