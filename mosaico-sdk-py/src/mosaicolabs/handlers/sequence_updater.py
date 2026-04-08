"""
Sequence Updating Module.

This module acts as the central controller for updating a sequence of data.
It manages the lifecycle of the sequence on the server (Create -> Write -> Finalize)
and distributes client resources to individual Topics.
"""

from typing import Any, Optional, Type

import pyarrow.flight as fl

from ..enum import TopicLevelErrorPolicy
from ..logging_config import get_logger
from ..models import Serializable
from .base_session_writer import _BaseSessionWriter
from .config import SessionWriterConfig
from .topic_writer import TopicWriter

# Set the hierarchical logger
logger = get_logger(__name__)


class SequenceUpdater(_BaseSessionWriter):
    """
    Orchestrates the sequence update and related data ingestion lifecycle of a Mosaico Sequence.

    The `SequenceUpdater` is the central controller for high-performance data writing.

    Important: Usage Pattern
        This class **must** be used within a `with` statement (Context Manager).
        The context entry triggers sequence registration on the server, while the exit handles
        automatic finalization or error cleanup based on the configured `SessionLevelErrorPolicy`.

    Important: Obtaining a Writer
        Do not instantiate this class directly. Use the
        [`SequenceHandler.update()`][mosaicolabs.handlers.SequenceHandler.update]
        factory method.
    """

    # -------------------- Constructor --------------------
    def __init__(
        self,
        *,
        sequence_name: str,
        client: fl.FlightClient,
        config: SessionWriterConfig,
    ):
        """
        Internal constructor for SequenceUpdater.

        **Do not call this directly.** Users must call
        [`SequenceHandler.update()`][mosaicolabs.handlers.SequenceHandler.update]
        to obtain an initialized writer.

        Example:
            ```python
            from mosaicolabs import MosaicoClient, SessionLevelErrorPolicy

            # Open the connection with the Mosaico Client
            with MosaicoClient.connect("localhost", 6726) as client:
                # Get the handler for the sequence
                seq_handler = client.sequence_handler("mission_log_042")
                # Update the sequence
                with seq_handler.update( # (1)!
                    on_error = SessionLevelErrorPolicy.Delete
                    ) as seq_updater:
                        # Start creating topics and pushing data
                        # (2)!

                # Exiting the block automatically flushes all topic buffers and finalizes the sequence on the server
            ```

            1. See also: [`SequenceHandler.update()`][mosaicolabs.handlers.SequenceHandler.update]
            2. See also:
                * [`SequenceUpdater.topic_create()`][mosaicolabs.handlers.SequenceUpdater.topic_create]
                * [`TopicWriter.push()`][mosaicolabs.handlers.TopicWriter.push]

        Args:
            sequence_name: Unique name for the new sequence.
            client: The primary control FlightClient.
            config: Operational configuration (e.g., error policies, batch sizes).
        """

        # Initialize base class
        super().__init__(
            sequence_name=sequence_name,
            client=client,
            config=config,
            logger=logger,
        )

    # -------------------- Base class abstract method override --------------------
    def _on_context_enter(self):
        """
        Performs the server-side handshake to start the sequence update.

        Raises:
            Exception: If the server rejects the session creation or returns an empty response.
        """
        # Initialize a new session for the sequence
        super()._init_session(self._name)

    # NOTE: No need of overriding `_on_context_exit` as default behavior is ok.

    def topic_create(
        self,
        topic_name: str,
        metadata: dict[str, Any],
        ontology_type: Type[Serializable],
        on_error: TopicLevelErrorPolicy = TopicLevelErrorPolicy.Raise,
    ) -> Optional[TopicWriter]:
        """
        Creates a new topic within the active sequence.

        Args:
            topic_name: The relative name of the new topic.
            metadata: Topic-specific user metadata.
            ontology_type: The `Serializable` data model class defining the topic's schema.
            on_error: The error policy to use in the `TopicWriter`.

        Returns:
            A `TopicWriter` instance configured for parallel ingestion, or `None` if creation fails.

        Raises:
            RuntimeError: If called outside of a `with` block.

        Example:
            ```python
            with MosaicoClient.connect("localhost", 6726) as client:
                # Start the Sequence Orchestrator
                with client.sequence_create(...) as seq_writer: # (1)!
                    # Create individual Topic Writers
                    imu_writer = seq_writer.topic_create(
                        topic_name="sensors/imu", # The univocal topic name
                        metadata={ # The topic/sensor custom metadata
                            "vendor": "inertix-dynamics",
                            "model": "ixd-f100",
                            "firmware_version": "1.2.0",
                            "serial_number": "IMUF-9A31D72X",
                            "calibrated":"false",
                        },
                        ontology_type=IMU, # The ontology type stored in this topic
                    )

                    # Another individual topic writer for the GPS device
                    gps_writer = seq_writer.topic_create(
                        topic_name="sensors/gps", # The univocal topic name
                        metadata={ # The topic/sensor custom metadata
                            "role": "primary_gps",
                            "vendor": "satnavics",
                            "model": "snx-g500",
                            "firmware_version": "3.2.0",
                            "serial_number": "GPS-7C1F4A9B",
                            "interface": { # (2)!
                                "type": "UART",
                                "baudrate": 115200,
                                "protocol": "NMEA",
                            },
                        }, # The topic/sensor custom metadata
                        ontology_type=GPS, # The ontology type stored in this topic
                    )

                    # Push data
                    imu_writer.push( # (3)!
                        message=Message(
                            timestamp_ns=1700000000000,
                            data=IMU(acceleration=Vector3d(x=0, y=0, z=9.81), ...),
                        )
                    )
                    # ...

                # Exiting the block automatically flushes all topic buffers and finalizes the sequence on the server
            ```

            1. See also: [`MosaicoClient.sequence_create()`][mosaicolabs.comm.MosaicoClient.sequence_create]
            2. The metadata fields will be queryable via the `Query` mechanism.
                The mechanism allows creating query expressions like: `QueryTopic().with_user_metadata("interface.type", eq="UART")`.
                See also:
                * [`mosaicolabs.models.platform.Topic`][mosaicolabs.models.platform.Topic]
                * [`mosaicolabs.models.query.builders.QueryTopic`][mosaicolabs.models.query.builders.QueryTopic].
            3. See also: [`TopicWriter.push()`][mosaicolabs.handlers.TopicWriter.push]
        """
        # Override for cutomizing documentation
        return super().topic_create(
            topic_name=topic_name,
            metadata=metadata,
            ontology_type=ontology_type,
            on_error=on_error,
        )
