"""
Sequence Writing Module.

This module acts as the central controller for writing a sequence of data.
It manages the lifecycle of the sequence on the server (Create -> Write -> Finalize)
and distributes client resources (Connections, Executors) to individual Topics.
"""

from typing import Any, Optional, Type

import pyarrow.flight as fl

from ..comm.connection import _ConnectionPool
from ..comm.do_action import _do_action
from ..comm.executor_pool import _ExecutorPool
from ..enum import (
    FlightAction,
    SequenceStatus,
    SessionLevelErrorPolicy,
    TopicLevelErrorPolicy,
)
from ..logging_config import get_logger
from ..models import Serializable
from .base_session_writer import _BaseSessionWriter
from .config import SessionWriterConfig
from .helpers import _make_exception, _validate_metadata, _validate_sequence_name
from .topic_writer import TopicWriter

# Set the hierarchical logger
logger = get_logger(__name__)


class SequenceWriter(_BaseSessionWriter):
    """
    Orchestrates the creation and data ingestion lifecycle of a Mosaico Sequence.

    The `SequenceWriter` is the central controller for high-performance data writing.
    It manages the transition of a sequence through its lifecycle states: **Create** -> **Write** -> **Finalize**.

    ### Key Responsibilities
    * **Lifecycle Management**: Coordinates creation, finalization, or abort signals with the server.
    * **Resource Distribution**: Implements a "Multi-Lane" architecture by distributing network connections
        from a Connection Pool and thread executors from an Executor Pool to individual
        [`TopicWriter`][mosaicolabs.handlers.TopicWriter]
        instances. This ensures strict isolation and maximum parallelism between
        diverse data streams.


    Important: Usage Pattern
        This class **must** be used within a `with` statement (Context Manager).
        The context entry triggers sequence registration on the server, while the exit handles
        automatic finalization or error cleanup based on the configured `SessionLevelErrorPolicy`.

    Important: Obtaining a Writer
        Do not instantiate this class directly. Use the
        [`MosaicoClient.sequence_create()`][mosaicolabs.comm.MosaicoClient.sequence_create]
        factory method.
    """

    # -------------------- Constructor --------------------
    def __init__(
        self,
        *,
        sequence_name: str,
        client: fl.FlightClient,
        connection_pool: Optional[_ConnectionPool],
        executor_pool: Optional[_ExecutorPool],
        metadata: dict[str, Any],
        config: SessionWriterConfig,
    ):
        """
        Internal constructor for SequenceWriter.

        **Do not call this directly.** Users must call
        [`MosaicoClient.sequence_create()`][mosaicolabs.comm.MosaicoClient.sequence_create]
        to obtain an initialized writer.

        Example:
            ```python
            from mosaicolabs import MosaicoClient, SessionLevelErrorPolicy

            # Open the connection with the Mosaico Client
            with MosaicoClient.connect("localhost", 6726) as client:
                # Start the Sequence Orchestrator
                with client.sequence_create( # (1)!
                    sequence_name="mission_log_042",
                    # Custom metadata for this data sequence.
                    metadata={
                        "driver": {
                            "driver_id": "drv_sim_017",
                            "role": "validation",
                            "experience_level": "senior",
                        },
                        "location": {
                            "city": "Milan",
                            "country": "IT",
                            "facility": "Downtown",
                            "gps": {
                                "lat": 45.46481,
                                "lon": 9.19201,
                            },
                        },
                    }
                    on_error = SessionLevelErrorPolicy.Delete
                    ) as seq_writer:
                        # Start creating topics and pushing data
                        # (2)!

                # Exiting the block automatically flushes all topic buffers, finalizes the sequence on the server
                # and closes all connections and pools
            ```

            1. See also: [`MosaicoClient.sequence_create()`][mosaicolabs.comm.MosaicoClient.sequence_create]
            2. See also:
                * [`SequenceWriter.topic_create()`][mosaicolabs.handlers.SequenceWriter.topic_create]
                * [`TopicWriter.push()`][mosaicolabs.handlers.TopicWriter.push]

        Args:
            sequence_name: Unique name for the new sequence.
            client: The primary control FlightClient.
            connection_pool: Shared pool of data connections for parallel writing.
            executor_pool: Shared pool of thread executors for asynchronous I/O.
            metadata: User-defined metadata dictionary.
            config: Operational configuration (e.g., error policies, batch sizes).
        """
        _validate_sequence_name(sequence_name)
        _validate_metadata(metadata)
        self._metadata: dict[str, Any] = metadata
        """The metadata of the new sequence"""

        # Initialize base class
        super().__init__(
            sequence_name=sequence_name,
            client=client,
            config=config,
            connection_pool=connection_pool,
            executor_pool=executor_pool,
            logger=logger,
        )

    # -------------------- Base class abstract method override --------------------
    def _on_context_enter(self):
        """
        Performs the server-side handshake to create the new sequence.

        Triggers the `SEQUENCE_CREATE` action, transmitting the sequence name
        and initial metadata. Then inits a new session.

        Raises:
            Exception: If the server rejects the creation or returns an empty response.
        """
        # 1. Send the `SEQUENCE_CREATE` command, to create the remote resource. This returns no response
        _do_action(
            client=self._control_client,
            action=FlightAction.SEQUENCE_CREATE,
            payload={
                "locator": self._name,
                "user_metadata": self._metadata,
            },
            expected_type=None,
        )

        # 2. Initialize a new session for the sequence
        super()._init_session(self._name)

    def _on_context_exit(
        self,
        exc_type: Optional[Type[BaseException]],
        exc_val: Optional[BaseException],
        exc_tb: Optional[Any],
    ) -> None:
        """
        Override the __exit__ method to handle exceptions and apply `on_error` policies.

        Args:
            exc_type: The type of the exception.
            exc_value: The exception value.
            traceback: The traceback.

        Returns:
            The return value of the base class __exit__ method.
        """

        # Run base session cleanup
        super()._on_context_exit(exc_type, exc_val, exc_tb)

        # Apply policy upon exception caught in the context
        if (
            exc_type is not None
            and self._config.on_error == SessionLevelErrorPolicy.Delete
        ):
            self._logger.error(
                f"Sequence writer for sequence {self._name} caught exception: '{exc_val}'."
                f"Triggering `SessionLevelErrorPolicy.Delete`."
            )
            # Delete the sequence
            self._delete()

    def _delete(self):
        """Internal: Sends Delete command (Delete policy)."""
        if self._status != SequenceStatus.Finalized:
            try:
                _do_action(
                    client=self._control_client,
                    action=FlightAction.SEQUENCE_DELETE,
                    payload={
                        "locator": self._name,
                    },
                    expected_type=None,
                )
                self._logger.info(f"Sequence '{self._name}' deleted successfully.")
                self._status = SequenceStatus.Error
            except Exception as e:
                raise _make_exception(
                    f"Error sending 'delete' for sequence '{self._name}'.",
                    e,
                )

    def topic_create(
        self,
        topic_name: str,
        metadata: dict[str, Any],
        ontology_type: Type[Serializable],
        on_error: TopicLevelErrorPolicy = TopicLevelErrorPolicy.Raise,
    ) -> Optional[TopicWriter]:
        """
        Creates a new topic within the active sequence.

        This method performs a "Multi-Lane" resource assignment, granting the new
        [`TopicWriter`][mosaicolabs.handlers.TopicWriter], its own connection from the pool
        and a dedicated executor for background serialization and I/O.

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
                    # Each writer gets its own assigned resources from the pools
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

                # Exiting the block automatically flushes all topic buffers, finalizes the sequence on the server
                # and closes all connections and pools
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

    @property
    def status(self) -> SequenceStatus:
        """
        Returns the current operational status of the sequence.

        Returns:
            The [`SequenceStatus`][mosaicolabs.enum.SequenceStatus].
        """
        return self._status
