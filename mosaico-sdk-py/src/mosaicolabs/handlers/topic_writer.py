"""
Topic Writing Module.

This module handles the buffered writing of data to a specific topic.
It abstracts the PyArrow Flight `DoPut` stream, handling batching,
serialization, and connection management.
"""

from concurrent.futures import ThreadPoolExecutor
import json
from typing import Any, Type, Optional
from mosaicolabs.models.message import Message
import pyarrow.flight as fl

from mosaicolabs.models import Serializable
from .internal.topic_write_state import _TopicWriteState
from .helpers import _make_exception
from ..helpers import pack_topic_resource_name
from ..comm.do_action import _do_action
from ..enum import FlightAction, OnErrorPolicy
from .config import WriterConfig
from ..logging_config import get_logger

# Set the hierarchical logger
logger = get_logger(__name__)


# TODO: Better manage topic lifecycle and error policy handling
# Policies:
# - Report and Skip: Skip the current record and continue with the next one.
# - Report and Close: Report via topic_notify and close the writer. Must manage writer disabling and actions on calling push on a disabled writer
# - Report and Delete: Delete the topic and report the error via sequence_notify. Must manage writer disabling and actions on calling push on a disabled writer
class TopicWriter:
    """
    Manages a high-performance data stream for a single Mosaico topic.

    The `TopicWriter` abstracts the complexity of the PyArrow Flight `DoPut` protocol,
    handling internal buffering, serialization, and network transmission.
    It accumulates records in memory and automatically flushes them to the server when
    configured batch limits—defined by either byte size or record count—are exceeded.

    ### Performance & Parallelism
    If an executor pool is provided by the parent client, the `TopicWriter` performs
    data serialization on background threads, preventing I/O operations from blocking
    the main application logic.

    Important: Obtaining a Writer
        End-users should not instantiate this class directly. Use the
        [`SequenceWriter.topic_create()`][mosaicolabs.handlers.SequenceWriter.topic_create]
        factory method to obtain an active writer.
    """

    def __init__(
        self,
        *,
        topic_name: str,
        sequence_name: str,
        client: fl.FlightClient,
        state: _TopicWriteState,
        config: WriterConfig,
    ):
        """
        Internal constructor for TopicWriter.

        **Do not call this directly.** Internal library modules should use the
        `_create()` factory. Users must call
        [`SequenceWriter.topic_create()`][mosaicolabs.handlers.SequenceWriter.topic_create]
        to obtain an initialized writer.

        Example:
            ```python
            with MosaicoClient.connect("localhost", 6726) as client:
                # Start the Sequence Orchestrator
                with client.sequence_create(...) as seq_writer: # (1)!
                    # Create individual Topic Writers
                    # Each writer gets its own assigned resources from the pools
                    imu_writer = seq_writer.topic_create( # (2)!
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

                    # Push data...
                    imu_writer.push( # (3)!
                        message=Message(
                            data=IMU(acceleration=Vector3d(x=0, y=0, z=9.81), ...),
                            timestamp_ns=1700000000000,
                        )
                    )
                # Exiting the seq_writer `with` block, the `_finalize()` method of all topic writers is called.
            ```

            1. See also: [`MosaicoClient.sequence_create()`][mosaicolabs.comm.MosaicoClient.sequence_create]
            2. See also: [`SequenceWriter.topic_create()`][mosaicolabs.handlers.SequenceWriter.topic_create]
            3. See also: [`TopicWriter.push()`][mosaicolabs.handlers.TopicWriter.push]

        Args:
            topic_name: The name of the specific topic.
            sequence_name: The name of the parent sequence.
            client: The FlightClient used for data transmission.
            state: The internal state object managing buffers and streams.
            config: Operational configuration for batching and error handling.
        """
        self._fl_client: fl.FlightClient = client
        """The FlightClient used for writing operations."""
        self._sequence_name: str = sequence_name
        """The name of the created sequence"""
        self._name: str = topic_name
        """The name of the new topic"""
        self._config: WriterConfig = config
        """The config of the writer"""
        self._wrstate: _TopicWriteState = state
        """The actual writer object"""

    @classmethod
    def _create(
        cls,
        sequence_name: str,
        topic_name: str,
        topic_key: str,
        client: fl.FlightClient,
        executor: Optional[ThreadPoolExecutor],
        ontology_type: Type[Serializable],
        config: WriterConfig,
    ) -> "TopicWriter":
        """
        Internal Factory method to initialize an active TopicWriter.

        This method performs the underlying handshake with the Mosaico server to
        open a `DoPut` stream and initializes the memory buffers based on the
        provided ontology type.

        Important: **Do not call this directly**
            Users must call
            [`SequenceWriter.topic_create()`][mosaicolabs.handlers.SequenceWriter.topic_create]
            to obtain an initialized writer.

        Args:
            sequence_name: Name of the parent sequence.
            topic_name: Unique name for this topic stream.
            topic_key: authorization key provided by the server during creation.
            client: The connection to use for the data stream.
            executor: Optional thread pool for background serialization.
            ontology_type: The data model class defining the record schema.
            config: Batching limits and error policies.

        Returns:
            An active `TopicWriter` instance ready for data ingestion.

        Raises:
            ValueError: If the ontology type is not a valid `Serializable` subclass.
            Exception: If the Flight stream fails to open on the server.
        """
        # Validate Ontology Class requirements (must have tags and serialization format)
        cls._validate_ontology_type(ontology_type)

        # Create Flight Descriptor: Tells server where to route the data
        descriptor = fl.FlightDescriptor.for_command(
            json.dumps(
                {
                    "resource_locator": pack_topic_resource_name(
                        sequence_name, topic_name
                    ),
                    "key": topic_key,
                }
            )
        )

        # Open Flight Stream (DoPut)
        try:
            writer, _ = client.do_put(descriptor, Message._get_schema(ontology_type))
        except Exception as e:
            raise _make_exception(
                f"Failed to open Flight stream for topic '{topic_name}'", e
            )

        assert ontology_type.__ontology_tag__ is not None

        # Initialize Internal Write State (manages the buffer and flushing logic)
        wrstate = _TopicWriteState(
            topic_name=topic_name,
            ontology_tag=ontology_type.__ontology_tag__,
            writer=writer,
            executor=executor,
            max_batch_size_bytes=config.max_batch_size_bytes,
            max_batch_size_records=config.max_batch_size_records,
        )

        return cls(
            topic_name=topic_name,
            sequence_name=sequence_name,
            client=client,
            state=wrstate,
            config=config,
        )

    # --- Context Manager ---
    def __enter__(self) -> "TopicWriter":
        """Context manager entry."""
        return self

    def __exit__(
        self,
        exc_type: Optional[Type[BaseException]],
        exc_val: Optional[BaseException],
        exc_tb: Optional[Any],
    ) -> None:
        """
        Context manager exit.

        Guarantees cleanup of the Flight stream. If an exception occurred within
        the block, it triggers the configured `OnErrorPolicy` (e.g., reporting the error).
        Exceptions from the with-block are always propagated.
        """
        error_occurred = exc_type is not None

        try:
            # Attempt to flush remaining data and close stream
            self._finalize(error=exc_val)
        except Exception as e:
            # FINALIZE FAILED: treat this as an error condition
            logger.exception(f"Failed to finalize topic '{self._name}': '{e}'")
            error_occurred = True
            if not exc_type:
                exc_type, exc_val = type(e), e

        if error_occurred:
            # Exit due to an error (original, cleanup, or finalize failure)
            try:
                if self._config.on_error == OnErrorPolicy.Report:
                    self._error_report(str(exc_val))
            except Exception as e:
                logger.exception(
                    f"Error handling topic '{self._name}' after exception: '{e}'"
                )

    def __del__(self):
        """Destructor check to ensure `_finalize()` was called."""
        name = getattr(self, "_name", "__not_initialized__")
        if hasattr(self, "is_active") and self.is_active():
            logger.warning(
                f"TopicWriter '{name}' destroyed without calling _finalize(). "
                "Resources may not have been released properly."
            )

    def _handle_exception_and_raise(self, err: Exception, msg: str):
        """Helper to cleanup resources and re-raise exceptions with context."""
        try:
            if self._config.on_error == OnErrorPolicy.Report:
                self._error_report(str(err))
        except Exception as report_err:
            logger.error(f"Failed to report error: '{report_err}'")
        finally:
            # Always attempt to close local resources
            if hasattr(self, "_wrstate") and self._wrstate:
                self._wrstate.close(with_error=True)

        raise _make_exception(f"Topic '{self._name}' operation failed: '{msg}'", err)

    @classmethod
    def _validate_ontology_type(cls, ontology_type: Type[Serializable]) -> None:
        if not issubclass(ontology_type, Serializable):
            raise ValueError(
                f"Ontology class '{ontology_type.__name__}' is not serializable."
            )

    def _error_report(self, err: str):
        """Sends an 'error' notification to the server regarding this topic."""
        ACTION = FlightAction.TOPIC_NOTIFY_CREATE
        try:
            _do_action(
                client=self._fl_client,
                action=ACTION,
                payload={
                    "name": pack_topic_resource_name(self._sequence_name, self._name),
                    "notify_type": "error",
                    "msg": str(err),
                },
                expected_type=None,
            )
            logger.warning(f"TopicWriter '{self._name}' reported error: '{err}'.")
        except Exception as e:
            logger.error(
                _make_exception(
                    f"Error sending '{ACTION}' action for sequence '{self._name}'.",
                    e,
                )
            )

    # --- Writing Logic ---
    def push(
        self,
        message: Message,
    ) -> None:
        """
        Adds a new record to the internal write buffer.

        Records are accumulated in memory. If a push triggers a batch limit,
        the buffer is automatically serialized and transmitted to the server.

        Args:
            message: A pre-constructed Message object.

        Raises:
            Exception: If a buffer flush fails during the operation.

        Example:
            ```python
            with MosaicoClient.connect("localhost", 6726) as client:
                # Start the Sequence Orchestrator
                with client.sequence_create(...) as seq_writer: # (1)!
                    # Create individual Topic Writers
                    # Each writer gets its own assigned resources from the pools
                    imu_writer = seq_writer.topic_create( # (2)!
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
                            "interface": {
                                "type": "UART",
                                "baudrate": 115200,
                                "protocol": "NMEA",
                            },
                        }, # The topic/sensor custom metadata
                        ontology_type=GPS, # The ontology type stored in this topic
                    )

                    gps_msg = Message(timestamp_ns=1700000000100, data=GPS(...))
                    gps_writer.push(message=gps_msg)
                # Exiting the seq_writer `with` block, the `_finalize()` method of all topic writers is called.
            ```

            1. See also: [`MosaicoClient.sequence_create()`][mosaicolabs.comm.MosaicoClient.sequence_create]
            2. See also: [`SequenceWriter.topic_create()`][mosaicolabs.handlers.SequenceWriter.topic_create]
        """
        # time.sleep(0.1)
        try:
            self._wrstate.push_record(message)
        except Exception as e:
            self._handle_exception_and_raise(e, "Error during TopicWriter.push")

    @property
    def name(self) -> str:
        """Returns the name of the topic"""
        return self._name

    def is_active(self) -> bool:
        """
        Returns `True` if the writing stream is open and the writer accepts new messages.
        """
        return self._wrstate.writer is None

    def _finalize(self, error: Optional[BaseException] = None) -> None:
        """
        Flushes all remaining buffered data and closes the remote Flight stream.

        This method ensures that any data residing in the local memory buffer is sent to the
        server before the connection is severed. It transitions the individual topic stream
        into a terminal state, allowing the platform to catalog the ingested data.

        In advanced ingestion workflows, `_finalize()` is the primary mechanism for **sensor-level
        resilience**. If an error occurs during the preparation or pushing of data for a
        *specific* topic, you can catch the exception locally, call `_finalize(error=...)`
        for that topic, and allow other healthy sensors to continue their injection without
        triggering a global sequence-wide failure.

        ### Error Reporting
        When called with a non-null `error` object, this method automatically:

        * **Reports the Failure**: Sends a server-side notification containing the
            exception string.
        * **Aborts the Buffer**: Closes the state machine immediately without
            attempting to flush potentially corrupted data.

        Note: Automatic Finalization
            In typical workflows, you do not need to call this manually. It is
            **automatically invoked** by the `__exit__` method of the parent
            [`SequenceWriter`][mosaicolabs.handlers.SequenceWriter] when the `with`
            block scope is closed.

        Args:
            error: If provided, the writer generates an error report and closes the
                stream without attempting a final buffer flush.

        Raises:
            Exception: If the server fails to acknowledge the stream closure or if the
                internal state machine encounters a terminal error.
        """
        with_error = error is not None
        try:
            if with_error:
                self._error_report(str(error))
            self._wrstate.close(with_error=with_error)
        except Exception as e:
            # Close the writer anyway to prevent further operations
            self._wrstate.writer = None
            raise _make_exception(
                exc_msg=e,
                msg=f"Error finalizing TopicWriter '{self._name}'.",
            )

        logger.info(
            f"TopicWriter '{self._name}' finalized {'WITH ERROR' if error is not None else ''} successfully."
        )
