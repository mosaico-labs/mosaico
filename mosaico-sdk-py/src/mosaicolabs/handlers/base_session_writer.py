"""
Session (Base) Writing Module.

This module acts as the central controller for writing a session in an existing sequence.
It manages the lifecycle of the session on the server (Create -> Write -> Finalize/Abort)
and distributes client resources (Connections, Executors) to individual Topics.
"""

from abc import ABC, abstractmethod
from dataclasses import asdict, fields
from logging import Logger
from typing import Any, Dict, Optional, Type

import pyarrow.flight as fl

from mosaicolabs.comm.connection import _ConnectionPool
from mosaicolabs.comm.do_action import _do_action, _DoActionResponseUUID
from mosaicolabs.comm.executor_pool import _ExecutorPool
from mosaicolabs.enum import (
    FlightAction,
    SessionLevelErrorPolicy,
    SessionStatus,
    TopicLevelErrorPolicy,
)
from mosaicolabs.handlers.config import (
    SessionWriterConfig,
    TopicWriterConfig,
    WriterConfig,
)
from mosaicolabs.handlers.helpers import (
    _make_exception,
    _validate_metadata,
    _validate_topic_name,
)
from mosaicolabs.handlers.topic_writer import TopicWriter
from mosaicolabs.helpers import pack_topic_resource_name
from mosaicolabs.models import Serializable


class _BaseSessionWriter(ABC):
    """
    Abstract base class that orchestrates the creation and data ingestion lifecycle of a Mosaico Session.

    This is the central controller for high-performance managing:

    ### Key Responsibilities
    * **Distribution** of shared client resources (Connection and Executor pools) across multiple isolated `TopicWriter` instances.
    * **Lifecycle Management**: Coordinates creation, finalization, or abort signals with the server.
    * **Resource Distribution**: Implements a "Multi-Lane" architecture by distributing network connections
        from a Connection Pool and thread executors from an Executor Pool to individual
        [`TopicWriter`][mosaicolabs.handlers.TopicWriter]
        instances. This ensures strict isolation and maximum parallelism between
        diverse data streams.

    **Implementation Note:**
    This class follows the Template Method pattern. Subclasses must implement
    `_on_context_enter` to define the initial server handshake.
    Default closing operations (called on __exit__ execution) are performed by this class.
    To customize such operations, user must override `_on_context_exit` method
    """

    # -------------------- Constructor --------------------
    def __init__(
        self,
        *,
        sequence_name: str,
        client: fl.FlightClient,
        connection_pool: Optional[_ConnectionPool],
        executor_pool: Optional[_ExecutorPool],
        config: SessionWriterConfig,
        logger: Logger,
    ):
        """
        Internal constructor for _BaseSessionWriter.

        **Do not call this directly.** This is an internal base class for the
        [`SequenceWriter`][mosaicolabs.handlers.SequenceWriter] class.
        Users must call [`MosaicoClient.sequence_create()`][mosaicolabs.comm.MosaicoClient.sequence_create]
        to obtain a user-facing writer instance.

        Args:
            sequence_name: Unique name for the sequence corresponding to the session.
            client: The primary control FlightClient.
            connection_pool: Shared pool of data connections for parallel writing.
            executor_pool: Shared pool of thread executors for asynchronous I/O.
            config: Operational configuration (e.g., error policies, batch sizes).
        """
        self._name: str = sequence_name
        """The name of the sequence this session refers to"""
        self._config: SessionWriterConfig = config
        """The config of the writer"""
        self._topic_writers: Dict[str, TopicWriter] = {}
        """The cache of the spawned topic writers"""
        self._control_client: fl.FlightClient = client
        """The FlightClient used for operations (creating topics, finalizing session)."""
        self._connection_pool: Optional[_ConnectionPool] = connection_pool
        """The pool of FlightClients available for data streaming."""
        self._executor_pool: Optional[_ExecutorPool] = executor_pool
        """The pool of ThreadPoolExecutors available for asynch I/O."""
        self._status: SessionStatus = SessionStatus.Null
        """The status of the new session"""
        self._uuid: str = ""
        """The session uuid for remote handshaking"""
        self._entered: bool = False
        """Tag for inspecting if the writer is used in a 'with' context"""
        self._logger: Logger = logger
        """The logger for the writer"""

    @abstractmethod
    def _on_context_enter(self):
        pass

    def _init_session(self, sequence_name: str):
        """
        Initializes a new session on the existing remote resource.

        Args:
            sequence_name: The name of the sequence owning the data pushed in this session.
        """

        # Send the `SESSION_CREATE` action, to start a new session on the existing remote resource.
        act_resp = _do_action(
            client=self._control_client,
            action=FlightAction.SESSION_CREATE,
            payload={
                "locator": sequence_name,
            },
            expected_type=_DoActionResponseUUID,
        )
        if act_resp is None:
            raise Exception(
                f"Action '{FlightAction.SESSION_CREATE.value}' returned no response, sequence '{sequence_name}'."
            )

        self._uuid = act_resp.uuid
        self._entered = True
        self._status = SessionStatus.Pending

    def _on_context_exit(
        self,
        exc_type: Optional[Type[BaseException]],
        exc_val: Optional[BaseException],
        exc_tb: Optional[Any],
    ) -> None:
        """
        Executes the default finalization and cleanup logic for the session.

        **Summary of Operations:**
        1.  **Detection**: Determines if the context was exited due to an error
            or a successful completion.
        2.  **Topic Orchestration**:
            - **On Success**: Triggers a normal `_finalize()` on all active `TopicWriter`
              instances to flush remaining buffers.
            - **On Failure**: Triggers an error-mode `_finalize()` on all topics to
              ensure immediate resource release without data integrity guarantees.
        3.  **Server Lifecycle Handshake**:
            - **Success Path**: Calls `self.close()` to send a finalization signal
              (default: `SESSION_FINALIZE`) to the server.
            - **Error Path**: Evaluates the `SessionWriterConfig.on_error` policy to either
              `_abort()` (delete the session) or `_error_report()` to the server.
        4.  **Status Integrity**: Updates the internal `_status` to `Error`
            if any part of the process fails.

        .. Notes::
        - Override this method if your specific session implementation requires
        custom teardown logic that differs from the standard Mosaico flow.
        Examples include:
            - Releasing additional local resources (file handles, hardware locks).
            - Implementing a different "commit" logic for the session.
            - Sending specialized notification signals to the server upon exit.

        Args:
            exc_type: The type of the exception raised in the with-block, if any.
            exc_val: The instance of the exception raised in the with-block, if any.
            exc_tb: The traceback of the exception raised in the with-block, if any.
        """
        error_in_block = exc_type is not None
        out_exc = exc_val

        if not error_in_block:
            try:
                # Normal Exit: Finalize everything
                self._close_topics()
                self._finalize()

            except Exception as e:
                # An exception occurred during cleanup or finalization
                self._logger.error(
                    f"Exception during __exit__ for session {self._uuid}, sequence '{self._name}': '{e}'"
                )
                # notify error and go on
                out_exc = e
                error_in_block = True
                # Re-handle later

        if error_in_block:  # either in with block or after close operations
            # Exception occurred: Clean up and handle policy
            self._logger.error(
                f"Exception caught in _BaseSessionWriter block for session {self._uuid}, sequence  '{self._name}'. Inner err: '{out_exc}'"
            )
            try:
                self._close_topics(error=out_exc)
            except Exception as e:
                self._logger.error(
                    f"Exception while finalizing topics for session {self._uuid}, sequence '{self._name}': '{e}'"
                )
                out_exc = e

            # Apply the session-level error policy
            try:
                if self._config.on_error == SessionLevelErrorPolicy.Delete:
                    # TODO: maybe convenient to explicitly deal with a possible "Unauthorized" error
                    self._delete()
                else:
                    self._error_report(str(out_exc))
                    self._finalize()
            except Exception as e:
                self._logger.error(
                    f"Exception while handling error policy or finalizing the session {self._uuid}, sequence '{self._name}': '{e}'"
                )
                out_exc = e

            # Last thing to do: DO NOT SET BEFORE!
            self._status = SessionStatus.Error

            if exc_type is None and out_exc is not None:
                self._logger.error(
                    f"Exception caught while handling errors in termination phase. Inner err: '{out_exc}'"
                )
                raise out_exc

    # --- Context Manager ---
    def __enter__(self) -> "_BaseSessionWriter":
        """
        Activates the session writer and initializes server-side resources.

        This method executes the specific lifecycle hook defined by the subclass
        (via `_on_context_enter`) to prepare the server for data intake. Once
        initialized, the writer enters a 'Pending' state, enabling the creation
        of topics and parallel data streaming.

        Returns:
            _BaseSessionWriter: The initialized and ready-to-write instance.
        """
        self._on_context_enter()
        return self

    def __exit__(
        self,
        exc_type: Optional[Type[BaseException]],
        exc_val: Optional[BaseException],
        exc_tb: Optional[Any],
    ) -> None:
        """
        Finalizes the session.

        - If successful: Finalizes all topics and the session itself.
        - If error: Finalizes topics in error mode and either Aborts (Delete)
          or Reports the error based on `SessionWriterConfig.on_error`.

        Args:
            exc_type: The type of the exception.
            exc_val: The exception value.
            exc_tb: The traceback.

        Returns:
            None: prevents exception suppression
        """
        try:
            return self._on_context_exit(
                exc_type=exc_type,
                exc_val=exc_val,
                exc_tb=exc_tb,
            )
        except Exception as cleanup_exc:
            if exc_val is not None:
                raise cleanup_exc from exc_val  # chain exceptions
            raise cleanup_exc

    def __del__(self):
        """Destructor check to warn if the writer was left pending."""
        name = getattr(self, "_name", "__not_initialized__")
        status = getattr(self, "_status", SessionStatus.Null)

        if status == SessionStatus.Pending:
            self._logger.warning(
                f"_BaseSessionWriter' for sequence '{name}' destroyed without calling close(). "
                "Resources may not have been released properly."
            )

    def _check_entered(self):
        """Ensures methods are only called inside a `with` block."""
        if not self._entered:
            raise RuntimeError(
                "SequenceWriter or SequenceUpdater must be used within a 'with' block."
            )

    # --- Private lifetime management methods ---
    def _finalize(self):
        """
        Finalizes the session on the server.

        Sends the `SESSION_FINALIZE` signal, which instructs the server to mark all
        ingested data as immutable.

        Note: Automatic Finalization
            This is called automatically on the `with` block exit.

        Raises:
            RuntimeError: If called outside of a `with` block.
            Exception: If the server-side finalization fails.
        """
        self._check_entered()
        if self._status == SessionStatus.Pending:
            try:
                _do_action(
                    client=self._control_client,
                    action=FlightAction.SESSION_FINALIZE,
                    payload={
                        "session_uuid": self._uuid,
                    },
                    expected_type=None,
                )
                self._status = SessionStatus.Finalized
                self._logger.info(
                    f"Session {self._uuid}, sequence '{self._name}' finalized successfully."
                )
                return
            except Exception as e:
                # _do_action raised: re-raise
                self._status = SessionStatus.Error  # Sets status to Error
                raise _make_exception(
                    f"Error sending 'finalize' action for session {self._uuid}, sequence '{self._name}'. Server state may be inconsistent.",
                    e,
                )

    def _error_report(self, err: str):
        """Internal: Sends error report to server."""
        if self._status == SessionStatus.Pending:
            err_msg = (
                f"Exception caught in _BaseSessionWriter block for session "
                f"{self._uuid}, sequence  '{self._name}'.\nInner err: '{err}'"
            )
            try:
                _do_action(
                    client=self._control_client,
                    action=FlightAction.SEQUENCE_NOTIFICATION_CREATE,
                    payload={
                        "locator": self._name,
                        "notification_type": "error",
                        "msg": err_msg,
                    },
                    expected_type=None,
                )
                self._logger.info(
                    f"Session {self._uuid}, sequence '{self._name}' reported error: '{err_msg}'"
                )
            except Exception as e:
                raise _make_exception(
                    f"Error sending 'sequence_report_error' for session '{self._uuid}', sequence '{self._name}'.",
                    e,
                )

    def _delete(self):
        """Internal: Sends Abort command (Delete policy)."""
        if self._status != SessionStatus.Finalized:
            try:
                _do_action(
                    client=self._control_client,
                    action=FlightAction.SESSION_DELETE,
                    payload={
                        "session_uuid": self._uuid,
                    },
                    expected_type=None,
                )
                self._logger.info(
                    f"Session {self._uuid}, sequence '{self._name}' aborted successfully."
                )
                self._status = SessionStatus.Error
            except Exception as e:
                raise _make_exception(
                    f"Error sending 'abort' for session {self._uuid}, sequence '{self._name}'.",
                    e,
                )

    def _close_topics(self, error: Optional[BaseException] = None) -> None:
        """
        Iterates over all TopicWriters and finalizes them.
        """
        self._logger.info(
            f"Freeing TopicWriters {'WITH ERROR' if error is not None else ''} for session {self._uuid}, sequence '{self._name}'."
        )
        errors = []
        for topic_name, twriter in self._topic_writers.items():
            try:
                if twriter.is_active:
                    twriter._finalize(error=error)
            except Exception as e:
                self._logger.error(
                    f"Failed to finalize topic '{topic_name}' for session {self._uuid}, sequence '{self._name}'.: '{e}'"
                )
                errors.append(e)

        # Delete all TopicWriter instances, nothing can be done from here on
        self._topic_writers = {}

        if errors:
            first_error = errors[0]
            # Raise for the `_on_context_exit` to handle the error
            raise _make_exception(
                f"Errors occurred closing topics for session {self._uuid}, sequence '{self._name}': {len(errors)} topic(s) failed to finalize.",
                first_error,
            )

    # --- Public API ---
    def topic_create(
        self,
        topic_name: str,
        metadata: dict[str, Any],
        ontology_type: Type[Serializable],
        on_error: TopicLevelErrorPolicy = TopicLevelErrorPolicy.Raise,
    ) -> Optional[TopicWriter]:
        """
        Creates a new topic within the active session.

        This method performs a "Multi-Lane" resource assignment, granting the new
        [`TopicWriter`][mosaicolabs.handlers.TopicWriter], its own connection from the pool
        and a dedicated executor for background serialization and I/O.

        Note:
            The class verifies that the topic name is valid and that the metadata is valid (i.e. it is a dict).

        Args:
            topic_name: The relative name of the new topic.
            metadata: Topic-specific user metadata.
            ontology_type: The `Serializable` data model class defining the topic's schema.
            on_error: The error policy to use in the `TopicWriter`.

        Returns:
            A `TopicWriter` instance configured for parallel ingestion, or `None` if creation fails.

        Raises:
            RuntimeError: If called outside of a `with` block.
        """
        self._check_entered()

        ACTION = FlightAction.TOPIC_CREATE
        if topic_name in self._topic_writers:
            self._logger.error(f"Topic '{topic_name}' already exists in this sequence.")
            return None

        _validate_topic_name(topic_name)
        _validate_metadata(metadata)

        self._logger.debug(
            f"Requesting new topic '{topic_name}' for sequence '{self._name}'"
        )

        try:
            # Register topic on server
            act_resp = _do_action(
                client=self._control_client,
                action=ACTION,
                payload={
                    "session_uuid": self._uuid,
                    "locator": pack_topic_resource_name(self._name, topic_name),
                    "serialization_format": ontology_type.__serialization_format__.value,
                    "ontology_tag": ontology_type.__ontology_tag__,
                    "user_metadata": metadata,
                },
                expected_type=_DoActionResponseUUID,
            )
        except Exception as e:
            self._logger.error(
                str(
                    _make_exception(
                        f"Failed to execute '{ACTION.value}' action for session {self._uuid}, sequence '{self._name}', topic '{topic_name}'.",
                        e,
                    )
                )
            )
            return None

        if act_resp is None:
            self._logger.error(f"Action '{ACTION.value}' returned no response.")
            return None

        # --- Resource Assignment Strategy ---
        if self._connection_pool:
            # Round-Robin assignment from the pool (Async mode)
            data_client = self._connection_pool.get_next()
        else:
            # Reuse control client (Sync mode)
            data_client = self._control_client

        # Assign executor if pool is available
        executor = self._executor_pool.get_next() if self._executor_pool else None

        # Copy the common values in TopicWriterConfig from SessionWriterConfig
        # and add the TopicLevelErrorPolicy
        session_writer_config_data = asdict(self._config)
        writer_config_fields = {field.name for field in fields(WriterConfig)}
        writer_config_data = {
            k: v
            for k, v in session_writer_config_data.items()
            if k in writer_config_fields
        }
        topic_writer_config = TopicWriterConfig(on_error=on_error, **writer_config_data)

        try:
            writer = TopicWriter._create(
                sequence_name=self._name,
                topic_name=topic_name,
                topic_uuid=act_resp.uuid,
                client=data_client,
                executor=executor,
                ontology_type=ontology_type,
                config=topic_writer_config,
            )
            self._topic_writers[topic_name] = writer

        except Exception as e:
            self._logger.error(
                str(
                    _make_exception(
                        f"Failed to initialize 'TopicWriter' for session {self._uuid}, sequence '{self._name}', topic '{topic_name}'. Topic will be deleted from db.",
                        e,
                    )
                )
            )
            try:
                _do_action(
                    client=self._control_client,
                    action=FlightAction.TOPIC_DELETE,
                    payload={
                        "locator": pack_topic_resource_name(self._name, topic_name)
                    },
                    expected_type=None,
                )
            except Exception:
                self._logger.error(
                    str(
                        _make_exception(
                            f"Failed to send TOPIC_DELETE do_action for session {self._uuid}, sequence '{self._name}', topic '{topic_name}'.",
                            e,
                        )
                    )
                )
            return None

        return writer

    @property
    def session_status(self) -> SessionStatus:
        """
        Returns the current operational status of the session corresponding to this sequence write or update.

        Returns:
            The [`SessionStatus`][mosaicolabs.enum.SessionStatus].
        """
        self._check_entered()
        return self._status

    @property
    def session_uuid(self) -> str:
        """
        Returns the UUID of the session corresponding to this sequence write or update.

        Returns:
            The UUID of the session.
        """
        self._check_entered()
        return self._uuid

    def topic_writer_exists(self, topic_name: str) -> bool:
        """
        Checks if a [`TopicWriter`][mosaicolabs.handlers.TopicWriter] has already been initialized
        for the given name.

        Args:
            topic_name: The name of the topic to check.

        Returns:
            True if the topic writer exists locally, False otherwise.
        """
        self._check_entered()
        return topic_name in self._topic_writers

    def list_topic_writers(self) -> list[str]:
        """
        Returns the list of all topic names currently managed by this writer.
        """
        self._check_entered()
        return [k for k in self._topic_writers.keys()]

    def get_topic_writer(self, topic_name: str) -> Optional[TopicWriter]:
        """
        Retrieves an existing [`TopicWriter`][mosaicolabs.handlers.TopicWriter] instance from the internal cache.

        This method is particularly useful when ingesting data from unified recording formats where
        different sensor types (e.g., Vision, IMU, Odometry) are stored chronologically
        in a single stream or file.

        In these scenarios, messages for various topics appear in an interleaved fashion.
        Using `get_topic_writer` allows the developer to:

        * **Reuse Buffers:** Efficiently switch between writers for different sensor streams.
        * **Ensure Data Ordering:** Maintain a consistent batching logic for each topic as
          you iterate through a mixed-content log.
        * **Optimize Throughput:** Leverage Mosaico's background I/O by routing all data
          for a specific identifier through a single, persistent writer instance.

        Args:
            topic_name: The unique name or identifier of the topic writer to retrieve.

        Returns:
            The `TopicWriter` instance if it has been previously initialized within this `SequenceWriter` context, otherwise `None`.

        Example:
            Processing a generic interleaved sensor log (like a ROS bag or a custom JSON log):

            ```python
            from mosaicolabs import SequenceWriter, IMU, Image

            # Topic to Ontology Mapping: Defines the schema for each sensor stream
            # Example: {"/camera": Image, "/imu": IMU}
            topic_to_ontology = { ... }

            # Adapter Factory: Maps raw sensor payloads to Mosaico Ontology instances
            # Example: {"/imu": lambda p: IMU(acceleration=Vector3d.from_list(p['acc']), ...)}
            adapter = { ... }

            with client.sequence_create("physical_ai_trial_01") as seq_writer:
                # log_iterator represents an interleaved stream (e.g., ROSbags, MCAP, or custom logs).
                for ts, topic, payload in log_iterator:

                    # Access the topic-specific buffer.
                    # get_topic_writer retrieves a persistent writer from the internal cache
                    twriter = seq_writer.get_topic_writer(topic)

                    if twriter is None:
                        # Dynamic Topic Registration.
                        # If the topic is encountered for the first time, register it using the
                        # pre-defined Ontology type to ensure data integrity.
                        twriter = seq_writer.topic_create(
                            topic_name=topic,
                            ontology_type=topic_to_ontology[topic]
                        )

                    # Data Transformation & Ingestion.
                    # The adapter converts the raw payload into a validated Mosaico object.
                    # push() handles high-performance batching and asynchronous I/O to the rust backend.
                    twriter.push( # (1)!
                        message=Message(
                            timestamp_ns=ts,
                            data=adapter[topic](payload),
                        )
                    )

            # SequenceWriter automatically calls _finalize() on all internal TopicWriters,
            # guaranteeing that every sensor measurement is safely committed to the platform.
            ```

            1. See also: [`TopicWriter.push()`][mosaicolabs.handlers.TopicWriter.push]
        """
        self._check_entered()
        return self._topic_writers.get(topic_name)
