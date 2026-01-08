"""
Sequence Writing Module.

This module acts as the central controller for writing a sequence of data.
It manages the lifecycle of the sequence on the server (Create -> Write -> Finalize)
and distributes client resources (Connections, Executors) to individual Topics.
"""

from typing import Any, Dict, Type, Optional
import pyarrow.flight as fl

from .config import WriterConfig
from .helpers import _make_exception, _validate_sequence_name
from .topic_writer import TopicWriter
from ..comm.do_action import _do_action, _DoActionResponseKey
from ..comm.connection import _ConnectionPool
from ..comm.executor_pool import _ExecutorPool
from ..enum import FlightAction, OnErrorPolicy, SequenceStatus
from ..helpers import pack_topic_resource_name
from ..logging import get_logger
from ..models import Serializable

# Set the hierarchical logger
logger = get_logger(__name__)


class SequenceWriter:
    """
    Orchestrates the creation and writing of a Sequence.

    **Key Responsibilities:**
    1.  **Lifecycle Management:** Sends creation, finalization, or abort signals to the server.
    2.  **Resource Distribution:** Implements the "Multi-Lane" architecture. It pulls
        network connections and thread executors from the `MosaicoClient` pools and
        assigns them to new `TopicWriter` instances. This ensures isolation and
        parallelism between topics (e.g., high-bandwidth video vs low-bandwidth telemetry).
    """

    # -------------------- Class attributes --------------------
    _name: str
    _metadata: Dict[str, Any]
    _topic_writers: Dict[str, TopicWriter]
    _control_client: fl.FlightClient
    """The FlightClient used for metadata operations (creating topics, finalizing sequence)."""

    _connection_pool: Optional[_ConnectionPool]
    """The pool of FlightClients available for data streaming."""

    _executor_pool: Optional[_ExecutorPool]
    """The pool of ThreadPoolExecutors available for asynch I/O."""

    _config: WriterConfig
    """Configuration object containing error policies and batch size limits."""

    _sequence_status: SequenceStatus = SequenceStatus.Null
    _key: Optional[str] = None
    _entered: bool = False

    # -------------------- Constructor --------------------
    def __init__(
        self,
        sequence_name: str,
        client: fl.FlightClient,
        connection_pool: Optional[_ConnectionPool],
        executor_pool: Optional[_ExecutorPool],
        metadata: dict[str, Any],
        config: WriterConfig,
    ):
        """
        Internal constructor. Use `MosaicoClient.sequence_create()` instead.
        """
        _validate_sequence_name(sequence_name)
        self._name: str = sequence_name
        self._metadata: dict[str, Any] = metadata
        self._config = config
        self._topic_writers: Dict[str, TopicWriter] = {}
        self._control_client = client
        self._connection_pool = connection_pool
        self._executor_pool = executor_pool

    # --- Context Manager ---
    def __enter__(self) -> "SequenceWriter":
        """
        Initializes the sequence on the server.

        Sends `SEQUENCE_CREATE` and retrieves the unique `sequence_key` needed
        to authorize topic creation.
        """
        ACTION = FlightAction.SEQUENCE_CREATE

        act_resp = _do_action(
            client=self._control_client,
            action=ACTION,
            payload={
                "name": self._name,
                "user_metadata": self._metadata,
            },
            expected_type=_DoActionResponseKey,
        )

        if act_resp is None:
            raise Exception(f"Action '{ACTION.value}' returned no response.")

        self._key = act_resp.key
        # Set internal state and return self
        self._entered = True
        self._sequence_status = SequenceStatus.Pending
        return self

    def __exit__(
        self,
        exc_type: Optional[Type[BaseException]],
        exc_val: Optional[BaseException],
        exc_tb: Optional[Any],
    ) -> None:
        """
        Finalizes the sequence.

        - If successful: Finalizes all topics and the sequence itself.
        - If error: Finalizes topics in error mode and either Aborts (Delete)
          or Reports the error based on `WriterConfig.on_error`.
        """
        error_in_block = exc_type is not None
        out_exc = exc_val

        if not error_in_block:
            try:
                # Normal Exit: Finalize everything
                self._close_topics(with_error=False)
                self.close()

            except Exception as e:
                # An exception occurred during cleanup or finalization
                logger.error(
                    f"Exception during __exit__ for sequence '{self._name}': '{e}'"
                )
                # notify error and go on
                out_exc = e
                error_in_block = True

        if error_in_block:  # either in with block or after close operations
            # Exception occurred: Clean up and handle policy
            logger.error(
                f"Exception in SequenceWriter '{self._name}' block. Inner err: '{out_exc}'"
            )
            try:
                self._close_topics(with_error=True)
            except Exception as e:
                logger.error(
                    f"Exception during __exit__ with error in block (finalizing topics) for sequence '{self._name}': '{e}'"
                )
                out_exc = e

            # Apply the sequence-level error policy
            if self._config.on_error == OnErrorPolicy.Delete:
                self._abort()
            else:
                self._error_report(str(out_exc))

            # Last thing to do: DO NOT SET BEFORE!
            self._sequence_status = SequenceStatus.Error

            if exc_type is None and out_exc is not None:
                raise out_exc  # Re-raise the cleanup error if it's the only one

    def __del__(self):
        """Destructor check to warn if the writer was left pending."""
        name = getattr(self, "_name", "__not_initialized__")
        status = getattr(self, "_sequence_status", SequenceStatus.Null)

        if status == SequenceStatus.Pending:
            logger.warning(
                f"SequenceWriter '{name}' destroyed without calling close(). "
                "Resources may not have been released properly."
            )

    def _check_entered(self):
        """Ensures methods are only called inside a `with` block."""
        if not self._entered:
            raise RuntimeError("SequenceWriter must be used within a 'with' block.")

    # --- Public API ---
    def topic_create(
        self,
        topic_name: str,
        metadata: dict[str, Any],
        ontology_type: Type[Serializable],
    ) -> Optional[TopicWriter]:
        """
        Creates a new topic within the sequence.

        This method assigns a dedicated connection and executor from the pool
        (if available) to the new topic, enabling parallel writing.

        Args:
            topic_name (str): The name of the new topic.
            metadata (dict[str, Any]): Topic-specific metadata.
            ontology_type (Type[Serializable]): The data model class.

        Returns:
            TopicWriter: A writer instance configured for this topic.
            None: If any error occurs


        """
        ACTION = FlightAction.TOPIC_CREATE
        self._check_entered()

        if topic_name in self._topic_writers:
            logger.error(f"Topic '{topic_name}' already exists in this sequence.")
            return None

        logger.debug(f"Requesting new topic '{topic_name}' for sequence '{self._name}'")

        try:
            # Register topic on server
            act_resp = _do_action(
                client=self._control_client,
                action=ACTION,
                payload={
                    "sequence_key": self._key,
                    "name": pack_topic_resource_name(self._name, topic_name),
                    "serialization_format": ontology_type.__serialization_format__.value,
                    "ontology_tag": ontology_type.__ontology_tag__,
                    "user_metadata": metadata,
                },
                expected_type=_DoActionResponseKey,
            )
        except Exception as e:
            logger.error(
                str(
                    _make_exception(
                        f"Failed to execute '{ACTION.value}' action for sequence '{self._name}', topic '{topic_name}'.",
                        e,
                    )
                )
            )
            return None

        if act_resp is None:
            logger.error(f"Action '{ACTION.value}' returned no response.")
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

        try:
            writer = TopicWriter.create(
                sequence_name=self._name,
                topic_name=topic_name,
                topic_key=act_resp.key,
                client=data_client,
                executor=executor,
                metadata=metadata,
                ontology_type=ontology_type,
                config=self._config,
            )
            self._topic_writers[topic_name] = writer

        except Exception as e:
            logger.error(
                str(
                    _make_exception(
                        f"Failed to initialize 'TopicWriter' for sequence '{self._name}', topic '{topic_name}'. Topic will be deleted from db.",
                        e,
                    )
                )
            )
            try:
                _do_action(
                    client=self._control_client,
                    action=FlightAction.TOPIC_DELETE,
                    payload={"name": pack_topic_resource_name(self._name, topic_name)},
                    expected_type=None,
                )
            except Exception:
                logger.error(
                    str(
                        _make_exception(
                            f"Failed to send TOPIC_DELETE do_action for sequence '{self._name}', topic '{topic_name}'.",
                            e,
                        )
                    )
                )
            return None

        return writer

    def sequence_status(self) -> SequenceStatus:
        """Returns the current status of the sequence."""
        return self._sequence_status

    def close(self):
        """
        Explicitly finalizes the sequence.

        Sends `SEQUENCE_FINALIZE` to the server, marking data as immutable.
        """
        self._check_entered()
        if self._sequence_status == SequenceStatus.Pending:
            try:
                _do_action(
                    client=self._control_client,
                    action=FlightAction.SEQUENCE_FINALIZE,
                    payload={
                        "name": self._name,
                        "key": self._key,
                    },
                    expected_type=None,
                )
                self._sequence_status = SequenceStatus.Finalized
                logger.info(f"Sequence '{self._name}' finalized successfully.")
                return
            except Exception as e:
                # _do_action raised: re-raise
                self._sequence_status = SequenceStatus.Error  # Sets status to Error
                raise _make_exception(
                    f"Error sending 'finalize' action for sequence '{self._name}'. Server state may be inconsistent.",
                    e,
                )

    def _error_report(self, err: str):
        """Internal: Sends error report to server."""
        if self._sequence_status == SequenceStatus.Pending:
            try:
                _do_action(
                    client=self._control_client,
                    action=FlightAction.SEQUENCE_NOTIFY_CREATE,
                    payload={
                        "name": self._name,
                        "notify_type": "error",
                        "msg": str(err),
                    },
                    expected_type=None,
                )
                logger.info(f"Sequence '{self._name}' reported error.")
            except Exception as e:
                raise _make_exception(
                    f"Error sending 'sequence_report_error' for '{self._name}'.", e
                )

    def _abort(self):
        """Internal: Sends Abort command (Delete policy)."""
        if self._sequence_status != SequenceStatus.Finalized:
            try:
                _do_action(
                    client=self._control_client,
                    action=FlightAction.SEQUENCE_ABORT,
                    payload={
                        "name": self._name,
                        "key": self._key,
                    },
                    expected_type=None,
                )
                logger.info(f"Sequence '{self._name}' aborted successfully.")
                self._sequence_status = SequenceStatus.Error
            except Exception as e:
                raise _make_exception(
                    f"Error sending 'abort' for sequence '{self._name}'.", e
                )

    def topic_exists(self, topic_name: str) -> bool:
        """Checks if a local TopicWriter exists for the name."""
        return topic_name in self._topic_writers

    def list_topics(self) -> list[str]:
        """Returns list of active topic names."""
        return [k for k in self._topic_writers.keys()]

    def get_topic(self, topic_name: str) -> Optional[TopicWriter]:
        """Retrieves a TopicWriter instance, if it exists."""
        return self._topic_writers.get(topic_name)

    def _close_topics(self, with_error: bool) -> None:
        """
        Iterates over all TopicWriters and finalizes them.
        """
        logger.info(
            f"Freeing TopicWriters {'WITH ERROR' if with_error else ''} for sequence '{self._name}'."
        )
        errors = []
        for topic_name, twriter in self._topic_writers.items():
            try:
                twriter.finalize(with_error=with_error)
            except Exception as e:
                logger.error(f"Failed to finalize topic '{topic_name}': '{e}'")
                errors.append(e)

        # Delete all TopicWriter instances, nothing can be done from here on
        self._topic_writers = {}

        if errors:
            first_error = errors[0]
            raise _make_exception(
                f"Errors occurred closing topics: {len(errors)} topic(s) failed to finalize.",
                first_error,
            )
