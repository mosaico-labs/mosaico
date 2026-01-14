"""
Topic Writing Module.

This module handles the buffered writing of data to a specific topic.
It abstracts the PyArrow Flight `DoPut` stream, handling batching,
serialization, and connection management.
"""

from concurrent.futures import ThreadPoolExecutor
import json
from typing import Any, Dict, Type, Optional
from mosaicolabs.models.header import Header
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


class TopicWriter:
    """
    Manages the data stream for a single topic.

    This class accumulates ontology records in a memory buffer. When the buffer
    exceeds the configured limits (`max_batch_size_bytes` or `max_batch_size_records`),
    it serializes the data (potentially using a background executor) and
    sends it to the server via the Flight `DoPut` protocol.
    """

    def __init__(
        self,
        topic_name: str,
        sequence_name: str,
        client: fl.FlightClient,
        state: _TopicWriteState,
        config: WriterConfig,
    ):
        """
        Internal constructor. Use `TopicWriter.create()` instead.
        """
        self._fl_client: fl.FlightClient = client
        self._sequence_name: str = sequence_name
        self._name: str = topic_name
        self._wrstate = state
        self._config = config

    @classmethod
    def create(
        cls,
        sequence_name: str,
        topic_name: str,
        topic_key: str,
        client: fl.FlightClient,
        executor: Optional[ThreadPoolExecutor],
        metadata: Dict[str, Any],
        ontology_type: Type[Serializable],
        config: WriterConfig,
    ) -> "TopicWriter":
        """
        Factory method to initialize the TopicWriter.

        It performs the following setup:
        1. Validates the ontology data class.
        2. Configures the Flight Descriptor with the topic key.
        3. Opens the active `DoPut` stream to the server.
        4. Initializes the internal write state (buffer).

        Args:
            sequence_name (str): Parent sequence name.
            topic_name (str): Topic name.
            topic_key (str): Authentication key provided by the server.
            client (fl.FlightClient): Connection to use for writing.
            executor (Optional[ThreadPoolExecutor]): Thread for async serialization.
            metadata (Dict[str, Any]): Topic-level user metadata.
            ontology_type (Type[Serializable]): The class of data being written.
            config (WriterConfig): Batching and error settings.

        Returns:
            TopicWriter: An active writer instance.
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
            writer, _ = client.do_put(descriptor, Message.get_schema(ontology_type))
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

        return cls(topic_name, sequence_name, client, wrstate, config)

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
            self.finalize(with_error=error_occurred)
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
        """Destructor check to ensure `finalize()` was called."""
        name = getattr(self, "_name", "__not_initialized__")
        if hasattr(self, "finalized") and not self.finalized():
            logger.warning(
                f"TopicWriter '{name}' destroyed without calling finalize(). "
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
        try:
            _do_action(
                client=self._fl_client,
                action=FlightAction.TOPIC_NOTIFY_CREATE,
                payload={
                    "name": pack_topic_resource_name(self._sequence_name, self._name),
                    "notify_type": "error",
                    "msg": str(err),
                },
                expected_type=None,
            )
            logger.info(f"TopicWriter '{self._name}' reported error.")
        except Exception as e:
            raise _make_exception(
                f"Error sending 'topic_report_error' action for sequence '{self._name}'.",
                e,
            )

    # --- Writing Logic ---
    def push(
        self,
        message: Optional[Message] = None,
        message_timestamp_ns: Optional[int] = None,
        message_header: Optional[Header] = None,
        ontology_obj: Optional[Serializable] = None,
    ) -> None:
        """
        Adds a new record to the write buffer.

        This method supports two input modes:
        1. Passing a pre-built `Message` object.
        2. Passing distinct components (`ontology_obj`, `message_timestamp_ns`, `message_header`).

        If the internal buffer is full, this method will trigger a flush to the server.

        Args:
            message (Optional[Message]): A complete Message object.
            message_timestamp_ns (Optional[int]): Message Timestamp in **nanoseconds** (if message not provided).
            message_header (Optional[Header]): Header info (optional - if message not provided).
            ontology_obj (Optional[Serializable]): The ontology data payload (if message not provided).
        """
        msg = message
        if not msg:
            if message_timestamp_ns is not None and ontology_obj is not None:
                msg = Message(
                    timestamp_ns=message_timestamp_ns,
                    data=ontology_obj,
                    message_header=message_header,
                )
            else:
                raise ValueError(
                    "Expected a valid message or the couple 'message_timestamp_ns' + 'ontology_obj'."
                )

        try:
            self._wrstate.push_record(msg)
        except Exception as e:
            self._handle_exception_and_raise(e, "Error during TopicWriter.push")

    def finalized(self) -> bool:
        """Returns True if the writer stream has been closed."""
        return self._wrstate.writer is None

    def finalize(self, with_error: bool = False) -> None:
        """
        Flushes pending data and closes the Flight stream.

        Args:
            with_error (bool): If True, indicates the stream is closing due to an error,
                               which may skip flushing partial buffers.
        """
        try:
            self._wrstate.close(with_error=with_error)
        except Exception:
            raise

        logger.info(
            f"TopicWriter '{self._name}' finalized {'WITH ERROR' if with_error else ''} successfully."
        )
