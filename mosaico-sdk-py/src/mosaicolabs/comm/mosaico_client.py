"""
Mosaico Client Entry Point.

This module provides the `MosaicoClient`, the primary interface for users to
interact with the Mosaico system. It manages the connection lifecycle,
resource pooling (connections and executors), and serves as a factory for
creating resource handlers (sequences, topics) and executing queries.
"""

import os
from typing import Any, Dict, List, Optional, Type
import pyarrow.flight as fl

from mosaicolabs.models.query import Query, QueryResponse
from mosaicolabs.models.query.protocols import QueryableProtocol

from ..helpers import pack_topic_resource_name
from ..handlers import TopicHandler, SequenceHandler, SequenceWriter
from .connection import _get_connection, _ConnectionStatus, _ConnectionPool
from .executor_pool import _ExecutorPool
from .do_action import _do_action, _DoActionQueryResponse, _DoActionResponseSysInfo
from ..logging_config import get_logger
from ..enum import FlightAction, OnErrorPolicy
from ..handlers.config import WriterConfig
from ..handlers.system_info import SystemInfo
from .connection import (
    DEFAULT_MAX_BATCH_BYTES,
    DEFAULT_MAX_BATCH_SIZE_RECORDS,
)

# Set the hierarchical logger
logger = get_logger(__name__)


class MosaicoClient:
    """
    The main client for the Mosaico Data-Platform.

    This class manages:
    1.  **Network Resources:** A control connection and a pool of data connections.
    2.  **Concurrency:** A pool of executors for handling async serialization/IO.
    3.  **State:** Caching of handlers to prevent redundant object creation.

    Usage:
        This class is designed to be used as a Context Manager:
        ```python
        with MosaicoClient.connect("localhost", 50051) as client:
            # use client...
        ```
    """

    # --- Private Sentinel Value ---
    # Used to ensure the constructor is only called via the `connect()` factory.
    _CONNECT_SENTINEL = object()

    # --- Class-level attributes ---
    _sequence_handlers_cache: Dict[str, SequenceHandler]
    """Cache for SequenceHandler instances, keyed by sequence_name. Used to avoid re-connecting for known sequences."""

    _topic_handlers_cache: Dict[str, TopicHandler]
    """Cache for TopicHandler instances, keyed by their resource ('sequence_name/topic_name') name."""

    _status: _ConnectionStatus = _ConnectionStatus.Closed
    """Tracks the current connection status (Open/Closed)."""

    _control_client: fl.FlightClient
    """The primary PyArrow Flight client used for SDK-Server control operations (e.g., creating layers, querying)."""

    _connection_pool: Optional[_ConnectionPool]
    """The pool of Flight clients used for parallel data writing."""

    _executor_pool: Optional[_ExecutorPool]
    """The pool of thread executors used for offloading serialization and I/O."""

    def __init__(
        self,
        control_client: fl.FlightClient,
        connection_pool: Optional[_ConnectionPool],
        executor_pool: Optional[_ExecutorPool],
        sentinel: object,
    ):
        """
        Internal initialization method.

        **Do not call this directly.** Use `MosaicoClient.connect()` instead.
        This constructor enforces the factory pattern by checking for a private sentinel.
        """
        if sentinel is not MosaicoClient._CONNECT_SENTINEL:
            raise RuntimeError(
                "MosaicoClient must be instantiated using the classmethod MosaicoClient.connect()."
            )

        self._control_client = control_client
        self._status = _ConnectionStatus.Open
        self._connection_pool = connection_pool
        self._executor_pool = executor_pool

        # Initialize caches
        self._sequence_handlers_cache = {}
        self._topic_handlers_cache = {}

    @classmethod
    def connect(
        cls,
        host: str,
        port: int,
        timeout: int = 5,
    ) -> "MosaicoClient":
        """
        Factory method to establish a connection to the Mosaico server.

        Args:
            host (str): The server host address.
            port (int): The server port.
            timeout (int): The waiting-for-connection timeout in seconds (default = 5s)

        Returns:
            MosaicoClient: An initialized client instance.

        Raises:
            fl.FlightUnavailableError: If the control connection cannot be established.
            Exception: If pool initialization fails.
        """

        # Establish the Control Connection
        logger.debug(f"Opening a connection '{host}:{port}'")
        try:
            control_client: fl.FlightClient = _get_connection(
                host=host, port=port, timeout=timeout
            )
        except Exception as e:
            raise ConnectionError(
                f"Connection to Flight server at '{host}:{port}' failed on startup.\nInner err: '{e}'"
            )

        # Initialize Pools
        connection_pool = None
        executor_pool = None

        try:
            # We attempt to create the connection pool. We use os.cpu_count()
            # as a heuristic for the optimal pool size.
            connection_pool = _ConnectionPool(
                host=host,
                port=port,
                pool_size=os.cpu_count(),
                timeout=timeout,
            )
        except Exception as e:
            # A failed pool is a fatal error.
            raise Exception(
                f"Exception while initializing Connection pool.\nInner err. '{e}'"
            )

        try:
            executor_pool = _ExecutorPool(pool_size=os.cpu_count())
        except Exception as e:
            raise Exception(
                f"Exception while initializing Executor pool.\nInner err. '{e}'"
            )

        # Call the private constructor
        return cls(
            control_client, connection_pool, executor_pool, cls._CONNECT_SENTINEL
        )

    # --- Context Manager Protocol ---

    def __enter__(self) -> "MosaicoClient":
        """Context manager entry point."""
        return self

    def __exit__(
        self,
        exc_type: Optional[Type[BaseException]],
        exc_val: Optional[BaseException],
        exc_tb: Optional[Any],
    ) -> None:
        """
        Context manager exit point. Ensures resources are closed.

        Exceptions raised within the `with` block are propagated.
        """
        try:
            self.close()
        except Exception as e:
            logger.error(
                f"Error releasing resources allocated from MosaicoClient.\nInner err: '{e}'"
            )

    def __del__(self):
        """Destructor. Failsafe if close() is not explicitly called."""
        if self._status == _ConnectionStatus.Open:
            logger.warning(
                "MosaicoClient destroyed without calling close(). "
                "Resources may not have been released properly."
            )

    # --- Handler Factory Methods ---

    def sequence_handler(self, sequence_name: str) -> Optional[SequenceHandler]:
        """
        Retrieves a `SequenceHandler` for the given sequence.

        Handlers are cached; subsequent calls for the same sequence return the existing object.

        Args:
            sequence_name (str): The name of the sequence.

        Returns:
            SequenceHandler: A handler for managing sequence operations.
        """
        sh = self._sequence_handlers_cache.get(sequence_name)
        if sh is None:
            sh = SequenceHandler.connect(
                sequence_name=sequence_name,
                client=self._control_client,
            )
            if not sh:
                return None

            self._sequence_handlers_cache[sequence_name] = sh
        return sh

    def topic_handler(
        self,
        sequence_name: str,
        topic_name: str,
    ) -> Optional[TopicHandler]:
        """
        Retrieves a `TopicHandler`.

        Input must be the separate sequence and topic names.

        Args:
            sequence_name (str): The parent sequence name.
            topic_name (str): The topic name.

        Returns:
            TopicHandler: A handler for managing topic operations.

        """
        # normalize inputs to a unique resource string
        topic_resource_name = pack_topic_resource_name(sequence_name, topic_name)

        th = self._topic_handlers_cache.get(topic_resource_name)
        if th is None:
            th = TopicHandler.connect(
                sequence_name=sequence_name,
                topic_name=topic_name,
                client=self._control_client,
            )
            if not th:
                return None

            self._topic_handlers_cache[topic_resource_name] = th
        return th

    # --- Main API Methods ---

    def sequence_create(
        self,
        sequence_name: str,
        metadata: dict[str, Any],
        on_error: OnErrorPolicy = OnErrorPolicy.Delete,
        max_batch_size_bytes: Optional[int] = None,
        max_batch_size_records: Optional[int] = None,
    ) -> SequenceWriter:
        """
        Creates a `SequenceWriter` to upload a new sequence.

        The writer will utilize the client's connection and executor pools.

        Args:
            sequence_name (str): Unique name for the sequence.
            metadata (dict[str, Any]): User-defined metadata.
            on_error (OnErrorPolicy): Behavior on write failure (e.g., Delete partial data).
            max_batch_size_bytes (Optional[int]): Max bytes per Arrow batch.
            max_batch_size_records (Optional[int]): Max records per Arrow batch.

        Returns:
            SequenceWriter: An initialized writer instance.
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

        return SequenceWriter(
            sequence_name=sequence_name,
            client=self._control_client,
            connection_pool=self._connection_pool,
            executor_pool=self._executor_pool,
            metadata=metadata,
            config=WriterConfig(
                on_error=on_error,
                max_batch_size_bytes=max_batch_size_bytes,
                max_batch_size_records=max_batch_size_records,
            ),
        )

    def _remove_from_sequence_handlers_cache(self, sequence_name: str):
        # remove from cache
        del self._sequence_handlers_cache[sequence_name]

    def _remove_from_topic_handlers_cache(self, topic_resource_name: str):
        # remove from cache
        del self._topic_handlers_cache[topic_resource_name]

    def clear_sequence_handlers_cache(self):
        self._sequence_handlers_cache = {}

    def clear_topic_handlers_cache(self):
        self._topic_handlers_cache = {}

    def sequence_system_info(self, sequence_name: str) -> Optional[SystemInfo]:
        """
        Retrieves system-level metadata for a specific sequence.

        This method queries the server for the physical state of the sequence,
        including its total storage footprint and creation history.

        Args:
            sequence_name (str): The unique identifier of the sequence on the
                Mosaico platform.

        Returns:
            Optional[SystemInfo]: A SystemInfo object containing the sequence
                diagnostics if found, or None if the sequence does not exist
                on the host.
        """
        # Get System Info
        ACTION = FlightAction.SEQUENCE_SYSTEM_INFO
        try:
            act_resp = _do_action(
                client=self._control_client,
                action=ACTION,
                payload={"name": sequence_name},
                expected_type=_DoActionResponseSysInfo,
            )
        except Exception as e:
            logger.error(
                f"Error sending system-info do_action '{ACTION}'.\nInner err: '{e}'"
            )
            return None

        if act_resp is None:
            logger.error(f"Action '{ACTION}' returned no response.")
            return None

        return SystemInfo(
            total_size_bytes=act_resp.total_size_bytes,
            created_datetime=act_resp.created_datetime,
            is_locked=act_resp.is_locked,
        )

    def topic_system_info(
        self,
        sequence_name: str,
        topic_name: str,
    ) -> Optional[SystemInfo]:
        """
        Retrieves system-level metadata for a specific topic within a sequence.

        Useful for verifying the data integrity or storage consumption of
        individual streams (e.g., a specific LIDAR or Camera topic) without
        loading the entire sequence metadata.

        Args:
            sequence_name (str): The name of the sequence containing the topic.
            topic_name (str): The specific topic name to query (e.g., "/front_cam/image_raw").

        Returns:
            Optional[SystemInfo]: A SystemInfo object containing the topic
                diagnostics if found, or None if the sequence or topic
                cannot be located.
        """
        # Get System Info
        ACTION = FlightAction.TOPIC_SYSTEM_INFO
        try:
            act_resp = _do_action(
                client=self._control_client,
                action=ACTION,
                payload={
                    "name": pack_topic_resource_name(
                        sequence_name=sequence_name, topic_name=topic_name
                    )
                },
                expected_type=_DoActionResponseSysInfo,
            )
        except Exception as e:
            logger.error(
                f"Error sending system-info do_action '{ACTION}'.\nInner err: '{e}'"
            )
            return None

        if act_resp is None:
            logger.error(f"Action '{ACTION}' returned no response.")
            return None

        return SystemInfo(
            total_size_bytes=act_resp.total_size_bytes,
            created_datetime=act_resp.created_datetime,
            is_locked=act_resp.is_locked,
            chunks_number=act_resp.chunks_number,
        )

    def sequence_delete(self, sequence_name: str):
        """
        Deletes a layer definition from the server.

        Args:
            layer_name (str): The name of the layer to delete.
        """
        try:
            _do_action(
                client=self._control_client,
                action=FlightAction.SEQUENCE_DELETE,
                payload={"name": sequence_name},
                expected_type=None,
            )

            self._remove_from_sequence_handlers_cache(sequence_name=sequence_name)

        except Exception as e:
            logger.error(f"Server error while asking for Sequence deletion, '{e}'")

    def list_sequences(self) -> List[str]:
        """
        Retrieves the list of all sequences available on the server.

        Returns:
            List[str]: The list of sequence names.
        """
        out_list = []
        for finfo in self._control_client.list_flights():
            if finfo.descriptor.path is None:
                logger.debug("`None` path found in `list_flights` endpoint")
                continue
            out_list.extend([p.decode("utf-8") for p in finfo.descriptor.path])
        return out_list

    def query(
        self,
        *queries: QueryableProtocol,
        query: Optional[Query] = None,
    ) -> Optional[QueryResponse]:
        """
        Executes one or more queries against the Mosaico database.
        The provided queries are joined in AND condition.

        Args:
            *queries: Variable arguments of query builder objects.
            query (Optional[Query]): A pre-constructed Query object (alternative to *queries).

        Returns:
            Optional[QueryResponse]: The query result or None if any error.

        Raises:
            ValueError: If conflicting query types are passed or no queries are provided.
        """
        if queries:
            self._queries = list(queries)
            # Validate for duplicate query types to prevent overwrite logic errors
            types_seen = {}
            for q in queries:
                t = type(q)
                if t in types_seen:
                    raise ValueError(
                        f"Duplicate query type detected: '{t.__name__}'. "
                        "Multiple instances of the same type will override each other when encoded.",
                    )
                else:
                    types_seen[t] = True
        elif query is not None:
            self._queries = query._queries
        else:
            raise ValueError("Expected input queries or a 'Query' object")

        query_dict: dict[str, Any] = {q.name(): q.to_dict() for q in self._queries}

        ACTION = FlightAction.QUERY

        try:
            act_resp = _do_action(
                client=self._control_client,
                action=ACTION,
                payload=query_dict,
                expected_type=_DoActionQueryResponse,
            )

        except Exception as e:
            logger.error(f"Query returned an internal error: '{e}'")
            return None

        if act_resp is None:
            logger.error(f"Action '{ACTION}' returned no response.")
            return None

        return act_resp.query_response

    def close(self):
        """
        Gracefully shuts down the client.

        Closes all cached handlers, terminates the connection and executor pools,
        and closes the main control connection.
        """
        if self._status == _ConnectionStatus.Open:
            # Close cached handlers
            for seq_inst in self._sequence_handlers_cache.values():
                seq_inst.close()
            for top_inst in self._topic_handlers_cache.values():
                top_inst.close()

            self.clear_sequence_handlers_cache()
            self.clear_topic_handlers_cache()

            # Close pools
            if self._connection_pool:
                self._connection_pool.close()
            if self._executor_pool:
                self._executor_pool.close()

            # Close main connection
            self._control_client.close()

        self._status = _ConnectionStatus.Closed
