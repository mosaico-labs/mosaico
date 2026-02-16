"""
Mosaico Client Entry Point.

This module provides the `MosaicoClient`, the primary interface for users to
interact with the Mosaico system. It manages the connection lifecycle,
resource pooling (connections and executors), and serves as a factory for
creating resource handlers (sequences, topics) and executing queries.
"""

import os
from typing import Any, Dict, List, Optional, Type
from mosaicolabs.comm.notifications import Notified
import pyarrow.flight as fl

from mosaicolabs.models.query import Query, QueryResponse
from mosaicolabs.models.query.protocols import QueryableProtocol

from ..helpers import pack_topic_resource_name
from ..handlers import TopicHandler, SequenceHandler, SequenceWriter
from .connection import _get_connection, _ConnectionStatus, _ConnectionPool
from .executor_pool import _ExecutorPool
from .do_action import (
    _do_action,
    _DoActionQueryResponse,
    _DoActionNotifyList,
)
from ..logging_config import get_logger
from ..enum import FlightAction, OnErrorPolicy
from ..handlers.config import WriterConfig
from .connection import (
    DEFAULT_MAX_BATCH_BYTES,
    DEFAULT_MAX_BATCH_SIZE_RECORDS,
)

# Set the hierarchical logger
logger = get_logger(__name__)


class MosaicoClient:
    """
    The gateway to the Mosaico Data Platform.

    This class centralizes connection management, resource pooling, and serves as a
    factory for specialized handlers. It is designed to manage the lifecycle of
    both network connections and asynchronous executors efficiently.

    Tip: Context Manager Usage
        The `MosaicoClient` is best used as a context manager to ensure all
        internal pools and connections are gracefully closed.

        ```python
        from mosaicolabs import MosaicoClient

        with MosaicoClient.connect("localhost", 6726) as client:
            sequences = client.list_sequences()
            print(f"Available data: {sequences}")
        ```
    """

    # --- Private Sentinel Value ---
    # Used to ensure the constructor is only called via the `connect()` factory.
    _CONNECT_SENTINEL = object()

    def __init__(
        self,
        *,
        host: str,
        port: int,
        timeout: int,
        control_client: fl.FlightClient,
        connection_pool: Optional[_ConnectionPool],
        executor_pool: Optional[_ExecutorPool],
        sentinel: object,
    ):
        """
        **Internal Constructor** (do not call this directly): The `MosaicoClient` enforces a strict
        factory pattern for security and proper resource setup.
        Please use the [`connect()`][mosaicolabs.comm.MosaicoClient.connect] method
        instead to obtain an initialized client.

        Important: Sentinel Enforcement
            This constructor checks for a private internal sentinel.
            Attempting to instantiate this class manually will result in a
            `RuntimeError`.

        Args:
            host: The remote server host.
            port: The remote server port.
            timeout: The connection timeout.
            control_client: The primary PyArrow Flight control client.
            connection_pool: Internal pool for data connections.
            executor_pool: Internal pool for async I/O.
            sentinel: Private object used to verify factory-based instantiation.
        """
        if sentinel is not MosaicoClient._CONNECT_SENTINEL:
            raise RuntimeError(
                "MosaicoClient must be instantiated using the classmethod MosaicoClient.connect()."
            )

        self._host = host
        """The remote server host"""
        self._port = port
        """The remote server port"""
        self._timeout = timeout
        """The connection timeout"""
        self._control_client: fl.FlightClient = control_client
        """The primary PyArrow Flight client used for SDK-Server control operations (e.g., creating layers, querying)."""
        self._status: _ConnectionStatus = _ConnectionStatus.Open
        """Tracks the current connection status (Open/Closed)."""
        self._connection_pool: Optional[_ConnectionPool] = connection_pool
        """The pool of Flight clients used for parallel data writing."""
        self._executor_pool: Optional[_ExecutorPool] = executor_pool
        """The pool of thread executors used for offloading serialization and I/O."""

        # Initialize caches
        self._sequence_handlers_cache: Dict[str, SequenceHandler] = {}
        """Cache for SequenceHandler instances, keyed by sequence_name. Used to avoid re-connecting for known sequences."""
        self._topic_handlers_cache: Dict[str, TopicHandler] = {}
        """Cache for TopicHandler instances, keyed by their resource ('sequence_name/topic_name') name."""

    def _init_pools(self):
        """Initialize Connection and Executor pools"""
        try:
            # Attempt to create the connection pool. We use os.cpu_count()
            # as a heuristic for the optimal pool size.
            if self._connection_pool is None:
                self._connection_pool = _ConnectionPool(
                    host=self._host,
                    port=self._port,
                    pool_size=os.cpu_count(),
                    timeout=self._timeout,
                )
        except Exception as e:
            raise ConnectionError(
                f"Exception while initializing Connection pool.\nInner err. '{e}'"
            )

        try:
            # Attempt to create the executor pool.
            if self._executor_pool is None:
                self._executor_pool = _ExecutorPool(pool_size=os.cpu_count())
        except Exception as e:
            raise Exception(
                f"Exception while initializing Executor pool.\nInner err. '{e}'"
            )

    @classmethod
    def connect(
        cls,
        host: str,
        port: int,
        timeout: int = 5,
    ) -> "MosaicoClient":
        """
        The primary entry point to the Mosaico Data Platform.

        This factory method is the **only recommended way** to obtain a valid
        `MosaicoClient` instance. It orchestrates the
        necessary handshake, initializes the primary control channel, and prepares
        the internal resource pools.

        Important: Factory Pattern
            Direct instantiation via `__init__` is restricted through a sentinel
            pattern and will raise a `RuntimeError`. This
            ensures that every client in use has been correctly configured with a
            valid network connection.

        Args:
            host (str): The server host address (e.g., "127.0.0.1" or "mosaico.local").
            port (int): The server port (e.g., 6726).
            timeout (int): Maximum time in seconds to wait for a connection response.
                Defaults to 5.

        Returns:
            MosaicoClient: An initialized and connected client ready for operations.

        Raises:
            ConnectionError: If the server is unreachable or the handshake fails.
            RuntimeError: If the class is instantiated directly instead of using this method.

        Example:
            ```python
            from mosaicolabs import MosaicoClient

            # Establish a connection to the Mosaico Data Platform
            with MosaicoClient.connect("localhost", 6726) as client:
                # Perform operations using the client
                pass
            ```
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

        # Call the private constructor
        return cls(
            host=host,
            port=port,
            timeout=timeout,
            control_client=control_client,
            connection_pool=None,
            executor_pool=None,
            sentinel=cls._CONNECT_SENTINEL,
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

    def _remove_from_sequence_handlers_cache(self, sequence_name: str):
        # remove from cache
        del self._sequence_handlers_cache[sequence_name]

    def _remove_from_topic_handlers_cache(self, topic_resource_name: str):
        # remove from cache
        del self._topic_handlers_cache[topic_resource_name]

    # --- Handler Factory Methods ---

    def sequence_handler(self, sequence_name: str) -> Optional[SequenceHandler]:
        """
        Retrieves a [`SequenceHandler`][mosaicolabs.handlers.SequenceHandler] for the given sequence.

        Handlers are cached; subsequent calls for the same sequence return the existing
        object to avoid redundant handshakes.

        Args:
            sequence_name (str): The unique identifier of the sequence.

        Returns:
            Optional[SequenceHandler]: A handler for managing sequence operations,
                or None if not found.

        Example:
            ```python
            from mosaicolabs import MosaicoClient

            # Establish a connection to the Mosaico Data Platform
            with MosaicoClient.connect("localhost", 6726) as client:
                # Retrieve a sequence handler
                sequence_handler = client.sequence_handler("my_sequence")
                if sequence_handler:
                    # Print sequence details
                    print(f"Sequence: {sequence_handler.name}")
                    print(f"Created: {sequence_handler.created_datetime}")
                    print(f"Topic list: {sequence_handler.topics}")
                    print(f"User Metadata: {sequence_handler.user_metadata}")
                    print(f"Size (MB): {sequence_handler.total_size_bytes / 1024 / 1024}")
            ```
        """
        sh = self._sequence_handlers_cache.get(sequence_name)
        if sh is None:
            sh = SequenceHandler._connect(
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
        Retrieves a [`TopicHandler`][mosaicolabs.handlers.TopicHandler] for a specific data channel.

        Args:
            sequence_name (str): The parent sequence name.
            topic_name (str): The specific topic name.

        Returns:
            Optional[TopicHandler]: A handler for managing topic operations,
                or None if not found.

        Example:
            ```python
            from mosaicolabs import MosaicoClient

            # Establish a connection to the Mosaico Data Platform
            with MosaicoClient.connect("localhost", 6726) as client:
                # Retrieve a topic handler
                topic_handler = client.topic_handler("my_sequence", "/front/camera/image_raw)
                if topic_handler:
                    # Print topic details
                    print(f"Topic: {topic_handler.sequence_name}:{topic_handler.name}")
                    print(f"Ontology Tag: {topic_handler.ontology_tag}")
                    print(f"Created: {topic_handler.created_datetime}")
                    print(f"User Metadata: {topic_handler.user_metadata}")
                    print(f"Size (MB): {topic_handler.total_size_bytes / 1024 / 1024}")

            ```
        """
        # normalize inputs to a unique resource string
        topic_resource_name = pack_topic_resource_name(sequence_name, topic_name)

        th = self._topic_handlers_cache.get(topic_resource_name)
        if th is None:
            th = TopicHandler._connect(
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
        Creates a new sequence on the platform and returns a [`SequenceWriter`][mosaicolabs.handlers.SequenceWriter] for ingestion.

        Important:
            The function **must** be called inside a with context, otherwise a
            RuntimeError is raised.

        Args:
            sequence_name (str): Unique name for the sequence.
            metadata (dict[str, Any]): User-defined metadata to attach.
            on_error (OnErrorPolicy): Behavior on write failure. Defaults to `Delete`.
            max_batch_size_bytes (Optional[int]): Max bytes per Arrow batch.
            max_batch_size_records (Optional[int]): Max records per Arrow batch.

        Returns:
            SequenceWriter: An initialized writer instance.

        Raises:
            RuntimeError: If the method is called outside a `with` context.
            Exception: If any error occurs during sequence injection.

        Example:
            ```python
            from mosaicolabs import MosaicoClient, OnErrorPolicy

            # Open the connection with the Mosaico Client
            with MosaicoClient.connect("localhost", 6726) as client:
                # Start the Sequence Orchestrator
                with client.sequence_create(
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
                    on_error = OnErrorPolicy.Delete # Default
                    ) as seq_writer:
                        # Start creating topics and pushing data...
                        # (1)!
            ```

            1. See also:
                * [`SequenceWriter.topic_create()`][mosaicolabs.handlers.SequenceWriter.topic_create]
                * [`TopicWriter.push()`][mosaicolabs.handlers.TopicWriter.push]
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

        # Init connection and executor pools
        self._init_pools()

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

    def sequence_delete(self, sequence_name: str):
        """
        Permanently deletes a sequence and all its associated data from the server.

        This operation is destructive and triggers a cascading deletion of all underlying
        resources, including all topics and data chunks belonging to the sequence.
        Once executed, all storage occupied by the sequence is freed.

        Important: Sequence Locking
            This action can only be performed on **unlocked** sequences. If a sequence
            is currently locked (e.g., for archival or safety reasons), the deletion
            request will be rejected by the server.

        Args:
            sequence_name (str): The unique name of the sequence to remove.
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
            logger.error(
                f"Server error (do_action) while asking for Sequence deletion, '{e}'"
            )

    def list_sequences(self) -> List[str]:
        """
        Retrieves a list of all sequence names available on the server.

        Returns:
            List[str]: The list of sequence identifiers.

        Example:
            ```python
            from mosaicolabs import MosaicoClient

            with MosaicoClient.connect("localhost", 6726) as client:
                sequences = client.list_sequences()
                print(f"Available sequences: {sequences}")
            ```
        """
        out_list = []
        for finfo in self._control_client.list_flights():
            if finfo.descriptor.path is None:
                logger.debug("`None` path found in `list_flights` endpoint")
                continue
            out_list.extend([p.decode("utf-8") for p in finfo.descriptor.path])
        return out_list

    def list_sequence_notify(self, sequence_name: str) -> List[Notified]:
        """
        Retrieves a list of all notifications available on the server for a specific sequence.

        Args:
            sequence_name (str): The name of the sequence to list notifications for.

        Returns:
            List[Notified]: The list of sequence notifications.

        Example:
            ```python
            from mosaicolabs import MosaicoClient

            with MosaicoClient.connect("localhost", 6726) as client:
                sequence_notifications = client.list_sequence_notify("my_sequence")
                for notify in sequence_notifications:
                    print(f"Notification Type: {notify.notify_type}")
                    print(f"Notification Message: {notify.message}")
                    print(f"Notification Created: {notify.created_datetime}")
            ```
        """
        ACTION = FlightAction.SEQUENCE_NOTIFY_LIST

        try:
            act_resp = _do_action(
                client=self._control_client,
                action=ACTION,
                payload={"name": sequence_name},
                expected_type=_DoActionNotifyList,
            )

            if act_resp is None:
                logger.error(f"Action '{ACTION}' returned no response.")
                return []

            return act_resp.notifies

        except Exception as e:
            logger.error(f"Query returned an internal error: '{e}'")
            return []

    def clear_sequence_notify(self, sequence_name: str):
        """
        Clears the notifications for a specific sequence from the server.

        Args:
            sequence_name (str): The name of the sequence.
        """
        ACTION = FlightAction.SEQUENCE_NOTIFY_PURGE

        try:
            _do_action(
                client=self._control_client,
                action=ACTION,
                payload={"name": sequence_name},
                expected_type=None,
            )

        except Exception as e:
            logger.error(f"Query returned an internal error: '{e}'")
            return []

    def list_topic_notify(self, sequence_name: str, topic_name: str) -> List[Notified]:
        """
        Retrieves a list of all notifications available on the server for a specific topic

        Args:
            sequence_name (str): The name of the sequence to list notifications for.
            topic_name (str): The name of the topic to list notifications for.

        Returns:
            List[str]: The list of topic notifications.

        Example:
            ```python
            from mosaicolabs import MosaicoClient

            with MosaicoClient.connect("localhost", 6726) as client:
                topic_notifications = client.list_topic_notify("my_sequence", "my_topic")
                for notify in topic_notifications:
                    print(f"Notification Type: {notify.notify_type}")
                    print(f"Notification Message: {notify.message}")
                    print(f"Notification Created: {notify.created_datetime}")
            ```
        """
        ACTION = FlightAction.TOPIC_NOTIFY_LIST

        try:
            act_resp = _do_action(
                client=self._control_client,
                action=ACTION,
                payload={
                    "name": pack_topic_resource_name(
                        sequence_name=sequence_name,
                        topic_name=topic_name,
                    )
                },
                expected_type=_DoActionNotifyList,
            )

            if act_resp is None:
                logger.error(f"Action '{ACTION}' returned no response.")
                return []

            return act_resp.notifies

        except Exception as e:
            logger.error(f"Query returned an internal error: '{e}'")
            return []

    def clear_topic_notify(self, sequence_name: str, topic_name: str):
        """
        Clears the notifications for a specific topic from the server.

        Args:
            sequence_name (str): The name of the sequence.
            topic_name (str): The name of the topic.
        """
        ACTION = FlightAction.TOPIC_NOTIFY_PURGE

        try:
            _do_action(
                client=self._control_client,
                action=ACTION,
                payload={
                    "name": pack_topic_resource_name(
                        sequence_name=sequence_name,
                        topic_name=topic_name,
                    )
                },
                expected_type=None,
            )

        except Exception as e:
            logger.error(f"Query returned an internal error: '{e}'")
            return []

    def query(
        self,
        *queries: QueryableProtocol,
        query: Optional[Query] = None,
    ) -> Optional[QueryResponse]:
        """
        Executes one or more queries against the Mosaico database.

        Multiple provided queries are joined using a logical **AND** condition.

        Args:
            *queries: Variable arguments of query builder objects (e.g., `QuerySequence`).
            query (Optional[Query]): An alternative pre-constructed Query object.

        Returns:
            Optional[QueryResponse]: The query results, or None if an error occurs.

        Raises:
            ValueError: If conflicting query types are passed or no queries are provided.

        Example: Query with variadic arguments
            ```python
            from mosaicolabs import QueryOntologyCatalog, QuerySequence, Query, IMU, MosaicoClient

            # Establish a connection to the Mosaico Data Platform
            with MosaicoClient.connect("localhost", 6726) as client:
                # Perform the server side query
                results = client.query(
                    # Append a filter for sequence metadata
                    QuerySequence()
                    .with_expression(
                        # Use query proxy for generating a _QuerySequenceExpression
                        Sequence.Q.user_metadata["environment.visibility"].lt(50)
                    )
                    .with_name_match("test_drive"),
                    # Append a filter with deep time-series data discovery and measurement time windowing
                    QueryOntologyCatalog()
                    .with_expression(IMU.Q.acceleration.x.gt(5.0))
                    .with_expression(IMU.Q.header.stamp.sec.gt(1700134567))
                    .with_expression(IMU.Q.header.stamp.nanosec.between([123456, 789123])),
                )
                # Inspect the results
                if results is not None:
                    # Results are automatically grouped by Sequence for easier data management
                    for item in results:
                        print(f"Sequence: {item.sequence.name}")
            ```

        Example: Query with `Query` object
            ```python
            from mosaicolabs import QueryOntologyCatalog, QuerySequence, Query, IMU, MosaicoClient

            # Establish a connection to the Mosaico Data Platform
            with MosaicoClient.connect("localhost", 6726) as client:
                # Build a filter with name pattern and metadata-related expression
                query = Query(
                    # Append a filter for sequence metadata
                    QuerySequence()
                    .with_expression(
                        # Use query proxy for generating a _QuerySequenceExpression
                        Sequence.Q.user_metadata["environment.visibility"].lt(50)
                    )
                    .with_name_match("test_drive"),
                    # Append a filter with deep time-series data discovery and measurement time windowing
                    QueryOntologyCatalog()
                    .with_expression(IMU.Q.acceleration.x.gt(5.0))
                    .with_expression(IMU.Q.header.stamp.sec.gt(1700134567))
                    .with_expression(IMU.Q.header.stamp.nanosec.between([123456, 789123])),
                )
                # Perform the server side query
                results = client.query(query=query)
                # Inspect the results
                if results is not None:
                    # Results are automatically grouped by Sequence for easier data management
                    for item in results:
                        print(f"Sequence: {item.sequence.name}")
            ```
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

            if act_resp is None:
                logger.error(f"Action '{ACTION}' returned no response.")
                return None

            return act_resp.query_response

        except Exception as e:
            logger.error(f"Query returned an internal error: '{e}'")
            return None

    def clear_sequence_handlers_cache(self):
        """
        Clears the internal cache of [`SequenceHandler`][mosaicolabs.handlers.SequenceHandler] objects.
        """
        self._sequence_handlers_cache = {}

    def clear_topic_handlers_cache(self):
        """
        Clears the internal cache of [`TopicHandler`][mosaicolabs.handlers.TopicHandler] objects.
        """
        self._topic_handlers_cache = {}

    def close(self):
        """
        Gracefully shuts down the Mosaico client and releases all underlying resources.

        This method ensures a clean termination of the client's lifecycle by:
        * **Closing Handlers:** Invalidates and closes all cached `SequenceHandlers` and `TopicHandlers` to prevent stale data access.
        * **Network Cleanup:** Terminated the connection pool to the `mosaicod` backend.
        * **Thread Termination:** Shuts down the internal thread executor pool responsible for asynchronous data fetching and background streaming.

        Note:
            If using the client as a context manager (via `with MosaicoClient.connect(...)`),
            this method is invoked automatically on exit. Explicit calls are required
            only for manual lifecycle management.

        Example:
            ```python
            from mosaicolabs import MosaicoClient

            # Manual connection management
            client = MosaicoClient.connect("localhost", 6726)
            # High-performance streaming or ML extraction
            qresp = client.query(...)
            # Do something else...

            # Ensure resources are consistently freed.
            client.close()
            ```
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
