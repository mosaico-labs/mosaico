"""
Mosaico Client Entry Point.

This module provides the `MosaicoClient`, the primary interface for users to
interact with the Mosaico system. It manages the connection lifecycle,
and serves as a factory for creating resource handlers (sequences, topics)
and executing queries.
"""

import os
from typing import Any, Dict, List, Optional, Type, Union

import pyarrow.flight as fl

from mosaicolabs.comm.notifications import Notification
from mosaicolabs.models.query import Query, QueryResponse
from mosaicolabs.models.query.protocols import QueryableProtocol

from ..enum import (
    APIKeyPermissionEnum,
    FlightAction,
    OnErrorPolicy,
    SessionLevelErrorPolicy,
)
from ..handlers import SequenceHandler, SequenceWriter, TopicHandler
from ..handlers.config import SessionWriterConfig
from ..helpers import pack_topic_resource_name
from ..logging_config import get_logger
from ..platform.api_key import APIKeyStatus
from .connection import (
    DEFAULT_MAX_BATCH_BYTES,
    DEFAULT_MAX_BATCH_SIZE_RECORDS,
    _ConnectionStatus,
    _get_connection,
)
from .do_action import (
    _do_action,
    _DoActionNotificationList,
    _DoActionQueryResponse,
    _DoActionResponseAPIKeyCreate,
    _DoActionResponseAPIKeyStatus,
)
from .middlewares import MosaicoAuthMiddlewareFactory

# Set the hierarchical logger
logger = get_logger(__name__)


class MosaicoClient:
    """
    The gateway to the Mosaico Data Platform.

    This class centralizes connection management, and serves as a
    factory for specialized handlers.

    Tip: Context Manager Usage
        The `MosaicoClient` is best used as a context manager to ensure
        the connection is gracefully closed.

        ```python
        from mosaicolabs import MosaicoClient

        with MosaicoClient.connect(
            "localhost",
            6726,
            api_key="msco_s3l8gcdwuadege3pkhou0k0n2t5omfij_f9010b9e",
        ) as client:
            sequences = client.list_sequences()
            print(f"Available data: {sequences}")
        ```
    """

    # --- Private Sentinel Value ---
    # Used to ensure the constructor is only called via the `connect()` factory.
    _CONNECT_SENTINEL = object()
    _MOSAICO_TLS_CERT_ENV_VAR: str = "MOSAICO_TLS_CERT_FILE"
    _MOSAICO_AUTH_API_KEY: str = "MOSAICO_API_KEY"

    def __init__(
        self,
        *,
        host: str,
        port: int,
        timeout: int,
        control_client: fl.FlightClient,
        sentinel: object,
        enable_tls: bool,
        tls_cert: Optional[bytes],
        api_key_fingerprint: Optional[str],
        middlewares: dict[str, fl.ClientMiddlewareFactory],
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
            sentinel: Private object used to verify factory-based instantiation.
            enable_tls: Enable TLS communication.
            tls_cert: The TLS certificate.
            api_key_fingerprint: The fingerprint of the API key to use for authentication.
            middlewares: The middlewares to be used for the connection.
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
        self._tls_cert: Optional[bytes] = tls_cert
        """The path to the TLS certificate file."""
        self._enable_tls: bool = enable_tls
        """If True, enable the TLS commmunication protocol"""
        self._middlewares: dict[str, fl.ClientMiddlewareFactory] = middlewares
        """The middlewares to be used for the connection."""
        self._api_key_fingerprint: Optional[str] = api_key_fingerprint
        """The current optional API-Key fingerprint, used for access control"""

        # Initialize caches
        self._sequence_handlers_cache: Dict[str, SequenceHandler] = {}
        """Cache for SequenceHandler instances, keyed by sequence_name. Used to avoid re-connecting for known sequences."""
        self._topic_handlers_cache: Dict[str, TopicHandler] = {}
        """Cache for TopicHandler instances, keyed by their resource ('sequence_name/topic_name') name."""

    @classmethod
    def connect(
        cls,
        host: str,
        port: int,
        timeout: int = 5,
        enable_tls: bool = False,
        tls_cert_path: Optional[str] = None,
        api_key: Optional[str] = None,
    ) -> "MosaicoClient":
        """
        The primary entry point to the Mosaico Data Platform.

        This factory method is the **only recommended way** to obtain a valid
        `MosaicoClient` instance. It orchestrates the necessary handshake,
        and initializes the communication channel.

        Important: Factory Pattern
            Direct instantiation via `__init__` is restricted through a sentinel
            pattern and will raise a `RuntimeError`. This
            ensures that every client in use has been correctly configured with a
            valid network connection.

        Note:
            If using the Authorization middleware (via an API-Key), this method requires the minimum
            [`APIKeyPermissionEnum.Read`][mosaicolabs.enum.APIKeyPermissionEnum.Read]
            permission.

        Args:
            host (str): The server host address (e.g., "127.0.0.1" or "mosaico.local").
            port (int): The server port (e.g., 6726).
            timeout (int): Maximum time in seconds to wait for a connection response.
                Defaults to 5.
            enable_tls (bool): Enable the TLS standard one-way TLS (server authenticated only) communication protocol.
                Defaults to False. If `tls_cert_path` is provided (not None), this flag does not have any effect.
            tls_cert_path (Optional[str]): Path to the TLS certificate file. Defaults to None.
                If `tls_cert_path=None` and `enable_tls=True`, a standard one-way TLS (server authenticated only) connection
                is established.
            api_key (Optional[str]): The API key for authentication. Defaults to None.

        Returns:
            MosaicoClient: An initialized and connected client ready for operations.

        Raises:
            ConnectionError: If the server is unreachable or the handshake fails.
            ValueError: If the tls_cert_path is invalid or unable to read the certificate (if using TLS).
            FileNotFoundError: If the tls_cert_path does not exist (if using TLS).

        Example:
            ```python
            from mosaicolabs import MosaicoClient

            # Establish a connection to the Mosaico Data Platform via One-way TLS and an API-KEY
            with MosaicoClient.connect(
                "localhost",
                6726,
                enable_tls=True,
                api_key="msco_vy9lqa7u4lr7w3vimhz5t8bvvc0xbmk2_9c94a86",
            ) as client:
                # Perform operations using the client
                pass
            ```
        """

        # Establish the Control Connection
        logger.debug(f"Opening a connection '{host}:{port}'")

        resolved_tls_cert = cls._resolve_tls_cert_path(tls_cert_path)

        enable_tls = enable_tls or tls_cert_path is not None

        middlewares = {}
        api_key_fingerprint = None
        if api_key:
            auth_mware = MosaicoAuthMiddlewareFactory(api_key=api_key)
            middlewares["mosaico_auth"] = auth_mware
            api_key_fingerprint = auth_mware.api_key_fingerprint

        try:
            control_client: fl.FlightClient = _get_connection(
                host=host,
                port=port,
                timeout=timeout,
                enable_tls=enable_tls,
                tls_cert=resolved_tls_cert,
                middlewares=middlewares,
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
            sentinel=cls._CONNECT_SENTINEL,
            tls_cert=resolved_tls_cert,
            enable_tls=enable_tls,
            api_key_fingerprint=api_key_fingerprint,
            middlewares=middlewares,
        )

    @classmethod
    def from_env(
        cls,
        host: str,
        port: int,
        timeout: int = 5,
    ) -> "MosaicoClient":
        """
        Creates a MosaicoClient instance by resolving configuration from environment variables.

        This method acts as a smart constructor that automatically discovers system
        settings. It currently focuses on security configurations, specifically
        resolving TLS settings and Auth API-Key if the required environment variables are present.

        As the SDK evolves, this method will be expanded to automatically detect
        additional parameters from the environment.

        Args:
            host (str): The server hostname or IP address.
            port (int): The port number of the Mosaico service.
            timeout (int): Maximum time in seconds to wait for a connection response.
                Defaults to 5.

        Returns:
            MosaicoClient: A client instance pre-configured with discovered settings.
            If no specific environment variables are found, it returns a
            client with default settings.

        Example:
            ``` python
                # If MOSAICOD_TLS_CERT_FILE is set in the shell:
                client = MosaicoClient.from_env("localhost", 6276)
            ```
        """

        tls_cert = os.environ.get(MosaicoClient._MOSAICO_TLS_CERT_ENV_VAR)

        return cls.connect(
            host=host,
            port=port,
            timeout=timeout,
            enable_tls=tls_cert is not None,
            tls_cert_path=tls_cert,
            api_key=os.environ.get(MosaicoClient._MOSAICO_AUTH_API_KEY),
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
        self._sequence_handlers_cache.pop(sequence_name, None)

    def _remove_from_topic_handlers_cache(self, topic_resource_name: str):
        # remove from cache
        self._topic_handlers_cache.pop(topic_resource_name, None)

    @staticmethod
    def _resolve_tls_cert_path(tls_cert_path: Optional[str]) -> Optional[bytes]:
        """
        Resolves the TLS certificate path.

        Args:
            tls_cert_path (Optional[str]): Path to the TLS certificate file.

        Returns:
            Optional[bytes]: The contents of the TLS certificate file, or None if the path is None.
        """

        logger.debug(f"Resolving Mosaico tls certificate path {tls_cert_path}")

        if tls_cert_path is None:
            return None

        if not tls_cert_path:
            raise ValueError("tls_cert_path cannot be empty")

        try:
            with open(tls_cert_path, "rb") as cert_file:
                return cert_file.read()
        except FileNotFoundError:
            raise FileNotFoundError(
                f"tls certificate path '{tls_cert_path}' does not exist"
            )
        except OSError as e:
            raise ValueError(
                f"Failed to read tls certificate from {tls_cert_path}: {e}"
            )

    # --- Handler Factory Methods ---

    def sequence_handler(self, sequence_name: str) -> Optional[SequenceHandler]:
        """
        Retrieves a [`SequenceHandler`][mosaicolabs.handlers.SequenceHandler] for the given sequence.

        Handlers are cached; subsequent calls for the same sequence return the existing
        object to avoid redundant handshakes.

        Note:
            If using the Authorization middleware (via an API-Key), this method requires the minimum
            [`APIKeyPermissionEnum.Read`][mosaicolabs.enum.APIKeyPermissionEnum.Read]
            permission.

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

        Note:
            If using the Authorization middleware (via an API-Key), this method requires the minimum
            [`APIKeyPermissionEnum.Read`][mosaicolabs.enum.APIKeyPermissionEnum.Read]
            permission.

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
        on_error: Union[
            SessionLevelErrorPolicy, OnErrorPolicy
        ] = SessionLevelErrorPolicy.Report,
        max_batch_size_bytes: Optional[int] = None,
        max_batch_size_records: Optional[int] = None,
    ) -> SequenceWriter:
        """
        Creates a new sequence on the platform and returns a [`SequenceWriter`][mosaicolabs.handlers.SequenceWriter] for ingestion.

        Important:
            The function **must** be called inside a with context, otherwise a
            RuntimeError is raised.

        Note:
            If using the Authorization middleware (via an API-Key), this method requires at least
            [`APIKeyPermissionEnum.Write`][mosaicolabs.enum.APIKeyPermissionEnum.Write]
            permission.

        Args:
            sequence_name (str): Unique name for the sequence.
            metadata (dict[str, Any]): User-defined metadata to attach.
            on_error (SessionLevelErrorPolicy | OnErrorPolicy): Behavior on write failure. Defaults to
                [`SessionLevelErrorPolicy.Report`][mosaicolabs.enum.SessionLevelErrorPolicy.Report].

                Deprecated:
                    [`OnErrorPolicy`][mosaicolabs.enum.OnErrorPolicy] is deprecated since v0.3.0; use
                    [`SessionLevelErrorPolicy`][mosaicolabs.enum.SessionLevelErrorPolicy] instead.
                    It will be removed in v0.4.0.

            max_batch_size_bytes (Optional[int]): Max bytes per Arrow batch.
            max_batch_size_records (Optional[int]): Max records per Arrow batch.

        Returns:
            SequenceWriter: An initialized writer instance.

        Raises:
            RuntimeError: If the method is called outside a `with` context.
            Exception: If any error occurs during sequence injection.

        Example:
            ```python
            from mosaicolabs import MosaicoClient, SessionLevelErrorPolicy

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
                    on_error = SessionLevelErrorPolicy.Delete
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

        # Safely convert to the right type
        on_error = SessionLevelErrorPolicy(on_error.value)

        return SequenceWriter(
            sequence_name=sequence_name,
            client=self._control_client,
            metadata=metadata,
            config=SessionWriterConfig(
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

        Note:
            If using the Authorization middleware (via an API-Key), this method requires at least
            [`APIKeyPermissionEnum.Delete`][mosaicolabs.enum.APIKeyPermissionEnum.Delete]
            permission.

        Args:
            sequence_name (str): The unique name of the sequence to remove.

        Raises:
            Exception: If any error occurs during sequence deletion.

        """
        try:
            _do_action(
                client=self._control_client,
                action=FlightAction.SEQUENCE_DELETE,
                payload={"locator": sequence_name},
                expected_type=None,
            )

            self._remove_from_sequence_handlers_cache(sequence_name=sequence_name)

        except Exception as e:
            logger.error(
                f"Server error (do_action) while asking for Sequence deletion, '{e}'"
            )
            raise

    def session_delete(self, session_uuid: str):
        """
        Permanently deletes a session and all its associated data from the server.

        This operation is destructive and triggers a cascading deletion of all underlying
        resources, including all topics and data chunks stored in the session.
        Once executed, all storage occupied by the session is freed.

        Note:
            If using the Authorization middleware (via an API-Key), this method requires at least
            [`APIKeyPermissionEnum.Delete`][mosaicolabs.enum.APIKeyPermissionEnum.Delete]
            permission.

        Args:
            session_uuid (str): The unique identifier of the session to remove.

        Raises:
            Exception: If any error occurs during session deletion.
        """
        try:
            _do_action(
                client=self._control_client,
                action=FlightAction.SESSION_DELETE,
                payload={"session_uuid": session_uuid},
                expected_type=None,
            )

        except Exception as e:
            logger.error(
                f"Server error (do_action) while asking for Session '{session_uuid}' deletion, '{e}'"
            )
            raise

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

    def list_sequence_notifications(self, sequence_name: str) -> List[Notification]:
        """
        Retrieves a list of all notifications available on the server for a specific sequence.

        Note:
            If using the Authorization middleware (via an API-Key), this method requires the minimum
            [`APIKeyPermissionEnum.Read`][mosaicolabs.enum.APIKeyPermissionEnum.Read]
            permission.

        Args:
            sequence_name (str): The name of the sequence to list notifications for.

        Returns:
            List[Notification]: The list of sequence notifications.

        Raises:
            Exception: If any error occurs during sequence notification listing.

        Example:
            ```python
            from mosaicolabs import MosaicoClient

            with MosaicoClient.connect("localhost", 6726) as client:
                sequence_notifications = client.list_sequence_notifications("my_sequence")
                for notification in sequence_notifications:
                    print(f"Notification Type: {notification.type}")
                    print(f"Notification Message: {notification.message}")
                    print(f"Notification Created: {notification.created_datetime}")
            ```
        """
        ACTION = FlightAction.SEQUENCE_NOTIFICATION_LIST

        try:
            act_resp = _do_action(
                client=self._control_client,
                action=ACTION,
                payload={"locator": sequence_name},
                expected_type=_DoActionNotificationList,
            )

            if act_resp is None:
                logger.error(f"Action '{ACTION}' returned no response.")
                return []

            return act_resp.notifications

        except Exception as e:
            logger.error(f"Query returned an internal error: '{e}'")
            raise

    def clear_sequence_notifications(self, sequence_name: str):
        """
        Clears the notifications for a specific sequence from the server.

        Note:
            If using the Authorization middleware (via an API-Key), this method requires at least
            [`APIKeyPermissionEnum.Delete`][mosaicolabs.enum.APIKeyPermissionEnum.Delete]
            permission.

        Args:
            sequence_name (str): The name of the sequence.

        Raises:
            Exception: If any error occurs during sequence notification clearing.
        """
        ACTION = FlightAction.SEQUENCE_NOTIFICATION_PURGE

        try:
            _do_action(
                client=self._control_client,
                action=ACTION,
                payload={"locator": sequence_name},
                expected_type=None,
            )

        except Exception as e:
            logger.error(f"Query returned an internal error: '{e}'")
            raise

    def list_topic_notifications(
        self, sequence_name: str, topic_name: str
    ) -> List[Notification]:
        """
        Retrieves a list of all notifications available on the server for a specific topic

        Note:
            If using the Authorization middleware (via an API-Key), this method requires the minimum
            [`APIKeyPermissionEnum.Read`][mosaicolabs.enum.APIKeyPermissionEnum.Read]
            permission.

        Args:
            sequence_name (str): The name of the sequence to list notifications for.
            topic_name (str): The name of the topic to list notifications for.

        Returns:
            List[Notification]: The list of topic notifications.

        Raises:
            Exception: If any error occurs during topic notification listing.

        Example:
            ```python
            from mosaicolabs import MosaicoClient

            with MosaicoClient.connect("localhost", 6726) as client:
                topic_notifications = client.list_topic_notifications("my_sequence", "my_topic")
                for notification in topic_notifications:
                    print(f"Notification Type: {notification.type}")
                    print(f"Notification Message: {notification.message}")
                    print(f"Notification Created: {notification.created_datetime}")
            ```
        """
        ACTION = FlightAction.TOPIC_NOTIFICATION_LIST

        try:
            act_resp = _do_action(
                client=self._control_client,
                action=ACTION,
                payload={
                    "locator": pack_topic_resource_name(
                        sequence_name=sequence_name,
                        topic_name=topic_name,
                    )
                },
                expected_type=_DoActionNotificationList,
            )

            if act_resp is None:
                logger.error(f"Action '{ACTION}' returned no response.")
                return []

            return act_resp.notifications

        except Exception as e:
            logger.error(f"Query returned an internal error: '{e}'")
            raise

    def clear_topic_notifications(self, sequence_name: str, topic_name: str):
        """
        Clears the notifications for a specific topic from the server.

        Note:
            If using the Authorization middleware (via an API-Key), this method requires at least
            [`APIKeyPermissionEnum.Delete`][mosaicolabs.enum.APIKeyPermissionEnum.Delete]
            permission.

        Args:
            sequence_name (str): The name of the sequence.
            topic_name (str): The name of the topic.

        Raises:
            Exception: If any error occurs during topic notification clearing.
        """
        ACTION = FlightAction.TOPIC_NOTIFICATION_PURGE

        try:
            _do_action(
                client=self._control_client,
                action=ACTION,
                payload={
                    "locator": pack_topic_resource_name(
                        sequence_name=sequence_name,
                        topic_name=topic_name,
                    )
                },
                expected_type=None,
            )

        except Exception as e:
            logger.error(f"Query returned an internal error: '{e}'")
            raise

    def query(
        self,
        *queries: QueryableProtocol,
        query: Optional[Query] = None,
    ) -> Optional[QueryResponse]:
        """
        Executes one or more queries against the Mosaico database.

        Multiple provided queries are joined using a logical **AND** condition.

        Note:
            If using the Authorization middleware (via an API-Key), this method requires the minimum
            [`APIKeyPermissionEnum.Read`][mosaicolabs.enum.APIKeyPermissionEnum.Read]
            permission.

        Args:
            *queries: Variable arguments of query builder objects (e.g., `QuerySequence`).
            query (Optional[Query]): An alternative pre-constructed Query object.

        Returns:
            Optional[QueryResponse]: The query results, or None if an error occurs.

        Raises:
            ValueError: If conflicting query types are passed or no queries are provided.
            Exception: If any error occurs during query execution.

        Example: Query with variadic arguments
            ```python
            from mosaicolabs import QueryOntologyCatalog, QuerySequence, Query, IMU, MosaicoClient

            # Establish a connection to the Mosaico Data Platform
            with MosaicoClient.connect("localhost", 6726) as client:
                # Perform the server side query
                results = client.query(
                    # Append a filter for sequence metadata
                    QuerySequence()
                    .with_user_metadata("environment.visibility", lt=50)
                    .with_name_match("test_drive"),
                    # Append a filter with deep time-series data discovery and measurement time windowing
                    QueryOntologyCatalog()
                    .with_expression(IMU.Q.acceleration.x.gt(5.0))
                    .with_expression(IMU.Q.timestamp_ns.gt(1700134567))
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
                    .with_user_metadata("environment.visibility", lt=50)
                    .with_name_match("test_drive"),
                    # Append a filter with deep time-series data discovery and measurement time windowing
                    QueryOntologyCatalog()
                    .with_expression(IMU.Q.acceleration.x.gt(5.0))
                    .with_expression(IMU.Q.timestamp_ns.gt(1700134567))
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
            raise

    def version(self) -> str:
        """
        Get the version of the Mosaico server.

        Note:
            If using the Authorization middleware (via an API-Key), this method requires the minimum
            [`APIKeyPermissionEnum.Read`][mosaicolabs.enum.APIKeyPermissionEnum.Read]
            permission.

        Returns:
            str: The version of the Mosaico server.

        Raises:
            Exception: If any error occurs during version retrieval.
        """
        ACTION = FlightAction.VERSION
        try:
            act_resp = _do_action(
                client=self._control_client,
                action=ACTION,
                payload={},
                expected_type=None,
            )

            if act_resp is None:
                logger.error(f"Action '{ACTION}' returned no response.")
                return ""

            return act_resp

        except Exception as e:
            logger.error(f"'Version' action returned an internal error: '{e}'")
            raise

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

    def api_key_create(
        self,
        permission: APIKeyPermissionEnum,
        description: str,
        expires_at_ns: Optional[int] = None,
    ) -> Optional[str]:
        """
        Creates a new API key with the specified permissions.

        Note:
            Requires the client to have [`APIKeyPermissionEnum.Manage`][mosaicolabs.enum.APIKeyPermissionEnum.Manage]
            permission. You can also optionally set an expiration time and a description for the key.

        Args:
            permission (APIKeyPermissionEnum): Permission for the key.
            description (str): Description for the key.
            expires_at_ns (Optional[int]): Optional expiration timestamp in nanoseconds.

        Returns:
            str: The generated API key token or None.

        Raises:
            Exception: If any error occurs during API key creation.

        Example:
            ```python
            from mosaicolabs import MosaicoClient, APIKeyPermissionEnum

            # Open the connection with the Mosaico Client
            with MosaicoClient.connect("localhost", 6726, api_key="<API_KEY_MANAGE>") as client:
                # Create a new API key with read and write permissions
                api_key = client.api_key_create(
                    permission=APIKeyPermissionEnum.Write,
                    description="API key for data ingestion",
                )
            ```
        """
        payload: dict[str, Any] = {
            "permissions": permission.value,
            "description": description,
        }
        if expires_at_ns is not None:
            payload["expires_at_ns"] = expires_at_ns

        ACTION = FlightAction.API_KEY_CREATE

        try:
            act_resp = _do_action(
                client=self._control_client,
                action=ACTION,
                payload=payload,
                expected_type=_DoActionResponseAPIKeyCreate,
            )

            if act_resp is None:
                logger.error(f"Action '{ACTION}' returned no response.")
                return None

            return act_resp.api_key_token

        except Exception as e:
            logger.error(f"API key creation failed with error: '{e}'")
            raise

    def api_key_status(
        self, api_key_fingerprint: Optional[str] = None
    ) -> Optional[APIKeyStatus]:
        """
        Retrieves the status and metadata of an API key.

        Note:
            Requires the client to have [`APIKeyPermissionEnum.Manage`][mosaicolabs.enum.APIKeyPermissionEnum.Manage]
            permission.

        Args:
            api_key_fingerprint (Optional[str]): The fingerprint of the API key to query.
                If not provided, the fingerprint of the current API key will be used.

        Returns:
            APIKeyStatus: An object containing the API key's status information, or None if the query fails.

        Raises:
            Exception: If any error occurs during API key status retrieval.
        """
        api_key_fingerprint = api_key_fingerprint or self._api_key_fingerprint
        if not api_key_fingerprint:
            logger.error(
                "API key fingerprint is required. Provide it as an argument or connect with an API key."
            )
            return None

        ACTION = FlightAction.API_KEY_STATUS

        try:
            act_resp = _do_action(
                client=self._control_client,
                action=ACTION,
                payload={"api_key_fingerprint": api_key_fingerprint},
                expected_type=_DoActionResponseAPIKeyStatus,
            )

            if act_resp is None:
                logger.error(f"Action '{ACTION}' returned no response.")
                return None

            return APIKeyStatus(
                created_at_ns=act_resp.created_at_ns,
                expires_at_ns=act_resp.expires_at_ns,
                description=act_resp.description,
            )

        except Exception as e:
            logger.error(f"API key status query failed with error: '{e}'")
            raise

    def api_key_revoke(self, api_key_fingerprint: str) -> None:
        """
        Revokes an API key by its fingerprint.

        Note:
            Requires the client to have [`APIKeyPermissionEnum.Manage`][mosaicolabs.enum.APIKeyPermissionEnum.Manage]
            permission.

        Args:
            api_key_fingerprint (str): The fingerprint of the API key to revoke.

        Returns:
            None.

        Raises:
            Exception: If any error occurs during API key revocation.
        """
        if not api_key_fingerprint:
            logger.error("api_key_fingerprint cannot be empty.")
            return None

        ACTION = FlightAction.API_KEY_REVOKE

        try:
            _do_action(
                client=self._control_client,
                action=ACTION,
                payload={"api_key_fingerprint": api_key_fingerprint},
                expected_type=None,
            )

            return None

        except Exception as e:
            logger.error(f"API key revoke failed with error: '{e}'")
            raise

    def close(self):
        """
        Gracefully shuts down the Mosaico client and releases all underlying resources.

        This method ensures a clean termination of the client's lifecycle by:

        * **Closing Handlers:** Invalidates and closes all cached `SequenceHandlers` and `TopicHandlers` to prevent stale data access.
        * **Network Cleanup:** Closes the connection to the `mosaicod` backend.

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

            # Close main connection
            self._control_client.close()

        self._status = _ConnectionStatus.Closed
