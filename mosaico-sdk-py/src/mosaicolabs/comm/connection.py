"""
Connection Management Module.

This module handles the creation and management of PyArrow Flight network connections.
It implements a pooling mechanism (`_ConnectionPool`) to allow parallel data writing,
preventing bottlenecking on a single TCP/gRPC socket during high-throughput operations.
"""

import pyarrow.flight as fl
from enum import Enum
from typing import List, Optional
from itertools import cycle

from ..logging import get_logger

# Constants defining batch size limits for Flight transmission
PYARROW_OUT_OF_RANGE_BYTES = 16 * 1024 * 1024  # 4 MB
DEFAULT_MAX_BATCH_BYTES = 10 * 1024 * 1024  # 3 MB
DEFAULT_MAX_BATCH_SIZE_RECORDS = 5_000

_DEFAULT_CONNECTION_POOL_SIZE = 2

# Set the hierarchical logger
logger = get_logger(__name__)


class _ConnectionStatus(Enum):
    """Enumeration representing the lifecycle state of a connection object."""

    Open = "open"
    Closed = "closed"


def _get_connection(host: str, port: int, timeout: int) -> fl.FlightClient:
    """
    Factory function to establish a single PyArrow Flight client connection.

    Args:
        host (str): The hostname or IP address of the server.
        port (int): The port number to connect to.
        timeout (int): The waiting-for-connection timeout in seconds (default = 2s)

    Returns:
        fl.FlightClient: An active Flight client instance connected to the specified address.
    """
    client = fl.FlightClient(f"grpc://{host}:{port}")
    client.wait_for_available(timeout=timeout)
    return client


class _ConnectionPool:
    """
    Manages a pool of PyArrow Flight connections.

    This class maintains a list of active `FlightClient` instances and uses a
    Round-Robin strategy to cycle through them. This allows the client to
    distribute write operations across multiple sockets, improving throughput
    by reducing contention.
    """

    def __init__(
        self,
        host: str,
        port: int,
        pool_size: Optional[int],
        timeout: int,
    ):
        """
        Initializes the connection pool.

        Args:
            host (str): The server hostname.
            port (int): The server port.
            pool_size (Optional[int]): The number of connections to maintain.
                                       If None, defaults to `_DEFAULT_CONNECTION_POOL_SIZE`.
        """
        self._host = host
        self._port = port
        self._size = pool_size or _DEFAULT_CONNECTION_POOL_SIZE
        self._clients: List[fl.FlightClient] = []
        self._iterator = None

        self._initialize_pool(timeout)

    def _initialize_pool(self, timeout: int):
        """
        Creates the connections and sets up the cyclic iterator.

        This method attempts to fill the pool. If an error occurs during creation,
        it cleans up any successfully created connections before raising the error.

        Raises:
            ValueError: If `pool_size` is less than 1.
            Exception: If a connection to the server cannot be established.
        """
        if self._size < 1:
            raise ValueError("Connection pool size must be at least 1")

        logger.debug(f"Initializing connection pool with {self._size} connections...")

        for i in range(self._size):
            try:
                # distinct connection instance
                self._clients.append(
                    _get_connection(host=self._host, port=self._port, timeout=timeout)
                )
            except Exception as e:
                logger.error(
                    f"Failed to create connection {i + 1}/{self._size} for pool: '{e}'"
                )
                # Clean up any connections successfully created before the failure
                # to prevent resource leaks (dangling sockets).
                self.close()
                raise e

        # Create an infinite cyclic iterator for round-robin assignment
        # e.g., returns client[0], then client[1], then client[0]...
        self._iterator = cycle(self._clients)

    def get_next(self) -> fl.FlightClient:
        """
        Retrieves the next available connection from the pool.

        Note: In CPython, `next()` on a built-in iterator is atomic due to the
        GIL, making this method thread-safe without additional locking.

        Returns:
            fl.FlightClient: The next Flight client in the rotation.

        Raises:
            RuntimeError: If the pool has not been initialized or has been closed.
        """
        if not self._clients or self._iterator is None:
            raise RuntimeError("Connection pool is not initialized or has been closed.")
        return next(self._iterator)

    def close(self):
        """
        Closes all connections in the pool and resets the state.

        This method attempts to close every client in the list, logging warnings
        if individual closures fail, rather than aborting the process.
        """
        for i, client in enumerate(self._clients):
            try:
                client.close()
            except Exception as e:
                logger.warning(f"Error closing pooled client #{i}: '{e}'")
        self._clients.clear()
        self._iterator = None
