"""
Connection Management Module.

This module handles the creation and management of PyArrow Flight network connections.
"""

from dataclasses import dataclass
from enum import Enum
from typing import Any, Optional

import pyarrow.flight as fl

from ..enum.grpc_compression import GRPCCompressionAlgorithm, GRPCCompressionLevel
from ..logging_config import get_logger

# Constants defining batch size limits for Flight transmission
PYARROW_OUT_OF_RANGE_BYTES = 16 * 1024 * 1024  # 4 MB
DEFAULT_MAX_BATCH_BYTES = 10 * 1024 * 1024  # 3 MB
DEFAULT_MAX_BATCH_SIZE_RECORDS = 5_000

# Set the hierarchical logger
logger = get_logger(__name__)


class _ConnectionStatus(Enum):
    """Enumeration representing the lifecycle state of a connection object."""

    Open = "open"
    Closed = "closed"


@dataclass
class GRPCCompression:
    """
    Represents the gRPC compression configuration.

    Args:
        algorithm (GRPCCompressionAlgorithm): The compression algorithm to use
        level (Optional[GRPCCompressionLevel]): The compression level to use

    Example:
        ```python
        from mosaicolabs import GRPCCompression, GRPCCompressionAlgorithm, GRPCCompressionLevel

        compression = GRPCCompression(
            algorithm=GRPCCompressionAlgorithm.Gzip,
            level=GRPCCompressionLevel.High,
        )
        ```

    """

    algorithm: GRPCCompressionAlgorithm
    """The compression algorithm to use"""
    level: Optional[GRPCCompressionLevel] = None
    """The compression level to use"""


def _get_connection(
    host: str,
    port: int,
    timeout: int,
    enable_tls: bool,
    compression: GRPCCompression,
    tls_cert: Optional[bytes],
    middlewares: Optional[dict[str, fl.ClientMiddlewareFactory]],
) -> fl.FlightClient:
    """
    Factory function to establish a single PyArrow Flight client connection.

    Args:
        host (str): The hostname or IP address of the server.
        port (int): The port number to connect to.
        timeout (int): The waiting-for-connection timeout in seconds (default = 2s)
        enable_tls (bool): Enable TLS communication.
        compression (GRPCCompression): The gRPC compression configuration.
        tls_cert (Optional[bytes]): The contents of the TLS certificate file.
        middleware (Optional[dict[str, fl.ClientMiddlewareFactory]]): The middlewares to be used for the connection.

    Returns:
        fl.FlightClient: An active Flight client instance connected to the specified address.
    """

    protocol = "grpc+tls" if enable_tls else "grpc"
    kwargs: dict[str, Any] = (
        {"tls_root_certs": tls_cert} if tls_cert is not None else {}
    )
    if middlewares is not None:
        kwargs.update({"middleware": [midwr for midwr in middlewares.values()]})

    if compression.algorithm != GRPCCompressionAlgorithm.Null:
        opts = [
            ("grpc.default_compression_algorithm", compression.algorithm.value),
            ("grpc.compression_enabled", 1),
        ]
        if compression.level is not None:
            opts += [("grpc.default_compression_level", compression.level.value)]
        kwargs.update({"generic_options": opts})

    try:
        client = fl.FlightClient(f"{protocol}://{host}:{port}", **kwargs)
    except fl.FlightUnavailableError as e:
        raise ConnectionError(f"Failed to connect to {host}:{port}") from e
    except fl.FlightInternalError as e:
        if "cert" in str(e).lower() or "ssl" in str(e).lower():
            raise ConnectionError(
                f"Error to validate certificate for {host}:{port}"
            ) from e
        raise ConnectionError(f"Error to connect to {host}:{port}") from e

    client.wait_for_available(timeout=timeout)
    return client
