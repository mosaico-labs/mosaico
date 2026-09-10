"""
Connection Management Module.

This module handles the creation and management of PyArrow Flight network connections.
"""

import time
from dataclasses import dataclass
from enum import Enum
from typing import Any, Optional

import pyarrow.flight as fl

from ..enum.flight_action import FlightAction
from ..enum.grpc_compression import GRPCCompressionAlgorithm, GRPCCompressionLevel
from ..logging_config import get_logger
from ..platform.server_config import ServerInfo
from .do_action import _do_action, _DoActionInfoResponse

# Set the hierarchical logger
logger = get_logger(__name__)


def _wait_for_available(client: fl.FlightClient, timeout: int) -> ServerInfo:
    """
    Probe the server with a INFO action until it responds or the timeout expires.
    """
    deadline = time.monotonic() + timeout
    last_exc: Optional[Exception] = None
    while True:
        try:
            act_resp = _do_action(
                client=client,
                action=FlightAction.INFO,
                payload={},
                expected_type=_DoActionInfoResponse,
            )
            if act_resp is None:
                raise ConnectionError(
                    f"Action '{FlightAction.INFO}' returned no response."
                )
            return act_resp.info
        except Exception as e:
            last_exc = e
            if time.monotonic() >= deadline:
                raise ConnectionError(
                    f"Server did not become available within {timeout}s (\nLast error: {last_exc})"
                ) from last_exc
            time.sleep(0.025)


class _ConnectionStatus(Enum):
    """Enumeration representing the lifecycle state of a connection object."""

    Open = "open"
    Closed = "closed"


@dataclass(frozen=True)
class ConnectionContext:
    """
    Bundles a live Flight client together with the server configuration resolved
    at connection time (via the 'info' DoAction).

    Every internal factory that used to receive a bare `fl.FlightClient` now receives
    this object instead, so any class holding a connection for its lifetime (handlers,
    writers, readers) can derive its own defaults from the specific server it is talking
    to, and new server-driven settings can be threaded through without touching every
    call site again. Being per-connection (not global), it also keeps configuration
    correctly isolated when a single script talks to multiple Mosaico servers at once.

    Args:
        flight_client (fl.FlightClient): The active PyArrow Flight client.
        server_info (ServerInfo): The server metadata/config resolved via the 'info' DoAction.
    """

    flight_client: fl.FlightClient
    server_info: ServerInfo

    def default_max_batch_size_bytes(self, size_reduction_ratio: float = 0.9) -> int:
        """
        Returns the default maximum batch size (in bytes) based on the server configuration.

        Args:
            size_reduction_ratio (float): A multiplier to reduce the server's max message size.
                Defaults to 0.9 (90% of the server's max message size).

        Returns:
            int: The calculated maximum batch size in bytes.
        """
        return int(self.server_info.config.max_grpc_message_size * size_reduction_ratio)


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
) -> ConnectionContext:
    """
    Factory function to establish a single PyArrow Flight client connection.

    Args:
        host (str): The hostname or IP address of the server.
        port (int): The port number to connect to.
        timeout (int): The waiting-for-connection timeout in seconds (default = 2s)
        enable_tls (bool): Enable TLS communication.
        compression (GRPCCompression): The gRPC compression configuration.
        tls_cert (Optional[bytes]): The contents of the TLS certificate file.
        middlewares (Optional[dict[str, fl.ClientMiddlewareFactory]]): The middlewares to be used for the connection.

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

    return ConnectionContext(
        flight_client=client,
        server_info=_wait_for_available(client, timeout),
    )
