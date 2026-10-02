from dataclasses import dataclass
from typing import Optional

from mosaicolabs.proto.v1 import responses_pb2


@dataclass(frozen=True)
class SemVerItem:
    """
    Metadata container for the server semantic version.

    This class acts as a Value Object, standardizing server info extracted from
    'info' DoAction. Being 'frozen' ensures the metadata remains immutable and
    hashable throughout its lifecycle.

    Attributes:
        major (int): The major version number.
        minor (int): The minor version number.
        patch (int): The patch version number.
        pre (str): The pre-release version identifier.
    """

    major: int
    """The major version number."""
    minor: int
    """The minor version number."""
    patch: int
    """The patch version number."""
    pre: Optional[str]
    """The pre-release version identifier."""


@dataclass(frozen=True)
class ServerConfig:
    """
    Metadata container for the server configuration.

    This class acts as a Value Object, standardizing server info extracted from
    'info' DoAction. Being 'frozen' ensures the metadata remains immutable and
    hashable throughout its lifecycle.

    Attributes:
        grpc_max_decode_message_size (int): The maximum incoming message size (in bytes) accepted by the server.
        grpc_max_encode_message_size (int): The maximum outgoing message size (in bytes) the server can emit.
        grpc_target_encode_message_size (int): The target message size (in bytes) the server aims for when streaming data.
    """

    grpc_max_decode_message_size: int
    """The maximum incoming message size (in bytes) accepted by the server."""
    grpc_max_encode_message_size: int
    """The maximum outgoing message size (in bytes) the server can emit."""
    grpc_target_encode_message_size: int
    """The target message size (in bytes) the server aims for when streaming data."""


@dataclass(frozen=True)
class ServerInfo:
    """
    Metadata container for the server info resource.

    This class acts as a Value Object, standardizing server info extracted from
    'info' DoAction. Being 'frozen' ensures the metadata remains immutable and
    hashable throughout its lifecycle.

    Attributes:
        version (str): The server version string.
        semver (SemVerItem): The semantic versioning details of the server.
        config (ServerConfig): The configuration details of the server.
    """

    version: str
    """The server version string."""
    semver: SemVerItem
    """The semantic versioning details of the server."""
    config: ServerConfig
    """The configuration details of the server."""

    @classmethod
    def _from_proto(cls, msg: responses_pb2.ServerInfo) -> "ServerInfo":
        """
        Factory method to create a ServerInfo instance from a decoded protobuf message.

        Args:
            msg (responses_pb2.ServerInfo): The decoded 'info' DoAction response.
        """
        semver = SemVerItem(
            major=msg.semver.major,
            minor=msg.semver.minor,
            patch=msg.semver.patch,
            pre=msg.semver.pre or None,
        )

        config = ServerConfig(
            grpc_max_decode_message_size=msg.config.grpc_max_decode_message_size,
            grpc_max_encode_message_size=msg.config.grpc_max_encode_message_size,
            grpc_target_encode_message_size=msg.config.grpc_target_encode_message_size,
        )

        return cls(
            version=msg.version,
            semver=semver,
            config=config,
        )
