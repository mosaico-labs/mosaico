from dataclasses import dataclass
from typing import Optional


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
        max_grpc_message_size (int): The maximum message size (in bytes) accepted/emitted by the gRPC protocol.
        target_message_size (int): The target message size (in bytes) the server aims for when streaming data.
    """

    max_grpc_message_size: int
    """The maximum message size (in bytes) accepted/emitted by the gRPC protocol."""
    target_message_size: int
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
    def from_dict(cls, data: dict) -> "ServerInfo":
        """
        Factory method to create a ServerInfo instance from a dictionary.

        Args:
            data (dict): A dictionary containing server info data.
        """
        semver_data = data.get("semver")
        config_data = data.get("config")

        if semver_data is None:
            raise KeyError("Unable to find 'semver' key in data dict.")
        if config_data is None:
            raise KeyError("Unable to find 'config' key in data dict.")

        semver = SemVerItem(
            major=semver_data["major"],
            minor=semver_data["minor"],
            patch=semver_data["patch"],
            pre=semver_data.get("pre"),
        )

        config = ServerConfig(
            max_grpc_message_size=config_data["max_grpc_message_size"],
            target_message_size=config_data["target_message_size"],
        )

        return cls(
            version=data["version"],
            semver=semver,
            config=config,
        )
