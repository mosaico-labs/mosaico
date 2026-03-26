from dataclasses import dataclass
from typing import Any, Dict, List, Optional, Tuple, Union

import pyarrow.flight as fl

from mosaicolabs.logging_config import get_logger

from ..helpers.helpers import unpack_topic_full_path
from .helpers import _decode_app_metadata
from .resource_info import (
    TopicResourceInfo,
)

# Set the hierarchical logger
logger = get_logger(__name__)


class TopicManifestError(Exception):
    """Raised when TopicResourceMetadata cannot be extracted from an endpoint."""

    pass


class SequenceManifestError(Exception):
    """Raised when SequenceResourceMetadata cannot be extracted from an endpoint."""

    pass


@dataclass(frozen=True)
class TopicResourceManifest:
    """
    Metadata container for a specific data topic resource.

    This class acts as a Value Object, standardizing topic and sequence
    identifiers extracted from Arrow Flight app_metadata. Being 'frozen'
    ensures the metadata remains immutable and hashable throughout its lifecycle.

    Attributes:
        name (str): The standardized name of the resource.
        sequence_name (str): The name of the sequence the resource belongs to.
        created_timestamp (int): The creation timestamp of the resource in nanoseconds.
        completed_timestamp (Optional[int]): The completion timestamp of the resource in nanoseconds.
        locked (bool): Whether the resource is locked.
        resource_info (TopicResourceInfo): The server side system info of the resource.
    """

    name: str
    sequence_name: str
    resource_info: TopicResourceInfo
    created_timestamp: int
    locked: bool
    completed_timestamp: Optional[int]

    @classmethod
    def _get_topic_name_from_locations(cls, locations: List[fl.Location]) -> str:
        # Flight endpoints can technically have multiple locations for redundancy,
        # but our specific domain logic expects exactly one primary location.
        if not locations:
            raise TopicManifestError(
                "Endpoint contains no locations; cannot resolve topic."
            )

        if len(locations) > 1:
            raise TopicManifestError(
                f"Multi-location endpoints not supported. Found: {len(locations)}"
            )
        # Extract URI (stored as bytes in pyarrow.flight)
        uri_bytes = locations[0].uri

        # Delegate parsing logic to the internal static helper
        _, topic_name = cls._parse_uri(uri_bytes)

        return topic_name

    @classmethod
    def _from_app_metadata(
        cls,
        app_mdata: Union[bytes, str],
    ) -> "TopicResourceManifest":
        """
        Factory method to create a manifest from an Arrow Flight app_metadata.

        Args:
            app_mdata (Union[bytes, str]): The app_metadata from the FlightInfo.

        Returns:
            TopicResourceMetadata: An immutable instance containing parsed data.

        Raises:
            TopicParsingError: If the endpoint has no locations, multiple
                locations, or if the URI format is invalid.
        """
        try:
            mdata = _decode_app_metadata(app_mdata)

            created_timestamp = mdata.get("created_at_ns")
            locked = mdata.get("locked")
            resource_locator = mdata.get("resource_locator")
            if created_timestamp is None or locked is None or resource_locator is None:
                raise TopicManifestError(
                    "Invalid format for TopicResourceManifest: missing required fields. The related topic can be malformed."
                )

            resource_info = TopicResourceInfo._from_info_metadata(mdata.get("info", {}))
            locator_tuple = unpack_topic_full_path(resource_locator)
            if locator_tuple is None:
                raise TopicManifestError(
                    f"Invalid format for TopicResourceManifest: cannot deduce sequence and topic name from locator '{resource_locator}'."
                )

            seq_name, top_name = locator_tuple

            return cls(
                name=top_name,
                sequence_name=seq_name,
                created_timestamp=created_timestamp,
                completed_timestamp=mdata.get("completed_at_ns"),
                locked=locked,
                resource_info=resource_info,
            )

        except Exception as e:
            # Wrap internal errors (like UnicodeDecode or Unpacking errors)
            # into a domain-specific exception for the caller to handle.
            raise TopicManifestError(
                f"Failed to parse topic manifest from endpoint: {e}"
            ) from e

    @staticmethod
    def _parse_uri(uri_bytes: bytes) -> Tuple[str, str]:
        """
        Decodes and validates the raw URI string.

        Internal helper that handles the 'mosaico:' protocol stripping
        and string splitting logic.
        """
        # Decode bytes to string and protocol validation (mosaico resource)
        decoded_uri = uri_bytes.decode("utf-8")
        if not decoded_uri.startswith("mosaico:"):
            raise TopicManifestError(
                f"URI missing required 'mosaico:' prefix: {decoded_uri}"
            )

        # Path Extraction
        path = decoded_uri.removeprefix("mosaico:")

        # Domain-specific unpacking (expects a tuple of strings)
        result = unpack_topic_full_path(path)

        if not result or len(result) != 2:
            raise TopicManifestError(
                f"Path '{path}' is not a valid sequence/topic pair."
            )

        return result


@dataclass
class SessionResourceManifest:
    """
    Metadata and structural information for a Mosaico Session resource.

    This Data Transfer Object summarizes the physical and logical state of a
    session on the server, retrieved via the get_fligh_info enpoint (for a sequence).

    Attributes:
        uuid (str): The UUID of the session.
        created_timestamp (int): The UTC timestamp of when the
            resource was first initialized.
        completed_timestamp (int): The UTC timestamp of when the
            resource was completed.
        topics (list[str]): The list of topics in the session.
    """

    uuid: str
    created_timestamp: int
    locked: bool
    completed_timestamp: Optional[int]
    topics: list[str]

    @classmethod
    def _from_app_metadata(
        cls,
        session_mdata: Dict[str, Any],
    ) -> "SessionResourceManifest":
        """
        Internal static method to construct a SessionResourceManifest from app_metadata.

        Args:
            session_mdata (Dict[str, Any]): The app_metadata from the FlightInfo.

        Returns:
            SessionResourceManifest: The SessionResourceManifest object.
        """

        # This should never happen. If it does, it's a malformed session.
        if not isinstance(session_mdata, dict):
            raise ValueError(
                f"Unrecognized type {type(session_mdata).__name__} for 'session_mdata' field."
            )

        session_uuid = session_mdata.get("uuid")
        created_timestamp = session_mdata.get("created_at_ns")
        locked = session_mdata.get("locked", False)

        # This should never happen. If it does, it's a malformed session.
        if session_uuid is None or created_timestamp is None:
            raise ValueError(
                f"Missing required 'uuid' or 'created_at' in session-related app_metadata: {session_mdata}."
            )

        return SessionResourceManifest(
            uuid=session_uuid,
            created_timestamp=created_timestamp,
            completed_timestamp=session_mdata.get("completed_at_ns"),
            locked=locked,
            topics=session_mdata.get("topics", []),
        )


@dataclass(frozen=True)
class SequenceResourceManifest:
    """
    Metadata container for a specific data sequence resource.

    This class acts as a Value Object, standardizing topic and sequence
    identifiers extracted from Arrow Flight transport layers. Being 'frozen'
    ensures the metadata remains immutable and hashable throughout its lifecycle.

    Attributes:
        resource_locator (str): The standardized name of the resource sequence.
        created_timestamp (int): The creation timestamp of the sequence in nanoseconds.
        sessions (List[SessionResourceManifest]): The list of sessions manifests composing the sequence.
    """

    resource_locator: str
    created_timestamp: int
    sessions: List[SessionResourceManifest]

    @classmethod
    def _from_app_metadata(
        cls,
        app_mdata: Union[bytes, str],
    ) -> "SequenceResourceManifest":
        """
        Factory method to create a SequenceResourceManifest from FlightInfo.app_metadata.

        Args:
            app_mdata: The app_metadata object containing the sequence resource info.

        Returns:
            SequenceResourceManifest: An immutable instance containing parsed data.

        Raises:
            SequenceManifestError: If the endpoint has no locations, multiple
                locations, or if the URI format is invalid.
        """

        try:
            # Parse and return the app_metadata fields
            mdata = _decode_app_metadata(app_mdata)

            resource_locator = mdata.get("resource_locator")
            created_timestamp = mdata.get("created_at_ns")
            if resource_locator is None or created_timestamp is None:
                raise SequenceManifestError(
                    "Unable to construct a 'SequenceResourceManifest': missing required fields in sequence app_metadata."
                )

            sessions = mdata.get("sessions", [])
            # FIXME: maybe not necessary
            if not isinstance(sessions, list):
                sessions = []

            return cls(
                resource_locator=resource_locator,
                created_timestamp=created_timestamp,
                sessions=[
                    SessionResourceManifest._from_app_metadata(session)
                    for session in sessions
                ],
            )

        except Exception as e:
            # Wrap internal errors (like UnicodeDecode or Unpacking errors)
            # into a domain-specific exception for the caller to handle.
            raise SequenceManifestError(
                f"Failed to parse metadata from endpoint: {e}"
            ) from e
