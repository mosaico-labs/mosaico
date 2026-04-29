from dataclasses import dataclass
from typing import Any, Dict, List, Optional, Tuple, Union

from pyarrow.flight import FlightEndpoint

from mosaicolabs.logging_config import get_logger

from ..helpers.helpers import unpack_topic_full_path
from .helpers import _decode_app_metadata

# Set the hierarchical logger
logger = get_logger(__name__)


class TopicManifestError(Exception):
    """Raised when TopicResourceManifest cannot be extracted from an endpoint."""

    pass


class SequenceManifestError(Exception):
    """Raised when SequenceResourceManifest cannot be extracted from `app_metadata`."""

    pass


class SessionManifestError(Exception):
    """Raised when SessionResourceManifest cannot be extracted from `app_metadata`."""

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
        locked (bool): Whether the resource is locked.
        total_size_bytes (int): The aggregate size of all data chunks in bytes.
        chunks_number (int): The total count of data partitions (chunks)
            stored on the server.
        completed_timestamp (Optional[int]): The completion timestamp of the resource in nanoseconds.
        timestamp_ns_min (Optional[int]): The minimum timestamp of the data in the topic.
        timestamp_ns_max (Optional[int]): The maximum timestamp of the data in the topic.
    """

    name: str
    sequence_name: str
    created_timestamp: int
    locked: bool
    total_size_bytes: int
    chunks_number: int
    completed_timestamp: Optional[int]
    timestamp_ns_min: Optional[int]
    timestamp_ns_max: Optional[int]

    @classmethod
    def _from_flight_endpoint(
        cls,
        endpoint: FlightEndpoint,
    ) -> "TopicResourceManifest":
        """
        Factory method to create a manifest from an Arrow Flight app_metadata.

        Args:
            app_mdata (Union[bytes, str]): The app_metadata from the FlightInfo.

        Returns:
            TopicResourceMetadata: An immutable instance containing parsed data.

        Raises:
            TopicManifestError: If the endpoint `app_metadata` misses required keys or it is not possible
                to unpack topic and sequence names from the locator.
        """
        try:
            app_mdata = _decode_app_metadata(endpoint.app_metadata)

            resrc_loc = app_mdata.get("resource_locator")
            if resrc_loc is None:
                raise TopicManifestError(
                    "Expected `resource_locator` key in app_metadata."
                )
            info_mdata = app_mdata.get("info", {})
            if not isinstance(info_mdata, dict):
                raise TopicManifestError(
                    f"Unrecognized format for key 'info' in app_metadata: type {type(info_mdata).__name__}, expected a JSON."
                )

            chunks_number = info_mdata.get("chunks_number")
            total_size_bytes = info_mdata.get("total_bytes")

            if chunks_number is None or total_size_bytes is None:
                raise TopicManifestError(
                    "'info' data in app_metadata misses required fields."
                )

            tmin, tmax = cls._parse_timestamp_range(info_mdata.get("timestamp", {}))

            created_timestamp = app_mdata.get("created_at_ns")
            locked = app_mdata.get("locked")
            if created_timestamp is None or locked is None:
                raise TopicManifestError(
                    "Invalid format for 'info' data in app_metadata: missing required fields. The related topic can be malformed."
                )

            locator_tuple = unpack_topic_full_path(resrc_loc)
            if locator_tuple is None:
                raise TopicManifestError(
                    f"Invalid format for 'resource_locator': cannot deduce sequence and topic name from '{resrc_loc}'."
                )

            seq_name, top_name = locator_tuple

            return cls(
                name=top_name,
                sequence_name=seq_name,
                created_timestamp=created_timestamp,
                completed_timestamp=app_mdata.get("completed_at_ns"),
                locked=locked,
                total_size_bytes=total_size_bytes,
                chunks_number=chunks_number,
                timestamp_ns_min=tmin,
                timestamp_ns_max=tmax,
            )

        except Exception as e:
            # Wrap internal errors (like UnicodeDecode or Unpacking errors)
            # into a domain-specific exception for the caller to handle.
            raise TopicManifestError(
                f"Failed to parse topic manifest from endpoint: {e}"
            ) from e

    @staticmethod
    def _parse_timestamp_range(
        tstamp_mdata: dict,
    ) -> Tuple[Optional[int], Optional[int]]:
        """
        Parses the minimum and maximum timestamps of the resource.

        Args:
            tstamp_mdata (dict): The timestamp metadata.

        Returns:
            Tuple[Optional[int], Optional[int]]: The minimum and maximum timestamps.
        """
        # (can be missing in manifest - i.e. degenerate Topics with no data stream)
        tmin = None
        tmax = None
        # Can be null (i.e. "timestamp" present but empty)
        if isinstance(tstamp_mdata, dict):
            tmin = tstamp_mdata.get("start_ns")
            tmax = tstamp_mdata.get("end_ns")
            # Ensure both keys exist
            if (tmin is None) != (tmax is None):
                logger.error(
                    f"Wrong format of 'timestamp' field: 'min' or 'max' are None, but not both, {tstamp_mdata}"
                )

        return tmin, tmax


@dataclass
class SessionResourceManifest:
    """
    Metadata and structural information for a Mosaico Session resource.

    This Data Transfer Object summarizes the physical and logical state of a
    session on the server, retrieved via the get_fligh_info enpoint (for a sequence).

    Attributes:
        locator (str): The locator of the session.
            The locator format is: '`sequence_name`:`session_identifier`'.
        created_timestamp (int): The UTC timestamp of when the
            resource was first initialized.
        locked (bool): Whether the session is locked.
        completed_timestamp (int): The UTC timestamp of when the
            resource was completed.
        topics (list[str]): The list of topics in the session.
    """

    locator: str
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

        Raises:
            SessionManifestError: If the endpoint `app_metadata` misses required keys.
        """

        # This should never happen. If it does, it's a malformed session.
        if not isinstance(session_mdata, dict):
            raise SessionManifestError(
                f"Unrecognized type {type(session_mdata).__name__} for 'session' field in app_metadata."
            )

        locator = session_mdata.get("locator")
        created_timestamp = session_mdata.get("created_at_ns")
        locked = session_mdata.get("locked")

        # This should never happen. If it does, it's a malformed session.
        if locator is None or created_timestamp is None or locked is None:
            raise SessionManifestError(
                f"Missing required 'locator' or 'created_at' or 'locked' in session-related app_metadata: {session_mdata}."
            )

        return SessionResourceManifest(
            locator=locator,
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
        locator (str): The standardized name of the sequence resource.
        created_timestamp (int): The creation timestamp of the sequence in nanoseconds.
        sessions (List[SessionResourceManifest]): The list of sessions manifests composing the sequence.
    """

    locator: str
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
            SequenceManifestError: If the endpoint `app_metadata` misses required keys.
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
                locator=resource_locator,
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
                f"Failed to parse metadata from app_metadata: {e}"
            ) from e
