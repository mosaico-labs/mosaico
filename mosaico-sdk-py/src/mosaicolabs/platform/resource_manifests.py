from dataclasses import dataclass
from typing import Any, Dict, List, Optional, Tuple

from pyarrow.flight import FlightEndpoint

from mosaicolabs.enum.serialization_format import SerializationFormat
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


def _get_metadata_value(
    metadata: Dict[str, Any],
    key: str,
    is_mandatory: bool = True,
    default: Optional[Any] = None,
) -> Any:
    """
    Safely retrieves a value from the metadata dictionary.

    Args:
        metadata (Dict[str, Any]): The metadata dictionary.
        key (str): The key to retrieve.
        is_mandatory (bool): Whether the key is mandatory.
        default (Optional[Any]): The default value to return if the key is not found.

    Returns:
        Any: The value associated with the key or the default value.
    """
    value = metadata.get(key, default)
    if is_mandatory and value is None:
        raise ValueError(f"Missing mandatory key '{key}' in metadata.")
    return value


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
    total_chunks_count: int
    ontology_tag: str
    serialization_format: SerializationFormat
    user_metadata: dict
    completed_timestamp: Optional[int]
    timestamp_ns_min: Optional[int]
    timestamp_ns_max: Optional[int]
    total_row_count: int

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

            created_timestamp = _get_metadata_value(app_mdata, "created_at_ns")
            locked = _get_metadata_value(app_mdata, "locked")
            resrc_loc = _get_metadata_value(app_mdata, "resource_locator")
            ontology_tag = _get_metadata_value(app_mdata, "ontology_tag")
            serialization_format = _get_metadata_value(
                app_mdata, "serialization_format"
            )
            try:
                serialization_format = SerializationFormat(serialization_format)
            except Exception as e:
                raise ValueError(
                    f"Unable to convert to a valid 'SerializationFormat'.\nInner err: {e}"
                )
            user_metadata = _get_metadata_value(app_mdata, "user_metadata")
            info_mdata = _get_metadata_value(app_mdata, "data_info")
            if not isinstance(info_mdata, dict):
                raise TopicManifestError(
                    f"Unrecognized format for key 'data_info' in app_metadata: type {type(info_mdata).__name__}, expected a JSON."
                )

            total_size_bytes = _get_metadata_value(info_mdata, "total_bytes")
            total_chunks_count = _get_metadata_value(info_mdata, "total_chunks_count")

            locator_tuple = unpack_topic_full_path(resrc_loc)
            if locator_tuple is None:
                raise TopicManifestError(
                    f"Invalid format for 'resource_locator': cannot deduce sequence and topic name from '{resrc_loc}'."
                )

            tmax = tmin = total_row_count = None
            # Get timestamp and row counts from 'time_window_info' first: if not None, an inner range has been asked
            time_window_info = _get_metadata_value(
                app_mdata, "time_window_info", is_mandatory=False
            )
            if time_window_info is not None:
                # If an inner range has nbeen asked, the fields are mandatory
                tmin, tmax = cls._parse_timestamp_range(
                    _get_metadata_value(
                        time_window_info, "interval", is_mandatory=False, default={}
                    )
                )
                total_row_count = _get_metadata_value(time_window_info, "row_count")
            else:
                # If no inner range has been asked, get the global timestamp range from 'data_info'
                # The fiels are mandatory
                tmin, tmax = cls._parse_timestamp_range(
                    _get_metadata_value(
                        info_mdata, "interval", is_mandatory=False, default={}
                    )
                )
                total_row_count = _get_metadata_value(info_mdata, "total_row_count")

            seq_name, top_name = locator_tuple

            return cls(
                name=top_name,
                sequence_name=seq_name,
                created_timestamp=created_timestamp,
                completed_timestamp=_get_metadata_value(
                    app_mdata, "completed_at_ns", is_mandatory=False
                ),
                locked=locked,
                serialization_format=serialization_format,
                ontology_tag=ontology_tag,
                total_size_bytes=total_size_bytes,
                total_chunks_count=total_chunks_count,
                user_metadata=user_metadata,
                timestamp_ns_min=tmin,
                timestamp_ns_max=tmax,
                total_row_count=total_row_count,
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
            tmin = _get_metadata_value(tstamp_mdata, "start_ns", is_mandatory=False)
            tmax = _get_metadata_value(tstamp_mdata, "end_ns", is_mandatory=False)
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

        locator = _get_metadata_value(session_mdata, "locator")
        created_timestamp = _get_metadata_value(session_mdata, "created_at_ns")
        locked = _get_metadata_value(session_mdata, "locked")

        return SessionResourceManifest(
            locator=locator,
            created_timestamp=created_timestamp,
            completed_timestamp=_get_metadata_value(
                session_mdata, "completed_at_ns", is_mandatory=False
            ),
            locked=locked,
            topics=_get_metadata_value(
                session_mdata, "topics", is_mandatory=False, default=[]
            ),
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
    user_metadata: dict
    sessions: List[SessionResourceManifest]

    @classmethod
    def _from_decoded_app_metadata(
        cls,
        app_mdata: Dict[str, Any],
    ) -> "SequenceResourceManifest":
        """
        Factory method to create a SequenceResourceManifest from FlightInfo.app_metadata.

        Args:
            app_mdata (Union[bytes, str]): The app_metadata object containing the sequence resource info.

        Returns:
            SequenceResourceManifest: An immutable instance containing parsed data.

        Raises:
            SequenceManifestError: If the endpoint `app_metadata` misses required keys.
        """

        try:
            resource_locator = _get_metadata_value(app_mdata, "resource_locator")
            created_timestamp = _get_metadata_value(app_mdata, "created_at_ns")
            user_metadata = _get_metadata_value(app_mdata, "user_metadata")

            sessions = _get_metadata_value(
                app_mdata, "sessions", is_mandatory=False, default=[]
            )

            return cls(
                locator=resource_locator,
                created_timestamp=created_timestamp,
                user_metadata=user_metadata,
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
