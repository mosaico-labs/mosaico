import json
from dataclasses import dataclass
from typing import List, Optional

from pyarrow.flight import FlightEndpoint, FlightInfo

from mosaicolabs.enum.serialization_format import SerializationFormat
from mosaicolabs.logging_config import get_logger
from mosaicolabs.proto.v1 import flight_pb2

from ..helpers.helpers import unpack_topic_full_path
from . import _proto_format
from ._proto_time import from_proto as timestamp_range_from_proto

# Set the hierarchical logger
logger = get_logger(__name__)


class TopicAppMetadataError(Exception):
    """Raised when TopicAppMetadata cannot be extracted from an endpoint."""

    pass


class SequenceAppMetadataError(Exception):
    """Raised when SequenceAppMetadata cannot be extracted from `app_metadata`."""

    pass


class SessionAppMetadataError(Exception):
    """Raised when SessionAppMetadata cannot be extracted from `app_metadata`."""

    pass


def _decode_user_metadata(raw: bytes) -> dict:
    """
    Decodes the `user_metadata` raw-bytes field shared by several app_metadata
    messages: arbitrary user-supplied JSON, as raw UTF-8 text. Empty bytes
    means no metadata was set.
    """
    if not raw:
        return {}
    return json.loads(raw)


@dataclass(frozen=True)
class TopicAppMetadata:
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
        total_chunks_count (int): The total count of data partitions (chunks)
            stored on the server.
        ontology_tag (str): The ontology tag associated with the resource.
        serialization_format (SerializationFormat): The serialization format of the resource.
        user_metadata (dict): User-defined metadata associated with the resource.
        completed_timestamp (Optional[int]): The completion timestamp of the resource in nanoseconds.
        timestamp_ns_min (Optional[int]): The minimum timestamp of the data in the topic.
        timestamp_ns_max (Optional[int]): The maximum timestamp of the data in the topic.
        total_row_count (int): The total number of rows in the topic.
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
    ) -> "TopicAppMetadata":
        """
        Factory method to create a TopicAppMetadata from an Arrow Flight endpoint.

        Args:
            endpoint (FlightEndpoint): The FlightEndpoint carrying the app_metadata to parse.

        Returns:
            TopicAppMetadata: An immutable instance containing parsed data.

        Raises:
            TopicAppMetadataError: If the endpoint `app_metadata` misses required keys or it is not possible
                to unpack topic and sequence names from the locator.
        """
        try:
            msg = flight_pb2.TopicAppMetadata.FromString(endpoint.app_metadata)

            try:
                serialization_format = _proto_format.from_proto(
                    msg.serialization_format
                )
            except ValueError as e:
                raise ValueError(
                    f"Unable to convert to a valid 'SerializationFormat'.\nInner err: {e}"
                )

            if not msg.HasField("data_info"):
                raise TopicAppMetadataError(
                    "Missing mandatory field 'data_info' in app_metadata."
                )

            locator_tuple = unpack_topic_full_path(msg.locator)
            if locator_tuple is None:
                raise TopicAppMetadataError(
                    f"Invalid format for 'locator': cannot deduce sequence and topic name from '{msg.locator}'."
                )

            tmax = tmin = total_row_count = None
            # Get timestamp and row counts from 'time_window_info' first: if set, an inner range has been asked
            if msg.HasField("time_window_info"):
                # If an inner range has been asked, the fields are mandatory
                tmin, tmax = timestamp_range_from_proto(
                    msg.time_window_info.interval
                    if msg.time_window_info.HasField("interval")
                    else None
                )
                total_row_count = msg.time_window_info.row_count
            else:
                # If no inner range has been asked, get the global timestamp range from 'data_info'
                tmin, tmax = timestamp_range_from_proto(
                    msg.data_info.interval
                    if msg.data_info.HasField("interval")
                    else None
                )
                total_row_count = msg.data_info.total_row_count

            seq_name, top_name = locator_tuple

            return cls(
                name=top_name,
                sequence_name=seq_name,
                created_timestamp=msg.created_at_ns,
                completed_timestamp=(
                    msg.completed_at_ns if msg.HasField("completed_at_ns") else None
                ),
                locked=msg.locked,
                serialization_format=serialization_format,
                ontology_tag=msg.ontology_tag,
                total_size_bytes=msg.data_info.total_bytes,
                total_chunks_count=msg.data_info.total_chunks_count,
                user_metadata=_decode_user_metadata(msg.user_metadata),
                timestamp_ns_min=tmin,
                timestamp_ns_max=tmax,
                total_row_count=total_row_count,
            )

        except Exception as e:
            # Wrap internal errors (like decode or Unpacking errors)
            # into a domain-specific exception for the caller to handle.
            raise TopicAppMetadataError(
                f"Failed to parse topic metadata from endpoint: {e}"
            ) from e


@dataclass
class SessionAppMetadata:
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
    def _from_proto(
        cls,
        msg: flight_pb2.SessionAppMetadata,
    ) -> "SessionAppMetadata":
        """
        Internal factory method to construct a SessionAppMetadata from a decoded
        protobuf message.

        Args:
            msg (flight_pb2.SessionAppMetadata): The decoded session app_metadata.

        Returns:
            SessionAppMetadata: The SessionAppMetadata object.
        """
        return cls(
            locator=msg.locator,
            created_timestamp=msg.created_at_ns,
            completed_timestamp=(
                msg.completed_at_ns if msg.HasField("completed_at_ns") else None
            ),
            locked=msg.locked,
            topics=list(msg.topics),
        )


@dataclass(frozen=True)
class SequenceAppMetadata:
    """
    Metadata container for a specific data sequence resource.

    This class acts as a Value Object, standardizing topic and sequence
    identifiers extracted from Arrow Flight transport layers. Being 'frozen'
    ensures the metadata remains immutable and hashable throughout its lifecycle.

    Attributes:
        locator (str): The standardized name of the sequence resource.
        created_timestamp (int): The creation timestamp of the sequence in nanoseconds.
        user_metadata (dict): User-defined metadata associated with the sequence.
        sessions (List[SessionAppMetadata]): The list of session metadata objects composing the sequence.
    """

    locator: str
    created_timestamp: int
    user_metadata: dict
    sessions: List[SessionAppMetadata]

    @classmethod
    def _from_proto(
        cls,
        msg: flight_pb2.SequenceAppMetadata,
    ) -> "SequenceAppMetadata":
        """
        Factory method to create a SequenceAppMetadata from a decoded protobuf message.

        Args:
            msg (flight_pb2.SequenceAppMetadata): The decoded sequence app_metadata.

        Returns:
            SequenceAppMetadata: An immutable instance containing parsed data.
        """
        try:
            return cls(
                locator=msg.locator,
                created_timestamp=msg.created_at_ns,
                user_metadata=_decode_user_metadata(msg.user_metadata),
                sessions=[
                    SessionAppMetadata._from_proto(session) for session in msg.sessions
                ],
            )

        except Exception as e:
            # Wrap internal errors (like decode errors) into a domain-specific
            # exception for the caller to handle.
            raise SequenceAppMetadataError(
                f"Failed to parse metadata from app_metadata: {e}"
            ) from e

    @classmethod
    def _from_flight_info(cls, flight_info: FlightInfo) -> "SequenceAppMetadata":
        """
        Factory method to create a SequenceAppMetadata straight from a FlightInfo's
        top-level `app_metadata` field.

        Args:
            flight_info (FlightInfo): The FlightInfo carrying the sequence's app_metadata.

        Returns:
            SequenceAppMetadata: An immutable instance containing parsed data.
        """
        msg = flight_pb2.SequenceAppMetadata.FromString(flight_info.app_metadata)
        return cls._from_proto(msg)
