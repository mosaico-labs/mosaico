from dataclasses import dataclass
from typing import Any, Dict, List, Optional

from mosaicolabs.logging_config import get_logger

# Set the hierarchical logger
logger = get_logger(__name__)


@dataclass
class TopicResourceInfo:
    """
    Metadata and structural information for a Mosaico
    [`Topic`][mosaicolabs.models.platform.Topic] resource.

    This Data Transfer Object summarizes the physical and logical state of a
    topic on the server, retrieved via the get_fligh_info enpoint.

    Attributes:
        created_timestamp (int): The UTC timestamp of when the
            resource was first initialized.
        total_size_bytes (int): The aggregate size of all data chunks in bytes.
        locked (bool): Indicates if the resource is currently read-only.
            Usually true if an upload is finalized or a retention policy is active.
        chunks_number (int): The total count of data partitions (chunks)
            stored on the server.
    """

    created_timestamp: int
    total_size_bytes: int
    locked: bool
    chunks_number: int

    @classmethod
    def _from_info_metadata(
        cls,
        info_data: Dict[str, Any],
    ) -> "TopicResourceInfo":
        """
        Internal static method to retrieve topic-related remote info.
        Queries the server to build the `Topic` model and discover all
        contained topics.

        Args:
            app_mdata (Union[bytes, str]): The app_metadata from the FlightInfo.

        Returns:
            TopicResourceInfo: The TopicResourceInfo object.
        """
        if not isinstance(info_data, dict):
            logger.error(
                f"Unrecognized type {type(info_data).__name__} for 'info' field. Returning 'invalid'."
            )
            # TODO: temporary fix before backend fixes this issue
            return TopicResourceInfo._make_void()

        chunks_number = info_data.get("chunks_number")
        locked = info_data.get("is_locked")
        total_size_bytes = info_data.get("total_size_bytes")
        created_timestamp = info_data.get("created_timestamp")

        if (
            chunks_number is None
            or locked is None
            or total_size_bytes is None
            or created_timestamp is None
        ):
            logger.error(
                "Missing required fields in 'info' field. Returning 'invalid'."
            )
            # TODO: temporary fix before backend fixes this issue
            return TopicResourceInfo._make_void()

        return TopicResourceInfo(
            chunks_number=chunks_number,
            locked=locked,
            total_size_bytes=total_size_bytes,
            created_timestamp=created_timestamp,
        )

    @classmethod
    def _make_void(cls) -> "TopicResourceInfo":
        """
        Internal static method to create a void TopicResourceInfo object.
        """
        return TopicResourceInfo(
            created_timestamp=0,
            total_size_bytes=0,
            locked=False,
            chunks_number=0,
        )


@dataclass
class SessionResourceInfo:
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
    completed_timestamp: Optional[int]
    # TODO: change to bool
    locked: Optional[bool]
    topics: list[str]

    @classmethod
    def _from_app_metadata(
        cls,
        session_mdata: Dict[str, Any],
    ) -> "SessionResourceInfo":
        """
        Internal static method to retrieve session-related remote info.
        Queries the server to build the `Session` model and discover all
        contained sessions.

        Args:
            app_mdata (Union[bytes, str]): The app_metadata from the FlightInfo.

        Returns:
            SessionResourceInfo: The SessionResourceInfo object.
        """

        # This should never happen. If it does, it's a malformed session.
        if not isinstance(session_mdata, dict):
            raise ValueError(
                f"Unrecognized type {type(session_mdata).__name__} for 'session_mdata' field."
            )

        session_uuid = session_mdata.get("uuid")
        created_timestamp = session_mdata.get("created_timestamp")

        # This should never happen. If it does, it's a malformed session.
        if session_uuid is None or created_timestamp is None:
            raise ValueError(
                f"Missing required 'uuid' or 'created_timestamp' in session-related app_metadata: {session_mdata}."
            )

        return SessionResourceInfo(
            uuid=session_uuid,
            created_timestamp=created_timestamp,
            completed_timestamp=session_mdata.get("completed_timestamp"),
            locked=session_mdata.get("is_locked"),
            topics=session_mdata.get("topics", []),
        )


@dataclass
class SequenceResourceInfo:
    """
    Metadata and structural information for a Mosaico
    [`Sequence`][mosaicolabs.models.platform.Sequence] resource.

    This Data Transfer Object summarizes the physical and logical state of a
    sequence on the server, retrieved via the get_fligh_info enpoint.

    Attributes:
        created_timestamp (int): The UTC timestamp of when the
            resource was first initialized.
        sessions (list[SessionResourceInfo]): The list of sessions in the sequence.
    """

    created_timestamp: int
    sessions: List[SessionResourceInfo]

    @classmethod
    def _from_app_metadata(
        cls,
        app_mdata: Dict[str, Any],
    ) -> "SequenceResourceInfo":
        """
        Internal static method to retrieve sequence-related remote info.
        Queries the server to build the `Sequence` model and discover all
        contained sequences.

        Args:
            app_mdata (Dict[str, Any]): The app_metadata dictionary from the FlightInfo.

        Returns:
            SequenceResourceInfo: The SequenceResourceInfo object.
        """
        # This should never happen. If it does, it's a malformed session.
        if not isinstance(app_mdata, dict):
            raise ValueError(
                f"Unrecognized type {type(app_mdata).__name__} for 'app_metadata' field."
            )

        created_timestamp = app_mdata.get("created_timestamp")
        sessions = app_mdata.get("sessions", [])
        # FIXME: maybe not necessary
        if not isinstance(sessions, list):
            sessions = []

        # This should never happen. If it does, it's a malformed session.
        if created_timestamp is None:
            raise ValueError("Missing required 'created_timestamp' in app_metadata.")

        return SequenceResourceInfo(
            created_timestamp=created_timestamp,
            sessions=[
                SessionResourceInfo._from_app_metadata(session) for session in sessions
            ],
        )
