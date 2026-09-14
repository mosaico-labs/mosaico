"""
Session Catalog Entity.

This module defines the `Session` class, which represents a read-only view of a
server-side writing Session platform resource.
"""

from dataclasses import dataclass
from typing import List, Optional

from mosaicolabs.helpers.helpers import unpack_topic_full_path
from mosaicolabs.platform.app_metadata import (
    SessionAppMetadata,
)


@dataclass(frozen=True)
class Session:
    """
    Represents a read-only view of a server-side writing Session platform resource.

    The `Session` class is designed to hold system-level metadata. It serves as the primary
    metadata container for a logical grouping of topics written in the writing session.

    Important: Data Retrieval
        This class provides a server-side **metadata-only** view of the session.
        To retrieve the actual time-series data contained within the topics of the session, you must
        use the [`TopicHandler.get_data_streamer()`][mosaicolabs.handlers.TopicHandler.get_data_streamer]
        method from a [`TopicHandler`][mosaicolabs.handlers.TopicHandler] instance.

    ### Querying with the **`.Q` Proxy**
    The session fields are not queryable via the **`.Q` proxy**.
    """

    locator: str
    """
    The session locator. 
    
    The locator is in the form '`sequence_name`:`session_identifier`', 
    e.g.: 'test-sequence-datastream:01KQ9XQ0HJ3V39F87CBP6PYA1T'
    """

    created_timestamp: int
    """The UTC timestamp [ns] when the writing session started"""

    locked: bool
    """The locked/unlocked status of the session"""

    completed_timestamp: Optional[int]
    """The UTC timestamp [ns] of the session finalization."""

    topics: List[str]
    """The list of topics recorded during this writing session"""

    @classmethod
    def _from_app_metadata(cls, app_metadata: SessionAppMetadata):
        """
        Factory method to create a Session from a SessionAppMetadata.

        Args:
            app_metadata (SessionAppMetadata): The app metadata for the session.

        Returns:
            Self: An initialized instance of this class.
        """
        topics = []
        for t_resrc_path in app_metadata.topics:
            seq_topic_tuple = unpack_topic_full_path(t_resrc_path)
            if not seq_topic_tuple:
                raise ValueError(f"Invalid topic name in response '{t_resrc_path}'")
            _, tname = seq_topic_tuple
            topics.append(tname)

        return cls(
            locator=app_metadata.locator,
            completed_timestamp=app_metadata.completed_timestamp,
            created_timestamp=app_metadata.created_timestamp,
            topics=topics,
            locked=app_metadata.locked,
        )
