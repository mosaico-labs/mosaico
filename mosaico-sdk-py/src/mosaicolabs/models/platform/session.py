"""
Sequence Catalog Entity.

This module defines the `Sequence` class, which represents a read-only view of a
Sequence's platform_metadata. A Sequence is a logical grouping of multiple Topics.
"""

from dataclasses import dataclass
from typing import List, Optional

from mosaicolabs.helpers.helpers import unpack_topic_full_path
from mosaicolabs.platform.resource_manifests import (
    SessionResourceManifest,
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

    uuid: str
    """The session UUID"""

    created_timestamp: int
    """The UTC timestamp [ns] when the writing session started"""

    locked: bool
    """The locked/unlocked status of the session"""

    completed_timestamp: Optional[int]
    """The UTC timestamp [ns] of the session finalization."""

    topics: List[str]
    """The list of topics recorded during this writing session"""

    @classmethod
    def _from_resource_manifest(cls, resrc_manifest: SessionResourceManifest):
        """
        Factory method to create a Session from a SessionResourceManifest.

        Args:
            resrc_manifest (SessionResourceManifest): The resource manifest for the session.

        Returns:
            Self: An initialized instance of this class.
        """
        topics = []
        for t_resrc_path in resrc_manifest.topics:
            seq_topic_tuple = unpack_topic_full_path(t_resrc_path)
            if not seq_topic_tuple:
                raise ValueError(f"Invalid topic name in response '{t_resrc_path}'")
            _, tname = seq_topic_tuple
            topics.append(tname)

        return cls(
            uuid=resrc_manifest.uuid,
            completed_timestamp=resrc_manifest.completed_timestamp,
            created_timestamp=resrc_manifest.created_timestamp,
            topics=topics,
            locked=resrc_manifest.locked,
        )
