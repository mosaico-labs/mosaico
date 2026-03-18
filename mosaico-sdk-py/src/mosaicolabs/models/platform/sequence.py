"""
Sequence Catalog Entity.

This module defines the `Sequence` class, which represents a read-only view of a
Sequence's platform_metadata. A Sequence is a logical grouping of multiple Topics.
"""

from typing import Any, Dict, List

import pydantic
from pydantic import PrivateAttr
from typing_extensions import Self

from mosaicolabs.platform.metadata import SequenceMetadata
from mosaicolabs.platform.resource_manifests import (
    SequenceResourceManifest,
)

from ..query.expressions import _QuerySequenceExpression
from ..query.generation.api import _QueryProxyMixin, queryable
from ..query.generation.pydantic_mapper import PydanticFieldMapper
from .session import Session


@queryable(
    mapper_type=PydanticFieldMapper,
    prefix="",
    query_expression_type=_QuerySequenceExpression,
)
class Sequence(pydantic.BaseModel, _QueryProxyMixin):
    """
    Represents a read-only view of a server-side Sequence platform resource.

    The `Sequence` class is designed to hold system-level metadata and enable fluid querying of
    user-defined properties. It serves as the primary metadata container
    for a logical grouping of related topics.

    Important: Data Retrieval
        This class provides a server-side **metadata-only** view of the sequence.
        To retrieve the actual time-series data contained within the sequence, you must
        use the [`SequenceHandler.get_data_streamer()`][mosaicolabs.handlers.SequenceHandler.get_data_streamer]
        method from a [`SequenceHandler`][mosaicolabs.handlers.SequenceHandler]
        instance.

    ### Querying with the **`.Q` Proxy**
    Warning: Deprecated
        Querying the sequence user-custom metadata via the `user_metadata` field of this class is deprecated.
        Use the [`QuerySequence.with_user_metadata()`][mosaicolabs.models.query.builders.QuerySequence.with_user_metadata] builder instead.

    Example:
        ```python
        from mosaicolabs import MosaicoClient, Sequence, QuerySequence

        with MosaicoClient.connect("localhost", 6726) as client:
            # Filter for a specific data value (using constructor)
            qresponse = client.query(
                QuerySequence()
                .with_user_metadata("project", eq="Apollo")
                .with_user_metadata("vehicle.software_stack.planning", eq="plan-4.1.7")
            )

            # Inspect the response
            if qresponse is not None:
                # Results are automatically grouped by Sequence for easier data management
                for item in qresponse:
                    print(f"Sequence: {item.sequence.name}")
                    print(f"Topics: {[topic.name for topic in item.topics]}")
        ```
    """

    user_metadata: Dict[str, Any]
    """
    Custom user-defined key-value pairs associated with the entity.

    ### Querying with the **`.Q` Proxy**
    Warning: Deprecated
        Querying the sequence user-custom metadata via the `user_metadata` field of this class is deprecated.
        Use the [`QuerySequence.with_user_metadata()`][mosaicolabs.models.query.builders.QuerySequence.with_user_metadata] builder instead.

    """

    # --- Private Fields ---
    # They are excluded from the standard Pydantic __init__ to prevent users
    # from manually setting system-controlled values.
    _created_timestamp: int = PrivateAttr()
    """The UTC timestamp when the sequence was created."""

    _name: str = PrivateAttr()
    """The name of the sequence."""

    _total_size_bytes: int = PrivateAttr()
    """The aggregated total size of the sequence in bytes"""

    _sessions: List[Session] = PrivateAttr(default_factory=list)
    """The list of sessions in the sequence"""

    @classmethod
    def _from_resource_info(
        cls,
        name: str,
        total_size_bytes: int,
        platform_metadata: SequenceMetadata,
        resrc_manifest: SequenceResourceManifest,
    ) -> Self:
        """
        Factory method to create a Sequence view from platform resource information.

        Args:
            name: The name of the platform resource.
            total_size_bytes: The total size of the sequence in bytes.
            platform_metadata: The metadata of the platform resource.
            resrc_manifest: The manifest of the platform resource.

        Returns:
            A Sequence instance.
        """
        if not isinstance(platform_metadata, SequenceMetadata):
            raise ValueError(
                "Metadata must be an instance of `mosaicolabs.comm.SequenceMetadata`."
            )
        user_metadata = getattr(platform_metadata, "user_metadata", None)
        if user_metadata is None:
            raise ValueError("Metadata must have a `user_metadata` attribute.")

        instance = cls(user_metadata=user_metadata)

        # Initialize shared private attrs
        instance._init_base_private(
            name=name,
            total_size_bytes=total_size_bytes,
            created_timestamp=resrc_manifest.created_timestamp,
            sessions=[
                Session._from_resource_manifest(s) for s in resrc_manifest.sessions
            ],
        )

        return instance

    def _init_base_private(
        self,
        *,
        name: str,
        created_timestamp: int,
        total_size_bytes: int,
        sessions: List[Session],
    ) -> None:
        """
        Internal helper to populate system-controlled private attributes.

        This is used by factory methods (`_from_resource_info`) to set attributes
        that are strictly read-only for the user.

        Args:
            name: The unique resource name.
            created_timestamp: The UTC timestamp of creation.
            total_size_bytes: The total size of the sequence in bytes.
            sessions: The list of sessions associated with this sequence.
        """
        self._created_timestamp = created_timestamp
        self._name = name
        self._sessions = sessions
        self._total_size_bytes = total_size_bytes

    # --- Properties ---
    @property
    def topics(self) -> List[str]:
        """
        Returns the list of names for all topics contained within this sequence.

        Note: Accessing Topic Data
            This property returns string identifiers. To interact
            with topic data or metadata, use the
            [`MosaicoClient.topic_handler()`][mosaicolabs.comm.MosaicoClient.topic_handler]
            factory.

        ### Querying with **Query Builders**
        The `topics` property is not queryable directly. Use [`QueryTopic`][mosaicolabs.models.query.QueryTopic] to query for topics.

        Example:
            ```python
            from mosaicolabs import MosaicoClient, QueryTopic

            with MosaicoClient.connect("localhost", 6726) as client:
                # Filter for a specific data value (using constructor)
                qresponse = client.query(
                    QueryTopic().with_name("/sensors/camera/front/image_raw")
                )

                # Inspect the response
                if qresponse is not None:
                    # Results are automatically grouped by Sequence for easier data management
                    for item in qresponse:
                        print(f"Sequence: {item.sequence.name}")
                        print(f"Topics: {[topic.name for topic in item.topics]}")
            ```
        """
        return [t for s in self._sessions for t in s.topics]

    @property
    def name(self) -> str:
        """
        The unique identifier or resource name of the entity.

        ### Querying with **Query Builders**
        The `name` property is queryable when constructing a [`QuerySequence`][mosaicolabs.models.query.QuerySequence]
        via the convenience methods:

        * [`QuerySequence.with_name()`][mosaicolabs.models.query.builders.QuerySequence.with_name]
        * [`QuerySequence.with_name_match()`][mosaicolabs.models.query.builders.QuerySequence.with_name_match]

        Example:
            ```python
            from mosaicolabs import MosaicoClient, QuerySequence

            with MosaicoClient.connect("localhost", 6726) as client:
                # Filter for a specific data value (using constructor)
                qresponse = client.query(
                    QuerySequence().with_name_match("test_winter_2025_01_"),
                )

                # Inspect the response
                if qresponse is not None:
                    # Results are automatically grouped by Sequence for easier data management
                    for item in qresponse:
                        print(f"Sequence: {item.sequence.name}")
                        print(f"Topics: {[topic.name for topic in item.topics]}")
            ```
        """
        return self._name

    @property
    def created_timestamp(self) -> int:
        """
        The UTC timestamp indicating when the entity was created on the server.

        ### Querying with **Query Builders**
        The `created_timestamp` property is queryable when constructing a [`QuerySequence`][mosaicolabs.models.query.QuerySequence]
        via the convenience method:

        * [`QuerySequence.with_created_timestamp()`][mosaicolabs.models.query.builders.QuerySequence.with_created_timestamp]

        Example:
            ```python
            from mosaicolabs import MosaicoClient, QuerySequence, Time

            with MosaicoClient.connect("localhost", 6726) as client:
                # Filter for a specific sequence creation time
                qresponse = client.query(
                    QuerySequence().with_created_timestamp(time_start=Time.from_float(1765432100)),
                )

                # Inspect the response
                if qresponse is not None:
                    # Results are automatically grouped by Sequence for easier data management
                    for item in qresponse:
                        print(f"Sequence: {item.sequence.name}")
                        print(f"Topics: {[topic.name for topic in item.topics]}")
            ```
        """
        return self._created_timestamp

    @property
    def updated_timestamps(self) -> List[int]:
        """
        The UTC timestamps indicating when the entity was updated on the server.

        ### Querying with **Query Builders**
        The `updated_timestamps` property is not queryable.
        """
        return [s.created_timestamp for s in self._sessions]

    @property
    def sessions(self) -> List[Session]:
        """
        The list of sessions associated with this sequence.

        ### Querying with **Query Builders**
        The `sessions` property is not queryable.
        """
        return self._sessions

    @property
    def total_size_bytes(self) -> int:
        """
        The total physical storage footprint of the entity on the server in bytes.

        ### Querying with **Query Builders**
        The `total_size_bytes` property is not queryable.
        """
        return self._total_size_bytes
