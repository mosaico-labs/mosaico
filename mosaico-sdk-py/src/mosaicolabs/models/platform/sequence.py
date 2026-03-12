"""
Sequence Catalog Entity.

This module defines the `Sequence` class, which represents a read-only view of a
Sequence's metadata. A Sequence is a logical grouping of multiple Topics.
"""

from typing import Any, List

from pydantic import PrivateAttr

from mosaicolabs.comm.metadata import PlatformMetadata
from mosaicolabs.comm.platform_resource_info import PlatformResourceInfo

from ..query.expressions import _QuerySequenceExpression
from ..query.generation.api import queryable
from ..query.generation.pydantic_mapper import PydanticFieldMapper
from .platform_base import PlatformBase


@queryable(
    mapper_type=PydanticFieldMapper,
    prefix="",
    query_expression_type=_QuerySequenceExpression,
)
class Sequence(PlatformBase):
    """
    Represents a read-only view of a server-side Sequence platform resource.

    The `Sequence` class is designed to hold system-level metadata and enable fluid querying of
    user-defined properties. It serves as the primary metadata container
    for a logical grouping of related topics.

    Important: Data Retrieval
        This class provides a **metadata-only** view of the sequence.
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
                QuerySequence(
                    Sequence.Q.with_user_metadata("project", eq="Apollo"),
                    Sequence.Q.with_user_metadata("vehicle.software_stack.planning", eq="plan-4.1.7"),
                )
            )

            # Inspect the response
            if qresponse is not None:
                # Results are automatically grouped by Sequence for easier data management
                for item in qresponse:
                    print(f"Sequence: {item.sequence.name}")
                    print(f"Topics: {[topic.name for topic in item.topics]}")
        ```
    """

    # --- Private Fields ---
    _topics: List[str] = PrivateAttr(default_factory=list)

    def _init_from_flight_info(
        self,
        metadata: PlatformMetadata,
        resrc_info: PlatformResourceInfo,
        **kwargs: Any,
    ) -> None:
        """
        Overridden factory for Sequence entities.

        Args:
            name: The name of the sequence.
            metadata: UNUSED.
            resrc_info: UNUSED.
            **kwargs: Keyword arguments containing the following keys:
                - `topics`: The list of topic names.
        """
        # Check for topics in kwargs
        topics = kwargs.get("topics")
        if topics is None:
            raise ValueError("Topics must be provided to initialize a Sequence.")

        # Populate Sequence-specific private attributes
        self._topics = topics

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
        return self._topics
