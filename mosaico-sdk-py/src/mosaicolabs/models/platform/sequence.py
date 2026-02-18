"""
Sequence Catalog Entity.

This module defines the `Sequence` class, which represents a read-only view of a
Sequence's metadata. A Sequence is a logical grouping of multiple Topics.
"""

from typing import Any, List
from pydantic import PrivateAttr


from ..query.generation.api import queryable
from ..query.generation.pydantic_mapper import PydanticFieldMapper
from ..query.expressions import _QuerySequenceExpression

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
    The `user_metadata` field of this class is queryable when constructing a [`QuerySequence`][mosaicolabs.models.query.QuerySequence]
    via the **`.Q` proxy**.
    Check the documentation of the [`PlatformBase`][mosaicolabs.models.platform.platform_base.PlatformBase--querying-with-the-q-proxy] to construct a
    a valid expression for the builders involving the `user_metadata` component.

    Example:
        ```python
        from mosaicolabs import MosaicoClient, Sequence, QuerySequence

        with MosaicoClient.connect("localhost", 6726) as client:
            # Filter for a specific data value (using constructor)
            qresponse = client.query(
                QuerySequence(
                    Sequence.Q.user_metadata["project"].eq("Apollo"), # Access the keys using the [] operator
                    Sequence.Q.user_metadata["vehicle.software_stack.planning"].match("plan-4."), # Navigate the nested dicts using the dot notation
                )
            )

            # # The same query using `with_expression`
            # qresponse = client.query(
            #     QuerySequence()
            #     .with_expression(Sequence.Q.user_metadata["project"].eq("Apollo"))
            #     .with_expression(
            #         Sequence.Q.user_metadata["vehicle.software_stack.planning"].match("plan-4.")
            #     )
            # )

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

    # --- Factory Method ---
    @classmethod
    def _from_flight_info(
        cls, name: str, metadata: Any, sys_info: Any, topics: List[str]
    ) -> "Sequence":
        """
        Internal factory method to construct a Sequence model from Flight protocol objects.

        Args:
            name: The unique name of the sequence.
            metadata: Decoded sequence metadata containing user properties.
            sys_info: System diagnostic information (size, lock status, dates).
            topics: A list of string names for all topics contained in the sequence.

        Returns:
            A read-only `Sequence` model instance.
        """
        instance = cls(
            user_metadata=metadata.user_metadata,
        )

        # Set private attributes explicitly
        instance._init_base_private(
            name=name,
            created_datetime=sys_info.created_datetime,
            is_locked=sys_info.is_locked,
            total_size_bytes=sys_info.total_size_bytes,
        )

        # Set local private attributes
        instance._topics = topics
        return instance

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
