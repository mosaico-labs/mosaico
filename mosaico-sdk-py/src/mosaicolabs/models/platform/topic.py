"""
Topic Catalog Entity.

This module defines the `Topic` class, which represents a read-only view of a
Topic's metadata in the platform catalog. It is used primarily for inspection
(listing topics) and query construction.
"""

from typing import Any, Optional

from pydantic import PrivateAttr
from ..query.generation.api import queryable
from ..query.generation.pydantic_mapper import PydanticFieldMapper
from ..query.expressions import _QueryTopicExpression

from .platform_base import PlatformBase


@queryable(
    mapper_type=PydanticFieldMapper,
    prefix="",
    query_expression_type=_QueryTopicExpression,
)
class Topic(PlatformBase):
    """
    Represents a read-only view of a server-side Topic platform resource.

    The `Topic` class provides access to topic-specific system metadata, such as the ontology tag (e.g., 'imu', 'camera') and the serialization format.
    It serves as a metadata-rich view of an individual data stream within the platform catalog.

    Important: Data Retrieval
        This class provides a **metadata-only** view of the topic.
        To retrieve the actual time-series messages contained within the topic, you must
        use the [`TopicHandler.get_data_streamer()`][mosaicolabs.handlers.TopicHandler.get_data_streamer]
        method from a [`TopicHandler`][mosaicolabs.handlers.TopicHandler]
        instance.

    ### Querying with the **`.Q` Proxy**
    The `user_metadata` field of this class is queryable when constructing a [`QueryTopic`][mosaicolabs.models.query.QueryTopic]
    via the **`.Q` proxy**.
    Check the documentation of the [`PlatformBase`][mosaicolabs.models.platform.platform_base.PlatformBase--querying-with-the-q-proxy] to construct a
    a valid expression for the builders involving the `user_metadata` component.

    Example:
        ```python
        from mosaicolabs import MosaicoClient, Topic, QueryTopic

        with MosaicoClient.connect("localhost", 6726) as client:
            # Filter for a specific data value (using constructor)
            qresponse = client.query(
                QueryTopic(
                    Topic.Q.user_metadata["update_rate_hz"].eq(100), # Access the keys using the [] operator
                    Topic.Q.user_metadata["interface.type"].eq("canbus"), # Navigate the nested dicts using the dot notation
                )
            )

            # # The same query using `with_expression`
            # qresponse = client.query(
            #     QueryTopic()
            #     .with_expression(Topic.Q.user_metadata["update_rate_hz"].eq(100))
            #     .with_expression(
            #         Topic.Q.user_metadata["interface.type"].match("canbus")
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

    # --- Private Fields (Internal State) ---
    _sequence_name: str = PrivateAttr()
    _ontology_tag: str = PrivateAttr()
    _serialization_format: str = PrivateAttr()
    _chunks_number: Optional[int] = PrivateAttr(default=None)

    # --- Factory Method ---
    @classmethod
    def _from_flight_info(
        cls, sequence_name: str, name: str, metadata: Any, sys_info: Any
    ) -> "Topic":
        """
        Internal factory method to construct a Topic model from Flight protocol objects.

        This method adapts low-level protocol responses into the high-level
        Catalog model.

        Args:
            sequence_name: The parent sequence identifier.
            name: The full resource name of the topic.
            metadata: Decoded topic metadata (properties and user metadata).
            sys_info: System diagnostic information.

        Returns:
            An initialized, read-only `Topic` model.
        """
        # Create the instance with public fields.
        # Note: metadata.user_metadata comes flat from the server; we unflatten it
        # to restore nested dictionary structures for the user.
        instance = cls(
            user_metadata=metadata.user_metadata,
        )

        # Set private attributes explicitly via the base helper
        instance._init_base_private(
            name=name,
            created_datetime=sys_info.created_datetime,
            is_locked=sys_info.is_locked,
            total_size_bytes=sys_info.total_size_bytes,
        )

        # Set local private attributes
        instance._sequence_name = sequence_name
        instance._ontology_tag = metadata.properties.ontology_tag
        instance._serialization_format = metadata.properties.serialization_format
        instance._chunks_number = sys_info.chunks_number

        return instance

    # --- Properties ---
    @property
    def ontology_tag(self) -> str:
        """
        The ontology type identifier (e.g., 'imu', 'gnss').

        This corresponds to the `__ontology_tag__` defined in the
        [`Serializable`][mosaicolabs.models.Serializable] class registry.

        ### Querying with **Query Builders**
        The `ontology_tag` property is queryable when constructing a [`QueryTopic`][mosaicolabs.models.query.QueryTopic]
        via the convenience method [`QueryTopic.with_ontology_tag()`][mosaicolabs.models.query.builders.QueryTopic.with_ontology_tag].

        Example:
            ```python
            from mosaicolabs import MosaicoClient, Topic, IMU, QueryTopic

            with MosaicoClient.connect("localhost", 6726) as client:
                # Filter for a specific data value (using constructor)
                qresponse = client.query(
                    QueryTopic().with_ontology_tag(IMU.ontology_tag()),
                )

                # Inspect the response
                if qresponse is not None:
                    # Results are automatically grouped by Sequence for easier data management
                    for item in qresponse:
                        print(f"Sequence: {item.sequence.name}")
                        print(f"Topics: {[topic.name for topic in item.topics]}")
            ```
        """
        return self._ontology_tag

    @property
    def sequence_name(self) -> str:
        """
        The name of the parent sequence containing this topic.

        ### Querying with **Query Builders**
        The `sequence_name` property is not queryable directly. Use [`QuerySequence`][mosaicolabs.models.query.QuerySequence] to query for sequences.

        Example:
            ```python
            from mosaicolabs import MosaicoClient, Topic, QuerySequence

            with MosaicoClient.connect("localhost", 6726) as client:
                # Filter for a specific data value (using constructor)
                qresponse = client.query(
                    QuerySequence().with_name("test_winter_20260129_103000")
                )

                # Inspect the response
                if qresponse is not None:
                    # Results are automatically grouped by Sequence for easier data management
                    for item in qresponse:
                        print(f"Sequence: {item.sequence.name}")
                        print(f"Topics: {[topic.name for topic in item.topics]}")
            ```
        """
        return self._sequence_name

    @property
    def chunks_number(self) -> Optional[int]:
        """
        The number of physical data chunks stored for this topic.

        May be `None` if the server did not provide detailed storage statistics.

        ### Querying with **Query Builders**
        The `chunks_number` property is not queryable.
        """
        return self._chunks_number

    @property
    def serialization_format(self) -> str:
        """
        The format used to serialize the topic data (e.g., 'arrow', 'image').

        This corresponds to the [`SerializationFormat`][mosaicolabs.enum.SerializationFormat] enum.

        ### Querying with **Query Builders**
        The `serialization_format` property is not queryable.
        """
        return self._serialization_format
