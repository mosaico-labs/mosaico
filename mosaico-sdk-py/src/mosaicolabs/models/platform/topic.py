"""
Topic Catalog Entity.

This module defines the `Topic` class, which represents a read-only view of a
Topic's metadata in the platform catalog. It is used primarily for inspection
(listing topics) and query construction.
"""

from typing import Any, Optional

from pydantic import PrivateAttr

from mosaicolabs.comm.metadata import PlatformMetadata, TopicMetadata
from mosaicolabs.comm.platform_resource_info import PlatformResourceInfo

from ..query.expressions import _QueryTopicExpression
from ..query.generation.api import queryable
from ..query.generation.pydantic_mapper import PydanticFieldMapper
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
    Warning: Deprecated
        Querying the topic user-custom metadata via the `user_metadata` field of this class is deprecated.
        Use the [`QueryTopic.with_user_metadata()`][mosaicolabs.models.query.builders.QueryTopic.with_user_metadata] builder instead.

    Example:
        ```python
        from mosaicolabs import MosaicoClient, Topic, QueryTopic

        with MosaicoClient.connect("localhost", 6726) as client:
            # Filter for a specific data value (using constructor)
            qresponse = client.query(
                QueryTopic(
                    Topic.Q.with_user_metadata("update_rate_hz", gt=100),
                    Topic.Q.with_user_metadata("interface.type", eq="canbus"), # Navigate the nested dicts using the dot notation
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

    # --- Private Fields (Internal State) ---
    _sequence_name: str = PrivateAttr()
    _ontology_tag: str = PrivateAttr()
    _serialization_format: str = PrivateAttr()
    _is_locked: bool = PrivateAttr(default=False)
    _chunks_number: Optional[int] = PrivateAttr(default=None)

    def _init_from_flight_info(
        self,
        metadata: PlatformMetadata,
        resrc_info: PlatformResourceInfo,
        **kwargs: Any,
    ) -> None:
        """
        Initialize the Topic instance from flight information.

        Args:
            metadata: The platform schema metadata of the topic.
            resrc_info: The system information of the topic.
            **kwargs: Additional keyword arguments.
                - `sequence_name`: The name of the parent sequence.
        """
        if not isinstance(metadata, TopicMetadata):
            raise ValueError(
                "Metadata must be an instance of `mosaicolabs.comm.TopicMetadata`."
            )

        sequence_name = kwargs.get("sequence_name")
        if sequence_name is None:
            raise ValueError("Sequence name must be provided to initialize a Topic.")

        if resrc_info.is_locked is None:
            raise ValueError("`is_locked` must be provided to initialize a Topic.")
        self._sequence_name = sequence_name
        self._ontology_tag = metadata.properties.ontology_tag
        self._serialization_format = metadata.properties.serialization_format
        self._chunks_number = resrc_info.chunks_number
        self._is_locked = resrc_info.is_locked

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

    @property
    def is_locked(self) -> bool:
        """
        Indicates if the topic resource is locked on the server.

        A locked state typically occurs after data writing is completed,
        preventing structural modifications.

        ### Querying with **Query Builders**
        The `is_locked` property is not queryable.
        """
        return self._is_locked
