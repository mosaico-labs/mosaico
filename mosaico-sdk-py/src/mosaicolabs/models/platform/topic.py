"""
Topic Catalog Entity.

This module defines the `Topic` class, which represents a read-only view of a
Topic's metadata in the platform catalog. It is used primarily for inspection
(listing topics) and query construction.
"""

from typing import Any, Dict, Optional

import pydantic
from pydantic import PrivateAttr
from typing_extensions import Self

from mosaicolabs.platform.metadata import TopicMetadata
from mosaicolabs.platform.resource_manifests import TopicResourceManifest

from ..query.expressions import _QueryTopicExpression
from ..query.generation.api import _QueryProxyMixin, queryable
from ..query.generation.pydantic_mapper import PydanticFieldMapper


@queryable(
    mapper_type=PydanticFieldMapper,
    prefix="",
    query_expression_type=_QueryTopicExpression,
)
class Topic(pydantic.BaseModel, _QueryProxyMixin):
    """
    Represents a read-only view of a server-side Topic platform resource.

    The `Topic` class provides access to topic-specific system metadata, such as the ontology tag (e.g., 'imu', 'camera') and the serialization format.
    It serves as a metadata-rich view of an individual data stream within the platform catalog.

    Important: Data Retrieval
        This class provides a server-side **metadata-only** view of the topic.
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
                QueryTopic()
                .with_user_metadata("update_rate_hz", gt=100)
                .with_user_metadata("interface.type", eq="canbus")
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
        Querying the topic user-custom metadata via the `user_metadata` field of this class is deprecated.
        Use the [`QueryTopic.with_user_metadata()`][mosaicolabs.models.query.builders.QueryTopic.with_user_metadata] builder instead.
    """

    # --- Private Fields (Internal State) ---
    _name: str = PrivateAttr()
    _sequence_name: str = PrivateAttr()
    _total_size_bytes: int = PrivateAttr()
    _created_timestamp: int = PrivateAttr()
    _ontology_tag: str = PrivateAttr()
    _serialization_format: str = PrivateAttr()
    _locked: bool = PrivateAttr(default=False)
    _chunks_number: Optional[int] = PrivateAttr(default=None)

    @classmethod
    def _from_resource_info(
        cls,
        name: str,
        sequence_name: str,
        platform_metadata: TopicMetadata,
        resrc_manifest: TopicResourceManifest,
    ) -> Self:
        """
        Factory method to create a Topic view from platform resource information.

        Args:
            name: The name of the platform resource.
            sequence_name: The name of the sequence the topic belongs to.
            platform_metadata: The metadata of the platform resource.
            resrc_manifest: The manifest of the platform resource.

        Returns:
            A Topic instance.
        """
        if not isinstance(platform_metadata, TopicMetadata):
            raise ValueError(
                "Metadata must be an instance of `mosaicolabs.comm.TopicMetadata`."
            )
        user_metadata = getattr(platform_metadata, "user_metadata", None)
        if user_metadata is None:
            raise ValueError("Metadata must have a `user_metadata` attribute.")

        instance = cls(user_metadata=user_metadata)

        # Initialize shared private attrs
        instance._init_base_private(
            name=name,
            sequence_name=sequence_name,
            platform_metadata=platform_metadata,
            resrc_manifest=resrc_manifest,
        )

        return instance

    def _init_base_private(
        self,
        *,
        name: str,
        sequence_name: str,
        platform_metadata: TopicMetadata,
        resrc_manifest: TopicResourceManifest,
    ) -> None:
        """
        Internal helper to populate system-controlled private attributes.

        This is used by factory methods (`_from_resource_info`) to set attributes
        that are strictly read-only for the user.

        Args:
            name: The unique resource name.
            sequence_name: The name of the sequence the topic belongs to.
            platform_metadata: The metadata of the platform resource.
            resrc_manifest: The manifest of the platform resource.
        """
        self._total_size_bytes = resrc_manifest.resource_info.total_size_bytes
        self._created_timestamp = resrc_manifest.created_timestamp
        self._name = name
        self._sequence_name = sequence_name
        self._ontology_tag = platform_metadata.properties.ontology_tag
        self._serialization_format = (
            platform_metadata.properties.serialization_format.value
        )
        self._chunks_number = resrc_manifest.resource_info.chunks_number
        self._locked = resrc_manifest.locked

    # --- Properties ---

    @property
    def name(self) -> str:
        """
        The unique identifier or resource name of the entity.

        ### Querying with **Query Builders**
        The `name` property is queryable when constructing a [`QueryTopic`][mosaicolabs.models.query.QueryTopic]
        via the convenience methods:

        * [`QueryTopic.with_name()`][mosaicolabs.models.query.builders.QueryTopic.with_name]
        * [`QueryTopic.with_name_match()`][mosaicolabs.models.query.builders.QueryTopic.with_name_match]

        Example:
            ```python
            from mosaicolabs import MosaicoClient, QueryTopic

            with MosaicoClient.connect("localhost", 6726) as client:
                # Filter for a specific data value (using constructor)
                qresponse = client.query(
                    QueryTopic().with_name("/front/imu"),
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
        The `created_timestamp` property is queryable when constructing a [`QueryTopic`][mosaicolabs.models.query.QueryTopic]
        via the convenience method:

        * [`QueryTopic.with_created_timestamp()`][mosaicolabs.models.query.builders.QueryTopic.with_created_timestamp]

        Example:
            ```python
            from mosaicolabs import MosaicoClient, Topic, IMU, QueryTopic, Time

            with MosaicoClient.connect("localhost", 6726) as client:
                # Filter for a specific topic creation time
                qresponse = client.query(
                    QueryTopic().with_created_timestamp(time_start=Time.from_float(1765432100)),
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
    def locked(self) -> bool:
        """
        Indicates if the topic resource is locked on the server.

        A locked state typically occurs after data writing is completed,
        preventing structural modifications.

        ### Querying with **Query Builders**
        The `locked` property is not queryable.
        """
        return self._locked

    @property
    def total_size_bytes(self) -> int:
        """
        The total physical storage footprint of the entity on the server in bytes.

        ### Querying with **Query Builders**
        The `total_size_bytes` property is not queryable.
        """
        return self._total_size_bytes
