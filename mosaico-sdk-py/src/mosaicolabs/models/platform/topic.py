"""
Topic Catalog Entity.

This module defines the `Topic` class, which represents a read-only view of a
Topic's metadata in the platform catalog. It is used primarily for inspection
(listing topics) and query construction.
"""

from dataclasses import dataclass
from typing import Any, Dict, Optional

from typing_extensions import Self

from mosaicolabs.platform.metadata import TopicMetadata
from mosaicolabs.platform.resource_manifests import TopicResourceManifest


@dataclass(frozen=True)
class Topic:
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

    ### Querying with **Query Builders**
    Querying Topic specific attributes (like `user_metadata` or `name`) can be made using the
    [QueryTopic()][mosaicolabs.models.query.builders.QueryTopic] query builder.

    Example:
        ```python
        from mosaicolabs import MosaicoClient, QueryTopic

        with MosaicoClient.connect("localhost", 6726) as client:
            # Filter for a specific data value (using constructor)
            qresponse = client.query(
                QueryTopic()
                .with_name_match("/sensors/imu") # (1)!
                .with_user_metadata("update_rate_hz", gt=100) # (2)!
                .with_user_metadata("interface.type", eq="canbus")
            )

            # Inspect the response
            if qresponse is not None:
                # Results are automatically grouped by Sequence for easier data management
                for item in qresponse:
                    print(f"Sequence: {item.sequence.name}")
                    print(f"Topics: {[topic.name for topic in item.topics]}")
        ```

        1. Find all the topics which name matches the pattern
        2. Query the (key, value) in the `user_metadata` JSON
    """

    user_metadata: Dict[str, Any]
    """
    Custom user-defined key-value pairs associated with the entity.

    ### Querying with **Query Builders**
    Querying the `user_metadata` attribute can be made using the
    [`QueryTopic.with_user_metadata()`][mosaicolabs.models.query.builders.QueryTopic.with_user_metadata] query builder.

    Example:
        ```python
        from mosaicolabs import MosaicoClient, QueryTopic

        with MosaicoClient.connect("localhost", 6726) as client:
            # Filter for a specific data value (using constructor)
            qresponse = client.query(
                QueryTopic()
                .with_user_metadata("update_rate_hz", gt=100) # (1)!
                .with_user_metadata("interface.type", eq="canbus")
            )

            # Inspect the response
            if qresponse is not None:
                # Results are automatically grouped by Sequence for easier data management
                for item in qresponse:
                    print(f"Sequence: {item.sequence.name}")
                    print(f"Topics: {[topic.name for topic in item.topics]}")
        ```

        1. Query the (key, value) in the `user_metadata` JSON
    """

    name: str
    """
    The unique identifier or resource name of the entity.

    ### Querying with **Query Builders**
    The `name` attribute is queryable when constructing a [`QueryTopic`][mosaicolabs.models.query.QueryTopic]
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

    sequence_name: str
    """
    The name of the parent sequence containing this topic.

    ### Querying with **Query Builders**
    The `sequence_name` attribute is queryable queryable when constructing a [`QuerySequence`][mosaicolabs.models.query.QuerySequence]
    via the convenience methods:

    * [`QuerySequence.with_name()`][mosaicolabs.models.query.builders.QuerySequence.with_name]
    * [`QuerySequence.with_name_match()`][mosaicolabs.models.query.builders.QuerySequence.with_name_match]

    Example:
        ```python
        from mosaicolabs import MosaicoClient, Topic, QuerySequence

        with MosaicoClient.connect("localhost", 6726) as client:
            # Filter for a specific data value (using constructor)
            qresponse = client.query(
                QuerySequence().with_name_match("test_winter_2026")
            )

            # Inspect the response
            if qresponse is not None:
                # Results are automatically grouped by Sequence for easier data management
                for item in qresponse:
                    print(f"Sequence: {item.sequence.name}")
                    print(f"Topics: {[topic.name for topic in item.topics]}")
        ```
    """

    created_timestamp: int
    """
    The UTC timestamp indicating when the entity was created on the server.

    ### Querying with **Query Builders**
    The `created_timestamp` attribute is queryable when constructing a [`QueryTopic`][mosaicolabs.models.query.QueryTopic]
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

    ontology_tag: str
    """
    The ontology type identifier (e.g., 'imu', 'gnss').

    This corresponds to the `__ontology_tag__` defined in the
    [`Serializable`][mosaicolabs.models.Serializable] class registry.

    ### Querying with **Query Builders**
    The `ontology_tag` attribute is queryable when constructing a [`QueryTopic`][mosaicolabs.models.query.QueryTopic]
    via the convenience method:
    
    * [`QueryTopic.with_ontology_tag()`][mosaicolabs.models.query.builders.QueryTopic.with_ontology_tag].

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

    chunks_number: Optional[int]
    """
    The number of physical data chunks stored for this topic.

    May be `None` if the server did not provide detailed storage statistics.

    ### Querying with **Query Builders**
    The `chunks_number` attribute is not queryable.
    """

    serialization_format: str
    """
    The format used to serialize the topic data (e.g., 'arrow', 'image').

    This corresponds to the [`SerializationFormat`][mosaicolabs.enum.SerializationFormat] enum.

    ### Querying with **Query Builders**
    The `serialization_format` attribute is not queryable.
    """

    locked: bool
    """
    Indicates if the topic resource is locked on the server.

    A locked state typically occurs after data writing is completed,
    preventing structural modifications.

    ### Querying with **Query Builders**
    The `locked` attribute is not queryable.
    """

    total_size_bytes: int
    """
    The total physical storage footprint of the entity on the server in bytes.

    ### Querying with **Query Builders**
    The `total_size_bytes` attribute is not queryable.
    """

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

        return cls(
            user_metadata=user_metadata,
            name=name,
            sequence_name=sequence_name,
            total_size_bytes=resrc_manifest.total_size_bytes,
            created_timestamp=resrc_manifest.created_timestamp,
            ontology_tag=platform_metadata.properties.ontology_tag,
            serialization_format=platform_metadata.properties.serialization_format.value,
            chunks_number=resrc_manifest.chunks_number,
            locked=resrc_manifest.locked,
        )
