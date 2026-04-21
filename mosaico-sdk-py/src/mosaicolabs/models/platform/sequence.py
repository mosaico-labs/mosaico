"""
Sequence Catalog Entity.

This module defines the `Sequence` class, which represents a read-only view of a
Sequence's platform_metadata. A Sequence is a logical grouping of multiple Topics.
"""

from dataclasses import dataclass
from typing import Any, Dict, List

from typing_extensions import Self

from mosaicolabs.platform.metadata import SequenceMetadata
from mosaicolabs.platform.resource_manifests import (
    SequenceResourceManifest,
)

from .session import Session


@dataclass(frozen=True)
class Sequence:
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

    ### Querying with **Query Builders**
    Querying Sequence specific attributes (like `user_metadata` or `name`) can be made using the
    [QuerySequence()][mosaicolabs.models.query.builders.QuerySequence] query builder.

    Example:
        ```python
        from mosaicolabs import MosaicoClient, QuerySequence

        with MosaicoClient.connect("localhost", 6726) as client:
            # Filter for a specific data value (using constructor)
            qresponse = client.query(
                QuerySequence()
                .with_name_match("test_winter_") # (1)!
                .with_user_metadata("project", eq="Apollo") # (2)!
                .with_user_metadata("vehicle.software_stack.planning", eq="plan-4.1.7")
            )

            # Inspect the response
            if qresponse is not None:
                # Results are automatically grouped by Sequence for easier data management
                for item in qresponse:
                    print(f"Sequence: {item.sequence.name}")
                    print(f"Topics: {[topic.name for topic in item.topics]}")
        ```

        1. Find all the sequences which name matches the pattern
        2. Query the (key, value) in the `user_metadata` JSON
    """

    user_metadata: Dict[str, Any]
    """
    Custom user-defined key-value pairs associated with the entity.

    ### Querying with **Query Builders**
    The `user_metadata` attribute is queryable when constructing a [`QuerySequence`][mosaicolabs.models.query.QuerySequence]
    via the convenience method:

    * [`QuerySequence.with_user_metadata()`][mosaicolabs.models.query.builders.QuerySequence.with_user_metadata]

    Example:
        ```python
        from mosaicolabs import MosaicoClient, Sequence, QuerySequence

        with MosaicoClient.connect("localhost", 6726) as client:
            # Filter for a specific data value (using constructor)
            qresponse = client.query(
                QuerySequence()
                .with_user_metadata("project", eq="Apollo") # (1)!
                .with_user_metadata("vehicle.software_stack.planning", eq="plan-4.1.7")
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

    created_timestamp: int
    """
    The UTC timestamp when the sequence was created.

    ### Querying with **Query Builders**
        The `created_timestamp` attribute is queryable when constructing a [`QuerySequence`][mosaicolabs.models.query.QuerySequence]
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

    name: str
    """
    The name of the sequence.

    ### Querying with **Query Builders**
        The `name` attribute is queryable when constructing a [`QuerySequence`][mosaicolabs.models.query.QuerySequence]
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

    total_size_bytes: int
    """
    The aggregated total size of the sequence in bytes
    
    ### Querying with **Query Builders**
        The `total_size_bytes` attribute is not queryable.
    """

    sessions: List[Session]
    """
    The list of sessions in the sequence
    
    ### Querying with **Query Builders**
        The `sessions` attribute is not queryable.
        
    """

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

        return cls(
            user_metadata=user_metadata,
            name=name,
            total_size_bytes=total_size_bytes,
            created_timestamp=resrc_manifest.created_timestamp,
            sessions=[
                Session._from_resource_manifest(s) for s in resrc_manifest.sessions
            ],
        )

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
        The `topics` property is queryable via the [`QueryTopic`][mosaicolabs.models.query.QueryTopic] builder,
        through the convenience methods:

        * [`QueryTopic.with_name()`][mosaicolabs.models.query.builders.QueryTopic.with_name]
        * [`QueryTopic.with_name_match()`][mosaicolabs.models.query.builders.QueryTopic.with_name_match]

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
        return [t for s in self.sessions for t in s.topics]

    @property
    def updated_timestamps(self) -> List[int]:
        """
        The UTC timestamps indicating when the entity was updated on the server.

        ### Querying with **Query Builders**
        The `updated_timestamps` property is not queryable.
        """
        return [s.created_timestamp for s in self.sessions]
