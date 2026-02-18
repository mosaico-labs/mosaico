"""
Platform Entity Base Module.

This module defines `PlatformBase`, the foundational class for the main catalog entities,
Sequences and Topics, within the SDK. It consolidates shared system attributes
(like creation time, locks, and size) and integrates with the Pydantic validation
system and the internal Query API.
"""

import datetime
from typing import Any, Dict

from pydantic import PrivateAttr
import pydantic
from ..query.generation.api import _QueryProxyMixin


class PlatformBase(pydantic.BaseModel, _QueryProxyMixin):
    """
    Base class for Mosaico Sequence and Topic entities.

    The `PlatformBase` serves as a read-only view of a server-side resource.
    It is designed to hold system-level metadata and enable fluid querying of
    user-defined properties.


    ### Core Functionality
    1.  **System Metadata**: Consolidates attributes like storage size and locking
        status that are common across the catalog.
    2.  **Query Interface**: Inherits from internal `_QueryableModel` to support expressive
        syntax for filtering resources (e.g., `Sequence.Q.user_metadata["env"] == "prod"`).

    Note: Read-Only Entities
        Instances of this class are factory-generated from server responses.
        Users should not instantiate this class directly.

    Attributes:
        user_metadata: A dictionary of custom key-value pairs assigned by the user.

    ### Querying with the **`.Q` Proxy**
    The `user_metadata` field is queryable when constructing a [`QueryTopic`][mosaicolabs.models.query.builders.QueryTopic]
    or [`QuerySequence`][mosaicolabs.models.query.builders.QuerySequence] via the **`.Q` proxy**.

    | Field Access Path | Queryable Type | Supported Operators |
    | :--- | :--- | :--- |
    | `<PlatformModel>.Q.user_metadata["key"]` | `String`, `Numeric`, `Boolean` | `.eq()`, `.neq()`, `.lt()`, `.gt()`, `.leq()`, `.geq()`, `.in_()`, `.between()`, `.match()` |

    Note: Universal Compatibility
        The `<PlatformModel>` placeholder represents any Mosaico class derived by
        [`PlatformBase`][mosaicolabs.models.platform.platform_base.PlatformBase]
        (i.e. [`Topic`][mosaicolabs.models.platform.Topic], [`Sequence`][mosaicolabs.models.platform.Sequence])

    Example:
        ```python
        from mosaicolabs import MosaicoClient, Topic, Sequence, QueryTopic, QuerySequence

        with MosaicoClient.connect("localhost", 6726) as client:
            # Filter for a specific metadata key value.
            qresponse = client.query(
                QueryTopic(Topic.Q.user_metadata["update_rate_hz"].geq(100))
            )
            # Filter for a specific nested metadata key value.
            qresponse = client.query(
                QuerySequence(Sequence.Q.user_metadata["project.version"].match("v1.0"))
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
    The `user_metadata` field is queryable when constructing a [`QueryTopic`][mosaicolabs.models.query.builders.QueryTopic]
    or [`QuerySequence`][mosaicolabs.models.query.builders.QuerySequence] using the **`.Q` proxy**

    | Field Access Path | Queryable Type | Supported Operators |
    | :--- | :--- | :--- |
    | `Sequence.Q.user_metadata["key"]` | `String`, `Numeric`, `Boolean` | `.eq()`, `.neq()`, `.lt()`, `.gt()`, `.leq()`, `.geq()`, `.in_()`, `.between()`, `.match()` |
    | `Sequence.Q.user_metadata["key.subkey.subsubkey..."]` | `String`, `Numeric`, `Boolean` | `.eq()`, `.neq()`, `.lt()`, `.gt()`, `.leq()`, `.geq()`, `.in_()`, `.between()`, `.match()` |
    | `Topic.Q.user_metadata["key"]` | `String`, `Numeric`, `Boolean` | `.eq()`, `.neq()`, `.lt()`, `.gt()`, `.leq()`, `.geq()`, `.in_()`, `.between()`, `.match()` |
    | `Topic.Q.user_metadata["key.subkey.subsubkey..."]` | `String`, `Numeric`, `Boolean` | `.eq()`, `.neq()`, `.lt()`, `.gt()`, `.leq()`, `.geq()`, `.in_()`, `.between()`, `.match()` |

    Example:
        ```python
        from mosaicolabs import MosaicoClient, Topic, Sequence, QueryTopic, QuerySequence

        with MosaicoClient.connect("localhost", 6726) as client:
            # Filter for a specific keys in sequence AND topic metadata.
            qresponse = client.query(
                QueryTopic(Topic.Q.user_metadata["update_rate_hz"].geq(100)),
                QuerySequence(Sequence.Q.user_metadata["project.version"].match("v1.0"))
            )

            # Inspect the response
            if qresponse is not None:
                # Results are automatically grouped by Sequence for easier data management
                for item in qresponse:
                    print(f"Sequence: {item.sequence.name}")
                    print(f"Topics: {[topic.name for topic in item.topics]}")
        ```
    """

    # --- Private Attributes ---
    # These fields are managed internally and populated via _init_base_private.
    # They are excluded from the standard Pydantic __init__ to prevent users
    # from manually setting system-controlled values.
    _is_locked: bool = PrivateAttr(default=False)
    _total_size_bytes: int = PrivateAttr()
    _created_datetime: datetime.datetime = PrivateAttr()
    _name: str = PrivateAttr()

    def _init_base_private(
        self,
        *,
        name: str,
        total_size_bytes: int,
        created_datetime: datetime.datetime,
        is_locked: bool = False,
    ) -> None:
        """
        Internal helper to populate system-controlled private attributes.

        This is used by factory methods (`_from_flight_info`) to set attributes
        that are strictly read-only for the user.

        Args:
            name: The unique resource name.
            total_size_bytes: The storage size on the server.
            created_datetime: The UTC timestamp of creation.
            is_locked: Whether the resource is currently locked (e.g., during writing).
        """
        self._is_locked = is_locked
        self._total_size_bytes = total_size_bytes
        self._created_datetime = created_datetime or datetime.datetime.now(
            datetime.timezone.utc
        )
        self._name = name or ""

    # --- Shared Properties ---
    @property
    def name(self) -> str:
        """
        The unique identifier or resource name of the entity.

        ### Querying with **Query Builders**
        The `name` property is queryable when constructing a [`QueryTopic`][mosaicolabs.models.query.QueryTopic]
        or a [`QuerySequence`][mosaicolabs.models.query.QuerySequence]
        via the convenience methods:

        * [`QueryTopic.with_name()`][mosaicolabs.models.query.builders.QueryTopic.with_name]
        * [`QueryTopic.with_name_match()`][mosaicolabs.models.query.builders.QueryTopic.with_name_match]
        * [`QuerySequence.with_name()`][mosaicolabs.models.query.builders.QuerySequence.with_name]
        * [`QuerySequence.with_name_match()`][mosaicolabs.models.query.builders.QuerySequence.with_name_match]

        Example:
            ```python
            from mosaicolabs import MosaicoClient, Topic, IMU, QueryTopic

            with MosaicoClient.connect("localhost", 6726) as client:
                # Filter for a specific data value (using constructor)
                qresponse = client.query(
                    QueryTopic().with_name("/front/imu"),
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
    def created_datetime(self) -> datetime.datetime:
        """
        The UTC timestamp indicating when the entity was created on the server.

        ### Querying with **Query Builders**
        The `created_datetime` property is queryable when constructing a [`QueryTopic`][mosaicolabs.models.query.QueryTopic]
        or a [`QuerySequence`][mosaicolabs.models.query.QuerySequence]
        via the convenience methods:

        * [`QueryTopic.with_created_timestamp()`][mosaicolabs.models.query.builders.QueryTopic.with_created_timestamp]
        * [`QuerySequence.with_created_timestamp()`][mosaicolabs.models.query.builders.QuerySequence.with_created_timestamp]

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
        return self._created_datetime

    @property
    def is_locked(self) -> bool:
        """
        Indicates if the resource is currently locked.

        A locked state typically occurs during active writing or maintenance operations,
        preventing deletion or structural modifications.

        ### Querying with **Query Builders**
        The `is_locked` property is not queryable.
        """
        return self._is_locked

    @property
    def total_size_bytes(self) -> int:
        """
        The total physical storage footprint of the entity on the server in bytes.

        ### Querying with **Query Builders**
        The `total_size_bytes` property is not queryable.
        """
        return self._total_size_bytes
