"""
This module provides the high-level "Fluent" API for constructing complex searches across the Mosaico Data Platform.

It implements a Domain-Specific Language that allows users to filter **Sequences**, **Topics**, and **Ontology** data using a type-safe, method-chaining interface.


**Key Components:**

* [**`Query`**][mosaicolabs.models.query.builders.Query]: The root container that aggregates multiple specialized sub-queries.
* [**`QueryOntologyCatalog`**][mosaicolabs.models.query.builders.QueryOntologyCatalog]: For fine-grained filtering based on sensor-specific field values (e.g., `IMU.Q.acceleration.x > 9.8`).
* [**`QueryTopic`**][mosaicolabs.models.query.builders.QueryTopic]: Specifically for filtering topic-level metadata.
* [**`QuerySequence`**][mosaicolabs.models.query.builders.QuerySequence]: Specifically for filtering sequence-level metadata.
"""

from typing import Any, Dict, List, Optional, Tuple, Type, get_origin

# Import custom types used in helper methods
from mosaicolabs.models import Time
from .protocols import QueryableProtocol

# Import the building blocks for expressions and how they are combined
from .expressions import (
    _QueryCatalogExpression,
    _QueryTopicExpression,
    _QuerySequenceExpression,
    _QueryExpression,
)


def _get_tag_from_expr_key(key: str):
    fields = key.split(".")
    if not len(fields) > 1:
        raise ValueError(f"expected 'ontology_tag.field0.field1... in key, got '{key}'")
    return fields[0]


def _validate_expression_unique_key(
    stored_exprs: List["_QueryExpression"], new_key: str
):
    """
    Private helper to validate a single expression against the
    class's key type.

    Raises a dynamic NotImplementedError if the key is already present.
    """
    if any(e.key == new_key for e in stored_exprs):
        raise NotImplementedError(
            f"Query builder already contains the key '{new_key}'. The current implementation allows a key can appear only once per query."
        )


def _validate_expression_type(
    expr: "_QueryExpression", expected_types: Tuple[Type[_QueryExpression], ...]
):
    """
    Private helper to validate a single expression against the
    class's __supported_query_expressions__ type.

    Raises a dynamic TypeError if the type is incorrect.
    """
    # Get the type this class supports

    if not isinstance(expr, expected_types):
        # Dynamically get the names of the types for the error message
        found_type = type(expr).__name__
        expected_names = [expct.__name__ for expct in expected_types]

        raise TypeError(
            f"Invalid expression type. Expected {expected_names}, but got '{found_type}'."
        )


def _validate_expression_operator_format(expr: "_QueryExpression"):
    """
    Private helper to validate a single expression against the
    class's __supported_query_expressions__ type.

    Raises a dynamic TypeError if the type is incorrect.
    """
    # Get the type this class supports

    if not expr.op.startswith("$"):
        raise ValueError(
            f"Invalid expression operator '{expr.op}': must start with '$'."
        )


# --- Logical Combinators --


class _QueryCombinator:
    """
    Merges multiple `_QueryExpression` instances into a single cohesive query block.

    The combinator is responsible for flattening individual expressions into a
    single dictionary suitable for the Mosaico API. Currently,
    all expressions combined in a single builder are treated with an implicit **AND** logic.
    """

    def __init__(
        self,
        expressions: List[_QueryExpression],
        # op: str = "$and",
    ):
        """
        Initializes the logical combinator.

        Args:
            expressions: A list of atomic expressions to merge.
        """
        # self.op = op
        self.expressions = expressions

    def to_dict(self) -> Dict[str, Any]:
        """
        Converts the logical group into a merged dictionary format.

        Example:
            `{"imu.acceleration.x": {"$gt": 5}, "imu.acceleration.y": {"$lt": 10}}`
        """
        if not self.expressions:
            return {}
        # return {self.op: [expr.to_dict() for expr in self.expressions]}
        return {
            key: val for expr in self.expressions for key, val in expr.to_dict().items()
        }


# --- Query Builders --


class QueryOntologyCatalog:
    """
    A top-level query object for the Data Catalog that combines multiple sensor-field expressions.

    This builder allows for fine-grained filtering based on the actual values contained within sensor payloads
    (e.g., IMU acceleration, GPS coordinates, or custom telemetry).
    It produces a "flat" dictionary output where field paths utilize dot-notation (e.g., `"imu.acceleration.x"`).

    This class is designed to work with the **`.Q` query proxy** injected into every
    [`Serializable`][mosaicolabs.models.Serializable] data ontology model.
    You can use this proxy on any registered sensor class (like [`IMU`][mosaicolabs.models.sensors.IMU],
    [`Vector3d`][mosaicolabs.models.data.geometry.Vector3d],
    [`Point3d`][mosaicolabs.models.data.geometry.Point3d]), etc.
    to create type-safe expressions.

    Example:
        ```python
        from mosaicolabs import MosaicoClient, Topic, QuerySequence

        with MosaicoClient.connect("localhost", 6726) as client:
            # Filter for a specific data value (using constructor)
            qresponse = client.query(
                QueryOntologyCatalog(IMU.Q.acceleration.x.lt(-4.0)) # Using constructor
                .with_expression(IMU.Q.acceleration.y.gt(5.0)) # Using with_expression
            )

            # Inspect the response
            if qresponse is not None:
                # Results are automatically grouped by Sequence for easier data management
                for item in qresponse:
                    print(f"Sequence: {item.sequence.name}")
                    print(f"Topics: {[topic.name for topic in item.topics]}")

            # Filter for a specific component value and extract the first and last occurrence times
            qresponse = client.query(
                QueryOntologyCatalog(IMU.Q.acceleration.x.lt(-4.0), include_timestamp_range=True)
                .with_expression(IMU.Q.acceleration.y.gt(5.0))
            )

            # Inspect the response
            if qresponse is not None:
                # Results are automatically grouped by Sequence for easier data management
                for item in qresponse:
                    print(f"Sequence: {item.sequence.name}")
                    print(f"Topics: {{topic.name:
                                [topic.timestamp_range.start, topic.timestamp_range.end]
                                for topic in item.topics}}")
        ```
    """

    __supported_query_expressions__: Tuple[Type[_QueryExpression], ...] = (
        _QueryCatalogExpression,
    )

    def __init__(
        self,
        *expressions: "_QueryExpression",
        include_timestamp_range: Optional[bool] = None,
    ):
        """
        The constructor initializes the query with an optional list of
        [`_QueryCatalogExpression`][mosaicolabs.models.query.expressions._QueryCatalogExpression] objects, generated
        via `<Model>.Q.` proxy, where model is any of the available data ontology (e.g. IMU.Q, GPS.Q, String.Q, etc.)

        Args:
            *expressions: A variable number of expressions, generated via the `.Q` proxy on an ontology model.
            include_timestamp_range: If `True`, the server will return the `start` and `end`
                timestamps corresponding to the temporal bounds of the matched data.

        Raises:
            TypeError: If an expression is not of the supported type.
            ValueError: If an operator does not start with the required '$' prefix.
            NotImplementedError: If a duplicate key (field path) is detected within the same query.
        """
        self._expressions = []
        self._include_tstamp_range = include_timestamp_range
        # Call the helper for each expression
        for expr in list(expressions):
            _validate_expression_type(
                expr,
                self.__supported_query_expressions__,
            )
            _validate_expression_operator_format(expr)
            _validate_expression_unique_key(self._expressions, expr.key)
            self._expressions.append(expr)

    def with_expression(self, expr: _QueryExpression) -> "QueryOntologyCatalog":
        """
        Adds a new [`_QueryCatalogExpression`][mosaicolabs.models.query.expressions._QueryCatalogExpression]
        expression to the query using a fluent interface.

        Example:
            ```python
            from mosaicolabs import MosaicoClient, Topic, QuerySequence

            with MosaicoClient.connect("localhost", 6726) as client:
                # Chain multiple sensor filters together
                qresponse = client.query(
                    QueryOntologyCatalog()
                    .with_expression(GPS.Q.status.satellites.geq(8))
                    .with_expression(GPS.Q.position.x.between([44.0, 45.0]))
                )

                # Inspect the response
                if qresponse is not None:
                    # Results are automatically grouped by Sequence for easier data management
                    for item in qresponse:
                        print(f"Sequence: {item.sequence.name}")
                        print(f"Topics: {[topic.name for topic in item.topics]}")

                # Filter for a specific component value and extract the first and last occurrence times
                qresponse = client.query(
                    QueryOntologyCatalog(include_timestamp_range=True)
                    .with_expression(IMU.Q.acceleration.x.lt(-4.0))
                    .with_expression(IMU.Q.acceleration.y.gt(5.0))
                )

                # Inspect the response
                if qresponse is not None:
                    # Results are automatically grouped by Sequence for easier data management
                    for item in qresponse:
                        print(f"Sequence: {item.sequence.name}")
                        print(f"Topics: {{topic.name:
                                    [topic.timestamp_range.start, topic.timestamp_range.end]
                                    for topic in item.topics}}")
            ```

        Args:
            expr: A valid expression generated via the `.Q` proxy on an ontology model,
                e.g., `GPS.Q.status.satellites.leq(10)`.

        Returns:
            The `QueryOntologyCatalog` instance for method chaining.
        """
        _validate_expression_type(
            expr,
            self.__supported_query_expressions__,
        )
        _validate_expression_operator_format(expr)
        _validate_expression_unique_key(self._expressions, expr.key)

        self._expressions.append(expr)
        return self

    # TODO: improve this query on the server side (remove necessity of ontology_type). Commented for now
    # def with_message_timestamp(
    #     self,
    #     ontology_type: object,
    #     time_start: Optional[Time] = None,
    #     time_end: Optional[Time] = None,
    # ) -> "QueryOntologyCatalog":
    #     """Helper method to add a filter for the 'creation_unix_timestamp' field."""
    #     # .between() expects a list [start, end]
    #     if time_start is None and time_end is None:
    #         raise ValueError(
    #             "At least one among 'time_start' and 'time_end' is mandatory"
    #         )

    #     ts_int = time_start.to_nanoseconds() if time_start else None
    #     te_int = time_end.to_nanoseconds() if time_end else None
    #     # special fields in data platform
    #     if not hasattr(ontology_type, "__ontology_tag__"):
    #         raise ValueError("Only Serializable types can be used as 'ontology_type'")
    #     sensor_tag = getattr(ontology_type, "__ontology_tag__")
    #     if ts_int and not te_int:
    #         expr = _QueryCatalogExpression(f"{sensor_tag}.timestamp_ns", "$geq", ts_int)
    #     elif te_int and not ts_int:
    #         expr = _QueryCatalogExpression(f"{sensor_tag}.timestamp_ns", "$leq", te_int)
    #     else:
    #         if not ts_int or not te_int:
    #             raise ValueError(
    #                 "This is embarassing"
    #             )  # will never happen (fix IDE complaining)
    #         if ts_int > te_int:
    #             raise ValueError("'time_start' must be less than 'time_end'.")

    #         expr = _QueryCatalogExpression(
    #             f"{sensor_tag}.timestamp_ns", "$between", [ts_int, te_int]
    #         )
    #     return self.with_expression(expr)

    # TODO: improve this query on the server side (remove necessity of ontology_type). Commented for now
    # def with_data_timestamp(
    #     self,
    #     ontology_type: type,
    #     time_start: Optional[Time] = None,
    #     time_end: Optional[Time] = None,
    # ) -> "QueryOntologyCatalog":
    #     """Helper method to add a filter for the 'creation_unix_timestamp' field."""
    #     # .between() expects a list [start, end]
    #     if time_start is None and time_end is None:
    #         raise ValueError(
    #             "At least one among 'time_start' and 'time_end' is mandatory"
    #         )

    #     # special fields in data platform
    #     if not hasattr(ontology_type, "__ontology_tag__"):
    #         raise ValueError(
    #             f"Only Serializable types can be used as 'ontology_type' class '{ontology_type.__name__}'"
    #         )
    #     sensor_tag = getattr(ontology_type, "__ontology_tag__")
    #     if time_start is not None and time_end is None:
    #         expr1 = _QueryCatalogExpression(
    #             f"{sensor_tag}.header.stamp.sec", "$geq", time_start.sec
    #         )
    #         expr2 = _QueryCatalogExpression(
    #             f"{sensor_tag}.header.stamp.nanosec", "$geq", time_start.nanosec
    #         )
    #     elif time_end is not None and time_start is None:
    #         expr1 = _QueryCatalogExpression(
    #             f"{sensor_tag}.header.stamp.sec", "$leq", time_end.sec
    #         )
    #         expr2 = _QueryCatalogExpression(
    #             f"{sensor_tag}.header.stamp.nanosec", "$leq", time_end.nanosec
    #         )
    #     else:
    #         if not time_start or not time_end:
    #             raise ValueError("This is embarassing")  # will never happen
    #         if time_start.to_nanoseconds() > time_end.to_nanoseconds():
    #             raise ValueError("'time_start' must be less than 'time_end'.")

    #         expr1 = _QueryCatalogExpression(
    #             f"{sensor_tag}.header.stamp.sec",
    #             "$between",
    #             [time_start.sec, time_end.sec],
    #         )
    #         expr2 = _QueryCatalogExpression(
    #             f"{sensor_tag}.header.stamp.nanosec",
    #             "$between",
    #             [time_start.nanosec, time_end.nanosec],
    #         )
    #     return self.with_expression(expr1).with_expression(expr2)

    # compatibility with QueryProtocol
    def name(self) -> str:
        """Returns the top-level key ('ontology') used for nesting inside a root [`Query`][mosaicolabs.models.query.builders.Query]."""
        return "ontology"

    # compatibility with QueryProtocol
    def to_dict(self) -> Dict[str, Any]:
        """
        Serializes the ontology expressions into a flat dictionary for the platform API.

        Example Output:
            `{"imu.timestamp_ns": {"$between": [...]}, "imu.acceleration.x": {"$leq": 10}}`

        Returns:
            A dictionary containing all merged sensor-field expressions.
        """
        query_dict = _QueryCombinator(list(self._expressions)).to_dict()
        if self._include_tstamp_range:
            query_dict.update({"include_timestamp_range": self._include_tstamp_range})
        return query_dict


class QueryTopic:
    """
    A top-level query object for Topic data that combines multiple expressions with a logical AND.

    This builder handles the complex partitioning required to query both flat system fields
    (like `name` or `ontology_tag`) and nested dictionary fields (like `user_metadata`).
    The resulting dictionary output preserves this hierarchical structure for server-side processing.

    Example:
        ```python
        from mosaicolabs import MosaicoClient, Image, Topic, QuerySequence

        with MosaicoClient.connect("localhost", 6726) as client:
            # Query for all 'image' topics created in a specific timeframe, matching some metadata (key, value) pair
            qresponse = client.query(
                QueryTopic()
                .with_ontology_tag(Image.ontology_tag())
                .with_created_timestamp(time_start=Time.from_float(1700000000))
                .with_expression(Topic.Q.user_metadata["camera_id.serial_number"].eq("ABC123_XYZ"))
            )

            # Inspect the response
            if qresponse is not None:
                # Results are automatically grouped by Sequence for easier data management
                for item in qresponse:
                    print(f"Sequence: {item.sequence.name}")
                    print(f"Topics: {[topic.name for topic in item.topics]}")
        ```
    """

    __supported_query_expressions__: Tuple[Type[_QueryExpression], ...] = (
        _QueryTopicExpression,
    )

    def __init__(self, *expressions: "_QueryExpression"):
        """
        The constructor initializes the query with an optional list of
        [`_QueryTopicExpression`][mosaicolabs.models.query.expressions._QueryTopicExpression] objects, generated
        via `Topic.Q.` proxy.

        This builder leverages the **`.Q` query proxy** on the `user_metadata`
        field of the [`Topic`][mosaicolabs.models.platform.Topic] model to provide
        a type-safe, fluent interface for filtering.

        Args:
            *expressions: A variable number of `Topic.Q` (`_QueryTopicExpression`) expression objects.

        Raises:
            TypeError: If an expression is not of the supported `Topic.Q` type.
            ValueError: If an operator does not follow the required internal '$' prefix format.
            NotImplementedError: If a duplicate key is detected, as the current implementation enforces unique keys per query.
        """
        self._expressions = []
        # Call the helper for each expression
        for expr in list(expressions):
            _validate_expression_type(
                expr,
                self.__supported_query_expressions__,
            )
            _validate_expression_operator_format(expr)
            _validate_expression_unique_key(self._expressions, expr.key)
            self._expressions.append(expr)

    def with_expression(self, expr: _QueryExpression) -> "QueryTopic":
        """
        Adds a new expression to the query using a fluent interface.

        This is the way to add filters for nested metadata.

        Example:
            ```python
            from mosaicolabs import MosaicoClient, Topic, QuerySequence

            with MosaicoClient.connect("localhost", 6726) as client:
                # Target a specific known topic path
                qresponse = client.query(
                    QueryTopic().with_expression(Topic.Q.user_metadata["version"].eq("1.0"))
                )

                # Inspect the response
                if qresponse is not None:
                    # Results are automatically grouped by Sequence for easier data management
                    for item in qresponse:
                        print(f"Sequence: {item.sequence.name}")
                        print(f"Topics: {[topic.name for topic in item.topics]}")
            ```

        Args:
            expr: A `_QueryTopicExpression` constructed via a `Topic.Q` proxy.

        Returns:
            The `QueryTopic` instance for method chaining.
        """

        _validate_expression_type(
            expr,
            self.__supported_query_expressions__,
        )
        _validate_expression_operator_format(expr)
        _validate_expression_unique_key(self._expressions, expr.key)
        self._expressions.append(expr)
        return self

    # --- Helper methods for common fields ---

    def with_name(self, name: str) -> "QueryTopic":
        """
        Adds an exact match filter for the topic 'name' field.

        Example:
            ```python
            from mosaicolabs import MosaicoClient, Topic, QuerySequence

            with MosaicoClient.connect("localhost", 6726) as client:
                # Target a specific known topic path
                qresponse = client.query(
                    QueryTopic().with_name("vehicle/front/camera")
                )

                # Inspect the response
                if qresponse is not None:
                    # Results are automatically grouped by Sequence for easier data management
                    for item in qresponse:
                        print(f"Sequence: {item.sequence.name}")
                        print(f"Topics: {[topic.name for topic in item.topics]}")
            ```

        Args:
            name: The exact name of the topic to match.

        Returns:
            The `QueryTopic` instance for method chaining.
        """
        return self.with_expression(_QueryTopicExpression("name", "$eq", f"{name}"))

    def with_name_match(self, name: str) -> "QueryTopic":
        """
        Adds a partial (fuzzy) match filter for the topic 'name' field.

        This performs an 'in-between' search (equivalent to %name%) on the full
        `sequence/topic` path.

        Example:
            ```python
            from mosaicolabs import MosaicoClient, Topic, QuerySequence

            with MosaicoClient.connect("localhost", 6726) as client:
                # Search for all topics containing the word 'camera'
                qresponse = client.query(
                    QueryTopic().with_name_match("camera")
                )

                # Inspect the response
                if qresponse is not None:
                    # Results are automatically grouped by Sequence for easier data management
                    for item in qresponse:
                        print(f"Sequence: {item.sequence.name}")
                        print(f"Topics: {[topic.name for topic in item.topics]}")
            ```

        Args:
            name: The string pattern to search for within the topic name.

        Returns:
            The `QueryTopic` instance for method chaining.
        """
        return self.with_expression(
            # employs explicit _QueryTopicExpression composition for dealing with
            # special fields in data platform
            _QueryTopicExpression("name", "$match", f"{name}")
        )

    def with_ontology_tag(self, ontology_tag: str) -> "QueryTopic":
        """
        Adds an exact match filter for the 'ontology_tag' field.

        This filter restricts the search to topics belonging to a specific data type
        identifier (e.g., 'imu', 'gnss').

        Example:
            ```python
            from mosaicolabs import MosaicoClient, Topic, QuerySequence

            with MosaicoClient.connect("localhost", 6726) as client:
                # Filter for IMU-only data streams
                qresponse = client.query(
                    QueryTopic().with_ontology_tag(IMU.ontology_tag())
                )

                # Inspect the response
                if qresponse is not None:
                    # Results are automatically grouped by Sequence for easier data management
                    for item in qresponse:
                        print(f"Sequence: {item.sequence.name}")
                        print(f"Topics: {[topic.name for topic in item.topics]}")
            ```
            **Note**: To ensure compatibility and avoid hardcoding strings, it is highly recommended to
            retrieve the tag dynamically using the
            [`ontology_tag()`][mosaicolabs.models.Serializable.ontology_tag]
            method of the desired ontology class.

        Args:
            ontology_tag: The string tag (e.g., 'imu', 'gps') to filter by.

        Returns:
            The `QueryTopic` instance for method chaining.
        """
        return self.with_expression(
            # employs explicit _QueryTopicExpression composition for dealing with
            # special fields in data platform
            _QueryTopicExpression("ontology_tag", "$eq", ontology_tag)
        )

    def with_created_timestamp(
        self, time_start: Optional[Time] = None, time_end: Optional[Time] = None
    ) -> "QueryTopic":
        """
        Adds a filter for the 'created_timestamp' field using high-precision Time.

        Example:
            ```python
            from mosaicolabs import MosaicoClient, Topic, QuerySequence

            with MosaicoClient.connect("localhost", 6726) as client:
                # Find sequences created during a specific day
                qresponse = client.query(
                    QueryTopic().with_created_timestamp(
                        time_start=Time.from_float(1704067200.0), # 2024-01-01
                        time_end=Time.from_float(1704153600.0)    # 2024-01-02
                    )
                )

                # Inspect the response
                if qresponse is not None:
                    # Results are automatically grouped by Sequence for easier data management
                    for item in qresponse:
                        print(f"Sequence: {item.sequence.name}")
                        print(f"Topics: {[topic.name for topic in item.topics]}")
            ```

        Args:
            time_start: Optional lower bound (inclusive).
            time_end: Optional upper bound (inclusive).

        Returns:
            The `QueryTopic` instance for method chaining.

        Raises:
            ValueError: If both bounds are None or if `time_start > time_end`.
        """
        # .between() expects a list [start, end]
        if time_start is None and time_end is None:
            raise ValueError(
                "At least one among 'time_start' and 'time_end' is mandatory"
            )

        ts_int = time_start.to_nanoseconds() if time_start else None
        te_int = time_end.to_nanoseconds() if time_end else None
        # employs explicit _QueryTopicExpression composition for dealing with
        # special fields in data platform
        if ts_int and not te_int:
            expr = _QueryTopicExpression("created_timestamp", "$geq", ts_int)
        elif te_int and not ts_int:
            expr = _QueryTopicExpression("created_timestamp", "$leq", te_int)
        else:
            if not ts_int or not te_int:
                raise ValueError("This is embarassing")  # will never happen
            if ts_int > te_int:
                raise ValueError("'time_start' must be less than 'time_end'.")

            expr = _QueryTopicExpression(
                "created_timestamp", "$between", [ts_int, te_int]
            )
        return self.with_expression(expr)

    # compatibility with QueryProtocol
    def name(self) -> str:
        """Returns the top-level key ('topic') used when nesting this query inside a root [`Query`][mosaicolabs.models.query.builders.Query]."""
        return "topic"

    # compatibility with QueryProtocol
    def to_dict(self) -> Dict[str, Any]:
        """
        Serializes the query into a nested dictionary for the platform API.

        This method partitions expressions into two groups:

        1. **System Fields**: Standard fields like `name` are kept in the root dictionary.
        2. **Metadata Fields**: Fields starting with a dictionary-type model key (e.g., `user_metadata`)
           are stripped of their prefix and nested under that key.

        Returns:
            A dictionary representation of the query, e.g., `{"name": {"$eq": "..."}, "user_metadata": {"key": {"$eq": "..."}}}`.
        """
        # Delayed import to avoid circular dependency
        from ..platform.topic import Topic

        # Identify all fields that are dictionaries (like user_metadata)
        metadata_field_names = {
            fname
            for fname, finfo in Topic.model_fields.items()
            if get_origin(finfo.annotation) is dict
        }

        # Partition all expressions into "normal" or "metadata"
        normal_exprs = []
        # Create a "bucket" for each metadata field (e.g., {"user_metadata": []})
        metadata_buckets = {name: [] for name in metadata_field_names}

        for expr in self._expressions:
            is_metadata_expr = False
            for meta_name in metadata_field_names:
                # Check if the expression's field path starts with a metadata field name
                # e.g., "user_metadata.mission" starts with "user_metadata"
                if expr.key == meta_name or expr.key.startswith(f"{meta_name}."):
                    metadata_buckets[meta_name].append(expr)
                    is_metadata_expr = True
                    break

            if not is_metadata_expr:
                normal_exprs.append(expr)

        # Combine the normal, top-level expressions
        # This will produce {"name": {"$eq": "..."}}
        exprs_dict = _QueryCombinator(normal_exprs).to_dict()

        # Build and merge the nested metadata dictionaries
        for meta_name, meta_exprs in metadata_buckets.items():
            if not meta_exprs:
                continue  # Skip if no expressions for this metadata field

            # Re-create expressions with the prefix stripped
            # e.g., "user_metadata.mission" -> "mission"
            stripped_exprs = []
            for expr in meta_exprs:
                if "." not in expr.key:
                    # Skip expressions on the root dict itself (e.g., user_metadata.is_null())
                    continue

                # Get the sub-key (e.g., "mission")
                sub_key = expr.key.split(".", 1)[1]
                # Create a new expression with the sub-key as its path
                stripped_exprs.append(
                    _QueryTopicExpression(sub_key, expr.op, expr.value)
                )

            if stripped_exprs:
                # Combine the new, stripped expressions into a dict
                meta_dict = _QueryCombinator(stripped_exprs).to_dict()
                # Add them nested under the metadata field name
                # e.g., exprs_dict["user_metadata"] = {"mission": {"$eq": "..."}}
                exprs_dict[meta_name] = meta_dict

        return exprs_dict


class QuerySequence:
    """
    A top-level query object for Sequence data that combines multiple expressions with a logical AND.

    This builder handles the complex partitioning required to query both flat system fields
    (like `name`) and nested dictionary fields (like `user_metadata`).
    The resulting dictionary output preserves this hierarchical structure for server-side processing.

    Example:
        ```python
        from mosaicolabs import MosaicoClient, Topic, QuerySequence

        with MosaicoClient.connect("localhost", 6726) as client:
            # Search for sequences by project name and creation date
            qresponse = client.query(
                QuerySequence()
                .with_expression(Sequence.Q.user_metadata["project"].eq("Apollo"))
                .with_created_timestamp(time_start=Time.from_float(1690000000.0))
            )

            # Inspect the response
            for item in qresponse:
                print(f"Sequence: {item.sequence.name}")
                print(f"Topics: {[topic.name for topic in item.topics]}")
        ```
    """

    __supported_query_expressions__: Tuple[Type[_QueryExpression], ...] = (
        _QuerySequenceExpression,
    )

    def __init__(self, *expressions: "_QueryExpression"):
        """
        The constructor initializes the query with an optional list of
        [`_QuerySequenceExpression`][mosaicolabs.models.query.expressions._QuerySequenceExpression] objects, generated
        via `Sequence.Q.` proxy.

        This builder leverages the **`.Q` query proxy** specifically on the `user_metadata`
        field of the [`Sequence`][mosaicolabs.models.platform.Sequence] model to provide
        a type-safe, fluent interface for filtering.

        Args:
            *expressions: A variable number of `Sequence.Q` (`_QuerySequenceExpression`) objects.

        Raises:
            TypeError: If an expression is not of the supported `Sequence.Q` type.
            ValueError: If an operator does not follow the required internal '$' prefix format.
            NotImplementedError: If a duplicate key is detected, as the current implementation enforces unique keys per query.
        """
        self._expressions = []
        # Call the helper for each expression
        for expr in list(expressions):
            _validate_expression_type(
                expr,
                self.__supported_query_expressions__,
            )
            _validate_expression_operator_format(expr)
            _validate_expression_unique_key(self._expressions, expr.key)
            self._expressions.append(expr)

    def with_expression(self, expr: _QueryExpression) -> "QuerySequence":
        """
        Adds a new expression to the query using a fluent interface.

        This is the way to add filters for nested metadata.

        Example:
            ```python
            from mosaicolabs import MosaicoClient, Topic, QuerySequence

            with MosaicoClient.connect("localhost", 6726) as client:
                # Target a specific known topic path
                qresponse = client.query(
                    QuerySequence().with_expression(Sequence.Q.user_metadata["project"].eq("Apollo"))
                )

                # Inspect the response
                for item in qresponse:
                    print(f"Sequence: {item.sequence.name}")
                    print(f"Topics: {[topic.name for topic in item.topics]}")
            ```

        Args:
            expr: A `_QuerySequenceExpression` constructed via a `Sequence.Q` proxy.

        Returns:
            The `QuerySequence` instance for method chaining.

        """
        _validate_expression_type(
            expr,
            self.__supported_query_expressions__,
        )
        _validate_expression_operator_format(expr)
        _validate_expression_unique_key(self._expressions, expr.key)

        self._expressions.append(expr)
        return self

    # --- Helper methods for common fields ---
    def with_name(self, name: str) -> "QuerySequence":
        """
        Adds an exact match filter for the sequence 'name' field.

        Example:
            ```python
            from mosaicolabs import MosaicoClient, Topic, QuerySequence

            with MosaicoClient.connect("localhost", 6726) as client:
                # Find all sequences with name equal to 'test_winter_01'
                qresponse = client.query(
                    QuerySequence().with_name("test_winter_01")
                )

                # Inspect the response
                for item in qresponse:
                    print(f"Sequence: {item.sequence.name}")
                    print(f"Topics: {[topic.name for topic in item.topics]}")
            ```

        Args:
            name: The exact name of the sequence to match.

        Returns:
            The `QuerySequence` instance for method chaining.
        """
        return self.with_expression(
            # employs explicit _QuerySequenceExpression composition for dealing with
            # special fields in data platform
            _QuerySequenceExpression("name", "$eq", name)
        )

    def with_name_match(self, name: str) -> "QuerySequence":
        """
        Adds a partial (fuzzy) match filter for the sequence 'name' field.

        This performs an 'in-between' search (equivalent to %name%) on the sequence name.

        Example:
            ```python
            from mosaicolabs import MosaicoClient, Topic, QuerySequence

            with MosaicoClient.connect("localhost", 6726) as client:
                # Find all sequences with name containing 'calibration_run_'
                qresponse = client.query(
                    QuerySequence().with_name_match("calibration_run_")
                )

                # Inspect the response
                for item in qresponse:
                    print(f"Sequence: {item.sequence.name}")
                    print(f"Topics: {[topic.name for topic in item.topics]}")
            ```

        Args:
            name: The string pattern to search for within the sequence name.

        Returns:
            The `QuerySequence` instance for method chaining.
        """
        return self.with_expression(
            # employs explicit _QuerySequenceExpression composition for dealing with
            # special fields in data platform
            _QuerySequenceExpression("name", "$match", f"{name}")
        )

    def with_created_timestamp(
        self, time_start: Optional[Time] = None, time_end: Optional[Time] = None
    ) -> "QuerySequence":
        """
        Adds a filter for the 'created_timestamp' field using high-precision Time.

        Example:
            ```python
            from mosaicolabs import MosaicoClient, Topic, QuerySequence

            with MosaicoClient.connect("localhost", 6726) as client:
                # Find sequences created during a specific time range
                qresponse = client.query(
                    QuerySequence().with_created_timestamp(
                        time_start=Time.from_float(1704067200.0), # 2024-01-01
                        time_end=Time.from_float(1704153600.0)    # 2024-01-02
                    )
                )

                # Inspect the response
                for item in qresponse:
                    print(f"Sequence: {item.sequence.name}")
                    print(f"Topics: {[topic.name for topic in item.topics]}")
            ```

        Args:
            time_start: Optional lower bound (inclusive).
            time_end: Optional upper bound (inclusive).

        Returns:
            The `QuerySequence` instance for method chaining.

        Raises:
            ValueError: If both bounds are `None` or if `time_start > time_end`.
        """
        # .between() expects a list [start, end]
        if time_start is None and time_end is None:
            raise ValueError(
                "At least one among 'time_start' and 'time_end' is mandatory"
            )

        ts_int = time_start.to_nanoseconds() if time_start else None
        te_int = time_end.to_nanoseconds() if time_end else None
        # employs explicit _QuerySequenceExpression composition for dealing with
        # special fields in data platform
        if ts_int and not te_int:
            expr = _QuerySequenceExpression("created_timestamp", "$geq", ts_int)
        elif te_int and not ts_int:
            expr = _QuerySequenceExpression("created_timestamp", "$leq", te_int)
        else:
            if not ts_int or not te_int:
                raise ValueError("This is embarassing")  # will never happen
            if ts_int > te_int:
                raise ValueError("'time_start' must be less than 'time_end'.")

            expr = _QuerySequenceExpression(
                "created_timestamp", "$between", [ts_int, te_int]
            )
        return self.with_expression(expr)

    # compatibility with QueryProtocol
    def name(self) -> str:
        """Returns the top-level key ('sequence') used for nesting inside a root [`Query`][mosaicolabs.models.query.builders.Query]."""
        return "sequence"

        # compatibility with QueryProtocol

    def to_dict(self) -> Dict[str, Any]:
        """
        Serializes the query into a nested dictionary for the platform API.

        This method partitions expressions into:

        1. **Normal Fields**: Fields like `name` are kept in a flat dictionary.
        2. **Metadata Fields**: Fields targeting `user_metadata` are collected and nested.

        Returns:
            A dictionary representation preserving the hierarchical structure.
        """
        # Delayed import to avoid circular dependency
        from ..platform.sequence import Sequence

        # Identify all fields that are dictionaries (like user_metadata)
        metadata_field_names = {
            fname
            for fname, finfo in Sequence.model_fields.items()
            if get_origin(finfo.annotation) is dict
        }

        # Partition all expressions into "normal" or "metadata"
        normal_exprs = []
        # Create a "bucket" for each metadata field (e.g., {"user_metadata": []})
        metadata_buckets = {name: [] for name in metadata_field_names}

        for expr in self._expressions:
            is_metadata_expr = False
            for meta_name in metadata_field_names:
                # Check if the expression's field path starts with a metadata field name
                # e.g., "user_metadata.mission" starts with "user_metadata"
                if expr.key == meta_name or expr.key.startswith(f"{meta_name}."):
                    metadata_buckets[meta_name].append(expr)
                    is_metadata_expr = True
                    break

            if not is_metadata_expr:
                normal_exprs.append(expr)

        # Combine the normal, top-level expressions
        # This will produce {"name": {"$eq": "..."}}
        exprs_dict = _QueryCombinator(normal_exprs).to_dict()

        # Build and merge the nested metadata dictionaries
        for meta_name, meta_exprs in metadata_buckets.items():
            if not meta_exprs:
                continue  # Skip if no expressions for this metadata field

            # Re-create expressions with the prefix stripped
            # e.g., "user_metadata.mission" -> "mission"
            stripped_exprs = []
            for expr in meta_exprs:
                if "." not in expr.key:
                    # Skip expressions on the root dict itself (e.g., user_metadata.is_null())
                    continue

                # Get the sub-key (e.g., "mission")
                sub_key = expr.key.split(".", 1)[1]
                # Create a new expression with the sub-key as its path
                stripped_exprs.append(
                    _QuerySequenceExpression(sub_key, expr.op, expr.value)
                )

            if stripped_exprs:
                # Combine the new, stripped expressions into a dict
                meta_dict = _QueryCombinator(stripped_exprs).to_dict()
                # Add them nested under the metadata field name
                # e.g., exprs_dict["user_metadata"] = {"mission": {"$eq": "..."}}
                exprs_dict[meta_name] = meta_dict

        return exprs_dict


class Query:
    """
    A top-level "root" query object that aggregates multiple specialized sub-queries into a single request body.

    This class serves as the final envelope for multi-domain queries, ensuring that
    different query types (Topic, Sequence, Ontology) do not overwrite each other.

    Example:
        ```python
        from mosaicolabs import QueryOntologyCatalog, QuerySequence, Query, IMU, MosaicoClient

        # Establish a connection to the Mosaico Data Platform
        with MosaicoClient.connect("localhost", 6726) as client:
            # Build a filter with name pattern and metadata-related expression
            query = Query(
                # Append a filter for sequence metadata
                QuerySequence()
                .with_expression(
                    # Use query proxy for generating a _QuerySequenceExpression
                    Sequence.Q.user_metadata["environment.visibility"].lt(50)
                )
                .with_name_match("test_drive"),
                # Append a filter with deep time-series data discovery and measurement time windowing
                QueryOntologyCatalog(include_timestamp_range=True)
                .with_expression(IMU.Q.acceleration.x.gt(5.0))
                .with_expression(IMU.Q.header.stamp.sec.gt(1700134567))
                .with_expression(IMU.Q.header.stamp.nanosec.between([123456, 789123])),
            )
            # Perform the server side query
            qresponse = client.query(query=query)
            # Inspect the response
                if qresponse is not None:
                    # Results are automatically grouped by Sequence for easier data management
                    for item in qresponse:
                        print(f"Sequence: {item.sequence.name}")
                        print(f"Topics: {{topic.name:
                                    [topic.timestamp_range.start, topic.timestamp_range.end]
                                    for topic in item.topics}}")
        ```
    """

    def __init__(self, *queries: QueryableProtocol):
        """
        Initializes the root query with a set of sub-queries.

        Args:
            *queries: A variable number of sub-query objects (e.g., `QueryTopic()`, `QuerySequence()`).

        Raises:
            ValueError: If duplicate query types are detected in the initial arguments.
        """
        self._queries = list(queries)

        # --- Validation ---
        # Check for duplicate query types (e.g., two QueryTopic instances)
        # as they would overwrite each other in the final dictionary.
        self._types_seen = {}
        for q in queries:
            t = type(q)
            if t in self._types_seen:
                raise ValueError(
                    f"Duplicate query type detected: '{t.__name__}'. "
                    "Multiple instances of the same type will override each other when encoded.",
                )
            else:
                self._types_seen[t] = True

    def append(self, *queries: QueryableProtocol):
        """
        Adds additional sub-queries to the existing root query.

        Args:
            *queries: Additional sub-query instances.

        Raises:
            ValueError: If an appended query type is already present in the request.

        Example:
            ```python
            from mosaicolabs import QueryOntologyCatalog, QuerySequence, Query, IMU, MosaicoClient

            # Build a filter with name pattern and metadata-related expression
            query = Query(
                # Append a filter for sequence metadata
                QuerySequence()
                .with_expression(
                    # Use query proxy for generating a _QuerySequenceExpression
                    Sequence.Q.user_metadata["environment.visibility"].lt(50)
                )
                .with_name_match("test_drive")
            )

            # Append a filter with deep time-series data discovery and measurement time windowing
            query.append(
                QueryOntologyCatalog()
                .with_expression(IMU.Q.acceleration.x.gt(5.0))
                .with_expression(IMU.Q.header.stamp.sec.gt(1700134567))
                .with_expression(IMU.Q.header.stamp.nanosec.between([123456, 789123])),
            )
            ```
        """
        for q in queries:
            t = type(q)
            if t in self._types_seen:
                raise ValueError(
                    f"Duplicate query type detected: '{t.__name__}'. "
                    "Multiple instances of the same type will override each other when encoded.",
                )
            else:
                self._types_seen[t] = True
                self._queries.append(q)

    def to_dict(self) -> Dict[str, Any]:
        """
        Serializes the entire multi-domain query into the final JSON dictionary.

        It orchestrates the conversion by calling the `.name()` and `.to_dict()`
        methods of each contained sub-query.

        Example Output:
            ```json
            {
                "topic": { ... topic filters ... },
                "sequence": { ... sequence filters ... },
                "ontology": { ... ontology filters ... }
            }
            ```

        Returns:
            The final aggregated query dictionary.
        """
        # Uses a dictionary comprehension to build the final object
        return {q.name(): q.to_dict() for q in self._queries}
