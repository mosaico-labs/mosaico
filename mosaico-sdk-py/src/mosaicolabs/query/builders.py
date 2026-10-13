"""
This module provides the high-level "Fluent" API for constructing complex searches across the Mosaico Data Platform.

It implements a Domain-Specific Language that allows users to filter **Sequences**, **Topics**, and **Ontology** data using a type-safe, method-chaining interface.


**Key Components:**

* [**`Query`**][mosaicolabs.query.builders.Query]: The root container that aggregates multiple specialized sub-queries.
* [**`QueryOntologyCatalog`**][mosaicolabs.query.builders.QueryOntologyCatalog]: For fine-grained filtering based on sensor-specific field values (e.g., `IMU.Q.acceleration.x > 9.8`).
* [**`QueryTopic`**][mosaicolabs.query.builders.QueryTopic]: Specifically for filtering topic-level metadata.
* [**`QuerySequence`**][mosaicolabs.query.builders.QuerySequence]: Specifically for filtering sequence-level metadata.
"""

from typing import Any, List, Optional, Tuple, Type, Union

from ..proto.v1 import query_pb2

# Import the building blocks for expressions
from .expressions import (
    _QueryCatalogExpression,
    _QueryExpression,
    _QuerySequenceExpression,
    _QueryTopicExpression,
)
from .generation.mixins import _make_queryable_field_intance, _QueryableDynamicValue
from .protocols import QueryableProtocol


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


def _fill_catalog_filter(
    msg: Union[query_pb2.TopicFilter, query_pb2.SequenceFilter],
    exprs: List[_QueryExpression],
):
    """
    Private helper filling a `TopicFilter` / `SequenceFilter` message.

    Expressions on `user_metadata.<key>` go into the `user_metadata` map under
    `<key>`, all the others target the message field with the same name.
    """
    for expr in exprs:
        if expr.key.startswith("user_metadata."):
            meta_key = expr.key.split(".", 1)[1]
            msg.user_metadata[meta_key].CopyFrom(expr._to_condition())
        else:
            getattr(msg, expr.key).CopyFrom(expr._to_condition())


# --- Query Builders --


class QueryOntologyCatalog:
    """
    A top-level query object for the Data Catalog that combines multiple sensor-field expressions.

    This builder allows for fine-grained filtering based on the actual values contained within sensor payloads
    (e.g., IMU acceleration, GPS coordinates, or custom telemetry).
    Field paths utilize dot-notation (e.g., `"IMU.acceleration.x"`).

    This class is designed to work with the **`.Q` query proxy** injected into every
    [`Serializable`][mosaicolabs.models.core.Serializable] data ontology model.
    You can use this proxy on any registered sensor class (like [`IMU`][mosaicolabs.models.sensors.IMU],
    [`Vector3d`][mosaicolabs.models.data.geometry.Vector3d],
    [`Point3d`][mosaicolabs.models.data.geometry.Point3d]), etc.
    to create type-safe expressions.

    Example:
        ```python
        from mosaicolabs import MosaicoClient, IMU, QueryOntologyCatalog

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

                    # Clusterize all topics within the sequence to extract the time intervals
                    clusters_dict = item.clusterize_all()

                    # Since clusterize_all() used default clustering_dt_ns, each topic will have
                    # just one cluster representing the first and last moment the query was satisfied
                    for t_name, clusters in clusters_dict.items():
                        print(f"{t_name}:\\n", "\\n".join(f"{cluster}" for cluster in clusters))
        ```
    """

    __supported_query_expressions__: Tuple[Type[_QueryExpression], ...] = (
        _QueryCatalogExpression,
    )

    def __init__(
        self,
        *expressions: "_QueryExpression",
    ):
        """
        The constructor initializes the query with an optional list of
        [`_QueryCatalogExpression`][mosaicolabs.query.expressions._QueryCatalogExpression] objects, generated
        via `<Model>.Q.` proxy, where model is any of the available data ontology (e.g. IMU.Q, GPS.Q, String.Q, etc.)

        Args:
            *expressions (_QueryExpression): A variable number of expressions, generated via the `.Q` proxy on an ontology model.

        Raises:
            TypeError: If an expression is not of the supported type.
        """
        self._expressions: list[_QueryExpression] = []
        # Call the helper for each expression
        for expr in expressions:
            _validate_expression_type(expr, self.__supported_query_expressions__)
            self._expressions.append(expr)

    @classmethod
    def _from_expressions(cls, *exprs: _QueryExpression) -> "QueryOntologyCatalog":
        """
        Internal method for creating a new query builder from a list of expressions.

        Args:
            exprs (_QueryExpression): A list of `_QueryExpression` instances.

        Returns:
            QueryOntologyCatalog: A new `QueryOntologyCatalog` instance containing the provided expressions.
        """
        return cls(*exprs)

    def with_expression(self, expr: _QueryExpression) -> "QueryOntologyCatalog":
        """
        Adds a new [`_QueryCatalogExpression`][mosaicolabs.query.expressions._QueryCatalogExpression]
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

                        # Clusterize all topics within the sequence to extract the time intervals
                        clusters_dict = item.clusterize_all()

                        # Since clusterize_all() used default clustering_dt_ns, each topic will have
                        # just one cluster representing the first and last moment the query was satisfied
                        for t_name, clusters in clusters_dict.items():
                            print(f"{t_name}:\\n", "\\n".join(f"{cluster}" for cluster in clusters))
            ```

        Args:
            expr (_QueryExpression): A valid expression generated via the `.Q` proxy on an ontology model,
                e.g., `GPS.Q.status.satellites.leq(10)`.

        Returns:
            QueryOntologyCatalog: The `QueryOntologyCatalog` instance for method chaining.
        """
        _validate_expression_type(
            expr,
            self.__supported_query_expressions__,
        )
        self._expressions.append(expr)
        return self

    # compatibility with QueryProtocol
    def _apply_to(self, msg: query_pb2.Filter):
        """Fills the `ontology` field of the `Filter` message, one predicate per expression."""
        msg.ontology.predicates.extend(
            expr._to_predicate() for expr in self._expressions
        )

    def expressions(self) -> List[_QueryExpression]:
        """
        Return the list of query expressions.
        """
        return self._expressions


class QueryTopic:
    """
    A top-level query object for Topic data that combines multiple expressions with a logical AND.

    This builder handles both flat system fields (like `name` or `ontology_tag`)
    and nested dictionary fields (like `user_metadata`).

    Example:
        ```python
        from mosaicolabs import MosaicoClient, Image, QueryTopic

        with MosaicoClient.connect("localhost", 6726) as client:
            # Query for all 'image' topics created in a specific timeframe, matching some metadata (key, value) pair
            qresponse = client.query(
                QueryTopic()
                .with_ontology_tag(Image.ontology_tag())
                .with_created_timestamp(time_start=1700000000_000000000)
                .with_user_metadata("camera_id.serial_number", eq="ABC123_XYZ")
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

    def __init__(self):
        """
        The constructor initializes an empty query builder
        """
        self._expressions: list[_QueryExpression] = []

    @classmethod
    def _from_expressions(cls, *exprs: _QueryExpression) -> "QueryTopic":
        """
        Internal method for creating a new query builder from a list of expressions.

        Args:
            exprs (_QueryTopicExpression): A list of `_QueryTopicExpression` instances.

        Returns:
            QueryTopic: A new `QueryTopic` instance containing the provided expressions.
        """
        instance = cls()

        for expr in exprs:
            _validate_expression_type(expr, cls.__supported_query_expressions__)
            _validate_expression_unique_key(instance._expressions, expr.key)
            instance._expressions.append(expr)

        return instance

    def _with_expression(self, expr: _QueryExpression) -> "QueryTopic":
        """
        Internal method for adding a new expression to the query inner list
        using a fluent interface.

        Args:
            expr (_QueryTopicExpression): A `_QueryTopicExpression`.

        Returns:
            QueryTopic: The `QueryTopic` instance for method chaining.
        """

        _validate_expression_type(expr, self.__supported_query_expressions__)
        _validate_expression_unique_key(self._expressions, expr.key)
        self._expressions.append(expr)
        return self

    # --- Helper methods for common fields ---

    def with_user_metadata(self, key: str, **operator_kwargs: Any) -> "QueryTopic":
        """
        Appends a metadata filter to the query using a fluent, operator-based interface.

        This method simplifies metadata discovery by allowing direct filtering on the `user_metadata`
        dictionary of the Topic. Each call adds a logical AND condition to the query.

        Args:
            key (str): The metadata key to filter on (e.g., "sensor_id"). Supports dot-notation
                for nested dictionary access (e.g., "calibration.focal_length").
            **operator_kwargs: A single keyword argument where the key is the operator
                and the value is the comparison target, e.g. `eq="value"`, `lt=100`, `ex=False`, etc.

        Raises:
            ValueError: If no operator is provided, if multiple operators are provided in
                a single call, or if an unsupported operator is used.

        Supported Operators:
            * `eq`: Equal to
            * `neq`: Not equal to
            * `gt`: Greater than
            * `geq`: Greater than or equal to
            * `lt`: Less than
            * `leq`: Less than or equal to
            * `match`: glob-style pattern matching, see
                [QueryableString.match][mosaicolabs.query.queryable_fields.QueryableString.match]
            * `between`: Range filter (expects a list of [min, max])
            * `outside`: Range filter (expects a list of [min, max])
            * `in_`: One-of (expects a list of values)
            * `ex`: The key exist (ex=True) or does not exist (ex=False)

        Example:
            ```python
            # Find sequences for 'Apollo' project with visibility under 100m
            query = (
                QueryTopic()
                .with_user_metadata("sensor_id", eq="ABC123_XYZ")
                .with_user_metadata("calibration.focal_length", between=(14, 24))
            )
            results = client.query(query)
            ```

        Returns:
            QueryTopic: The current instance to support method chaining.
        """
        if len(operator_kwargs.keys()) != 1:
            raise ValueError("'with_user_metadata' accepts exactly one operator key.")
        op, value = operator_kwargs.popitem()

        qfield_inst = _make_queryable_field_intance(
            queryable_type=_QueryableDynamicValue,
            field_full_path=f"user_metadata.{key}",
            expression_type=_QueryTopicExpression,
        )

        op_callback = getattr(qfield_inst, op)

        return self._with_expression(op_callback(value))

    def with_name(self, name: str) -> "QueryTopic":
        """
        Adds an exact match filter for the topic 'name' field.

        Example:
            ```python
            from mosaicolabs import MosaicoClient, Topic, QueryTopic

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
            name (str): The exact name of the topic to match.

        Returns:
            QueryTopic: The `QueryTopic` instance for method chaining.
        """
        return self._with_expression(
            _QueryTopicExpression("name", query_pb2.OPERATOR_EQ, f"{name}")
        )

    def with_name_match(self, name: str) -> "QueryTopic":
        """
        Adds a RegEx-like filter for the topic 'name' field. Supported operators are:

        * `*`: matches a multiple (zero or more) characters, including space.
        * `?`: matches a single (exactly one) characters, including space.
        * `[]`: matches a character set. Examples: `[aeiou]` to match any vocals, or `[a-z]` to match a range
        * `#`: matches any single digit (0 — 9). Shortcut for `[0-9]`

        Supposing the server contains a topic with name `car1/imu/front` we can get it using:

        * `*imu*`
        * `car#/[a-z]*/[a-z]**`
        * `car?/[umi]*/?????`

        Note:
            If `name` contains none of the wildcards above, the operator
            is equivalent to [`.with_name()`][mosaicolabs.query.builders.QueryTopic.with_name],
            i.e. an exact match will be performed.

        Example:
            ```python
            from mosaicolabs import MosaicoClient, Topic, QuerySequence

            with MosaicoClient.connect("localhost", 6726) as client:
                # Search for all topics containing the word 'camera'
                qresponse = client.query(
                    QueryTopic().with_name_match("/[a-z]/camera")
                )

                # Inspect the response
                if qresponse is not None:
                    # Results are automatically grouped by Sequence for easier data management
                    for item in qresponse:
                        print(f"Sequence: {item.sequence.name}")
                        print(f"Topics: {[topic.name for topic in item.topics]}")
            ```

        Args:
            name (str): The string pattern to search for within the topic name.

        Returns:
            QueryTopic: The `QueryTopic` instance for method chaining.
        """
        return self._with_expression(
            # employs explicit _QueryTopicExpression composition for dealing with
            # special fields in data platform
            _QueryTopicExpression("name", query_pb2.OPERATOR_MATCH, f"{name}")
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
            [`ontology_tag()`][mosaicolabs.models.core.Serializable.ontology_tag]
            method of the desired ontology class.

        Args:
            ontology_tag (str): The string tag (e.g., 'imu', 'gps') to filter by.

        Returns:
            QueryTopic: The `QueryTopic` instance for method chaining.
        """
        return self._with_expression(
            # employs explicit _QueryTopicExpression composition for dealing with
            # special fields in data platform
            _QueryTopicExpression("ontology_tag", query_pb2.OPERATOR_EQ, ontology_tag)
        )

    def with_created_timestamp(
        self,
        time_start: Optional[int] = None,
        time_end: Optional[int] = None,
    ) -> "QueryTopic":
        """
        Adds a filter for the 'created_at_ns' field using high-precision Time.

        Example:
            ```python
            from mosaicolabs import MosaicoClient, QueryTopic

            with MosaicoClient.connect("localhost", 6726) as client:
                # Find sequences created during a specific day
                qresponse = client.query(
                    QueryTopic().with_created_timestamp(
                        time_start=1704067200_000000000, # 2024-01-01
                        time_end=1704153600_000000000    # 2024-01-02
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
            time_start (Optional[int]): Optional lower bound (inclusive).
            time_end (Optional[int]): Optional upper bound (inclusive).

        Returns:
            QueryTopic: The `QueryTopic` instance for method chaining.

        Raises:
            ValueError: If both bounds are None or if `time_start > time_end`.
        """
        # .between() expects a list [start, end]
        if time_start is None and time_end is None:
            raise ValueError(
                "At least one among 'time_start' and 'time_end' is mandatory"
            )

        ts_int = time_start if time_start else None
        te_int = time_end if time_end else None
        # employs explicit _QueryTopicExpression composition for dealing with
        # special fields in data platform
        if ts_int and not te_int:
            expr = _QueryTopicExpression("created_at_ns", query_pb2.OPERATOR_GE, ts_int)
        elif te_int and not ts_int:
            expr = _QueryTopicExpression("created_at_ns", query_pb2.OPERATOR_LE, te_int)
        else:
            if not ts_int or not te_int:
                raise ValueError("This is embarassing")  # will never happen
            if ts_int > te_int:
                raise ValueError("'time_start' must be less than 'time_end'.")

            expr = _QueryTopicExpression(
                "created_at_ns", query_pb2.OPERATOR_BETWEEN, [ts_int, te_int]
            )
        return self._with_expression(expr)

    # compatibility with QueryProtocol
    def _apply_to(self, msg: query_pb2.Filter):
        """Fills the `topic` field of the `Filter` message."""
        _fill_catalog_filter(msg.topic, self._expressions)

    def expressions(self) -> List[_QueryExpression]:
        """
        The list of query expressions.

        Returns:
            List[_QueryExpression]: The list of expressions currently stored in the query.
        """
        return self._expressions


class QuerySequence:
    """
    A top-level query object for Sequence data that combines multiple expressions with a logical AND.

    This builder handles both flat system fields (like `name`) and nested
    dictionary fields (like `user_metadata`).

    Example:
        ```python
        from mosaicolabs import MosaicoClient, QuerySequence

        with MosaicoClient.connect("localhost", 6726) as client:
            # Search for sequences by project name and creation date
            qresponse = client.query(
                QuerySequence()
                .with_user_metadata("project", eq="Apollo")
                .with_created_timestamp(time_start=1690000000_000000000)
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

    def __init__(self):
        """
        The constructor initializes an empty query builder
        """
        self._expressions: List[_QueryExpression] = []

    @classmethod
    def _from_expressions(cls, *exprs: _QueryExpression) -> "QuerySequence":
        """
        Internal method for creating a new query builder from a list of expressions.

        Args:
            exprs (_QueryExpression): A list of `_QuerySequenceExpression` instances.

        Returns:
            QuerySequence: A new `QuerySequence` instance containing the provided expressions.
        """
        instance = cls()

        for expr in exprs:
            _validate_expression_type(expr, cls.__supported_query_expressions__)
            _validate_expression_unique_key(instance._expressions, expr.key)
            instance._expressions.append(expr)

        return instance

    def _with_expression(self, expr: _QueryExpression) -> "QuerySequence":
        """
        Internal method for adding a new expression to the query inner list
        using a fluent interface.

        Args:
            expr: A `_QuerySequenceExpression`.

        Returns:
            QuerySequence: The `QuerySequence` instance for method chaining.

        """
        _validate_expression_type(expr, self.__supported_query_expressions__)
        _validate_expression_unique_key(self._expressions, expr.key)

        self._expressions.append(expr)
        return self

    def with_user_metadata(self, key: str, **operator_kwargs: Any) -> "QuerySequence":
        """
        Appends a metadata filter to the query using a fluent, operator-based interface.

        This method simplifies metadata discovery by allowing direct filtering on the `user_metadata`
        dictionary of the Sequence. Each call adds a logical AND condition to the query.

        Args:
            key (str): The metadata key to filter on (e.g., "project"). Supports dot-notation
                for nested dictionary access (e.g., "vehicle.id").
            **operator_kwargs: A single keyword argument where the key is the operator
                and the value is the comparison target, e.g. `eq="value"`, `lt=100`, etc.

        Raises:
            ValueError: If no operator is provided, if multiple operators are provided in
                a single call, or if an unsupported operator is used.

        Supported Operators:
            * `eq`: Equal to
            * `neq`: Not equal to
            * `gt`: Greater than
            * `geq`: Greater than or equal to
            * `lt`: Less than
            * `leq`: Less than or equal to
            * `match`: glob-style pattern matching, see
                [QueryableString.match][mosaicolabs.query.queryable_fields.QueryableString.match]
            * `between`: Range filter (expects a list of [min, max])
            * `outside`: Range filter (expects a list of [min, max])
            * `in_`: One-of (expects a list of values)
            * `ex`: The key exist (ex=True) or does not exist (ex=False)

        Example:
            ```python
            # Find sequences for 'Apollo' project with visibility under 100m
            query = (
                QuerySequence()
                .with_user_metadata("project", eq="Apollo")
                .with_user_metadata("environment.visibility", lt=100)
            )
            results = client.query(query)
            ```

        Returns:
            QuerySequence: The current instance to support method chaining.
        """

        if len(operator_kwargs.keys()) != 1:
            raise ValueError("'with_user_metadata' accepts exactly one operator key.")
        op, value = operator_kwargs.popitem()

        qfield_inst = _make_queryable_field_intance(
            queryable_type=_QueryableDynamicValue,
            field_full_path=f"user_metadata.{key}",
            expression_type=_QuerySequenceExpression,
        )

        op_callback = getattr(qfield_inst, op)

        return self._with_expression(op_callback(value))

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
            name (str): The exact name of the sequence to match.

        Returns:
            QuerySequence: The `QuerySequence` instance for method chaining.
        """
        return self._with_expression(
            # employs explicit _QuerySequenceExpression composition for dealing with
            # special fields in data platform
            _QuerySequenceExpression("name", query_pb2.OPERATOR_EQ, name)
        )

    def with_name_match(self, name: str) -> "QuerySequence":
        """
        Adds a RegEx filter for the sequence 'name' field. Supported RegEx operations are:

        * `*`: matches a multiple (zero or more) characters, including space.
        * `?`: matches a single (exactly one) characters, including space.
        * `[]`: matches a character set. Examples: `[aeiou]` to match any vocals, or `[a-z]` to match a range
        * `#`: matches any single digit (0 — 9). Shortcut for `[0-9]`

        Supposing the server contains a sequence with name `experiment1-car` we can get it using:

        * `experiment*`
        * `[a-z]*1-car`
        * `experiment?/[a-z]*`
        * `experiment1-car`

        Note:
            If `name` contains none of the wildcards above, the operator
            is equivalent to [`.with_name()`][mosaicolabs.query.builders.QuerySequence.with_name],
            i.e. an exact match will be performed.

        Example:
            ```python
            from mosaicolabs import MosaicoClient, Topic, QuerySequence

            with MosaicoClient.connect("localhost", 6726) as client:
                # Find all sequences with name containing 'calibration_run_'
                qresponse = client.query(
                    QuerySequence().with_name_match("calibration_run_*")
                )

                # Inspect the response
                for item in qresponse:
                    print(f"Sequence: {item.sequence.name}")
                    print(f"Topics: {[topic.name for topic in item.topics]}")
            ```

        Args:
            name (str): The string pattern to search for within the sequence name.

        Returns:
            QuerySequence: The `QuerySequence` instance for method chaining.
        """
        return self._with_expression(
            # employs explicit _QuerySequenceExpression composition for dealing with
            # special fields in data platform
            _QuerySequenceExpression("name", query_pb2.OPERATOR_MATCH, f"{name}")
        )

    def with_created_timestamp(
        self,
        time_start: Optional[int] = None,
        time_end: Optional[int] = None,
    ) -> "QuerySequence":
        """
        Adds a filter for the 'created_at_ns' field using high-precision Time.

        Example:
            ```python
            from mosaicolabs import MosaicoClient, QuerySequence

            with MosaicoClient.connect("localhost", 6726) as client:
                # Find sequences created during a specific time range
                qresponse = client.query(
                    QuerySequence().with_created_timestamp(
                        time_start=1704067200_000000000, # 2024-01-01
                        time_end=1704153600_000000000    # 2024-01-02
                    )
                )

                # Inspect the response
                for item in qresponse:
                    print(f"Sequence: {item.sequence.name}")
                    print(f"Topics: {[topic.name for topic in item.topics]}")
            ```

        Args:
            time_start (Optional[int]): Optional lower bound (inclusive).
            time_end (Optional[int]): Optional upper bound (inclusive).

        Returns:
            QuerySequence: The `QuerySequence` instance for method chaining.

        Raises:
            ValueError: If both bounds are `None` or if `time_start > time_end`.
        """
        # .between() expects a list [start, end]
        if time_start is None and time_end is None:
            raise ValueError(
                "At least one among 'time_start' and 'time_end' is mandatory"
            )

        ts_int = time_start if time_start else None
        te_int = time_end if time_end else None
        # employs explicit _QuerySequenceExpression composition for dealing with
        # special fields in data platform
        if ts_int and not te_int:
            expr = _QuerySequenceExpression(
                "created_at_ns", query_pb2.OPERATOR_GE, ts_int
            )
        elif te_int and not ts_int:
            expr = _QuerySequenceExpression(
                "created_at_ns", query_pb2.OPERATOR_LE, te_int
            )
        else:
            if not ts_int or not te_int:
                raise ValueError("This is embarassing")  # will never happen
            if ts_int > te_int:
                raise ValueError("'time_start' must be less than 'time_end'.")

            expr = _QuerySequenceExpression(
                "created_at_ns", query_pb2.OPERATOR_BETWEEN, [ts_int, te_int]
            )
        return self._with_expression(expr)

    # TODO: possible to query updated_timestamps?

    # compatibility with QueryProtocol
    def _apply_to(self, msg: query_pb2.Filter):
        """Fills the `sequence` field of the `Filter` message."""
        _fill_catalog_filter(msg.sequence, self._expressions)

    def expressions(self) -> List[_QueryExpression]:
        """
        The list of query expressions.

        Returns:
            List[_QueryExpression]: The list of expressions currently stored in the query.
        """
        return self._expressions


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
                .with_user_metadata("environment.visibility", lt=50)
                .with_name_match("test_drive*"),
                # Append a filter with deep time-series data discovery and measurement time windowing
                QueryOntologyCatalog()
                .with_expression(IMU.Q.acceleration.x.gt(5.0))
                .with_expression(IMU.Q.timestamp_ns.gt(1700134567))
            )
            # Perform the server side query
            qresponse = client.query(query=query)
            # Inspect the response
            if qresponse is not None:
                # Results are automatically grouped by Sequence for easier data management
                for item in qresponse:
                    print(f"Sequence: {item.sequence.name}")
                    print(
                        f"Topics: {
                            {
                                topic.name: [
                                    (cluster.timerange.start, cluster.timerange.end)
                                    for cluster in topic.clusterize()
                                ]
                                for topic in item.topics
                            }
                        }"
                    )
        ```
    """

    def __init__(self, *queries: QueryableProtocol):
        """
        Initializes the root query with a set of sub-queries.

        Args:
            *queries (QueryableProtocol): A variable number of sub-query objects (e.g., `QueryTopic()`, `QuerySequence()`).

        Raises:
            ValueError: If duplicate query types are detected in the initial arguments.
        """
        self._queries = list(queries)

        # --- Validation ---
        # Check for duplicate query types (e.g., two QueryTopic instances)
        # as they would overwrite each other in the final `Filter` message.
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
            *queries (QueryableProtocol): Additional sub-query instances.

        Raises:
            ValueError: If an appended query type is already present in the request.

        Example:
            ```python
            from mosaicolabs import QueryOntologyCatalog, QuerySequence, Query, IMU, MosaicoClient

            # Build a filter with name pattern and metadata-related expression
            query = Query(
                # Append a filter for sequence metadata
                QuerySequence()
                .with_user_metadata("environment.visibility", lt=50)
                .with_name_match("test_drive???")
            )

            # Append a filter with deep time-series data discovery and measurement time windowing
            query.append(
                QueryOntologyCatalog()
                .with_expression(IMU.Q.acceleration.x.gt(5.0))
                .with_expression(IMU.Q.timestamp_ns.gt(1700134567))
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

    def _to_proto(self) -> query_pb2.Filter:
        """
        Builds the `Filter` message of the entire multi-domain query.

        Each contained sub-query fills its own field of the message
        (`sequence`, `topic` or `ontology`).
        """
        msg = query_pb2.Filter()
        for q in self._queries:
            q._apply_to(msg)
        return msg
