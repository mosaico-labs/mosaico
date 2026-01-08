from typing import Any, Dict, List, Optional, Tuple, Type, get_origin

# Import custom types used in helper methods
from mosaicolabs.models import Time
from .protocols import QueryableProtocol

# Import the building blocks for expressions and how they are combined
from .expressions import (
    _QueryCatalogExpression,
    _QueryTopicExpression,
    _QuerySequenceExpression,
    _QueryCombinator,
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


class QueryOntologyCatalog:
    """
    A top-level query object for data catalog, that combines multiple
    expressions with a logical AND.

    This query type produces a "flat" dictionary output where field paths
    are dot-notated (e.g., "gps.timestamp_ns").
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
        Initializes the query with an optional set of initial expressions.

        Args:
            *expressions: A variable number of _QueryCatalogExpression objects.
            include_timestamp_range (Optional[bool]): If True, the server returns the start
                and end timestamps corresponding to the first and last time the queried
                condition occurred.
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
        Adds a new expression to the query (fluent interface).

        Args:
            expr: A _QueryCatalogExpression, e.g., GPS.Q.satellites.leq(10).

        Returns:
            The QueryOntologyCatalog instance for method chaining.
        """
        _validate_expression_type(
            expr,
            self.__supported_query_expressions__,
        )
        _validate_expression_operator_format(expr)
        _validate_expression_unique_key(self._expressions, expr.key)

        self._expressions.append(expr)
        return self

    def with_message_timestamp(
        self,
        ontology_type: object,
        time_start: Optional[Time] = None,
        time_end: Optional[Time] = None,
    ) -> "QueryOntologyCatalog":
        """Helper method to add a filter for the 'creation_unix_timestamp' field."""
        # .between() expects a list [start, end]
        if time_start is None and time_end is None:
            raise ValueError(
                "At least one among 'time_start' and 'time_end' is mandatory"
            )

        ts_int = time_start.to_nanoseconds() if time_start else None
        te_int = time_end.to_nanoseconds() if time_end else None
        # special fields in data platform
        if not hasattr(ontology_type, "__ontology_tag__"):
            raise ValueError("Only Serializable types can be used as 'ontology_type'")
        sensor_tag = getattr(ontology_type, "__ontology_tag__")
        if ts_int and not te_int:
            expr = _QueryCatalogExpression(f"{sensor_tag}.timestamp_ns", "$geq", ts_int)
        elif te_int and not ts_int:
            expr = _QueryCatalogExpression(f"{sensor_tag}.timestamp_ns", "$leq", te_int)
        else:
            if not ts_int or not te_int:
                raise ValueError(
                    "This is embarassing"
                )  # will never happen (fix IDE complaining)
            if ts_int > te_int:
                raise ValueError("'time_start' must be less than 'time_end'.")

            expr = _QueryCatalogExpression(
                f"{sensor_tag}.timestamp_ns", "$between", [ts_int, te_int]
            )
        return self.with_expression(expr)

    def with_data_timestamp(
        self,
        ontology_type: type,
        time_start: Optional[Time] = None,
        time_end: Optional[Time] = None,
    ) -> "QueryOntologyCatalog":
        """Helper method to add a filter for the 'creation_unix_timestamp' field."""
        # .between() expects a list [start, end]
        if time_start is None and time_end is None:
            raise ValueError(
                "At least one among 'time_start' and 'time_end' is mandatory"
            )

        # special fields in data platform
        if not hasattr(ontology_type, "__ontology_tag__"):
            raise ValueError(
                f"Only Serializable types can be used as 'ontology_type' class '{ontology_type.__name__}'"
            )
        sensor_tag = getattr(ontology_type, "__ontology_tag__")
        if time_start is not None and time_end is None:
            expr1 = _QueryCatalogExpression(
                f"{sensor_tag}.header.stamp.sec", "$geq", time_start.sec
            )
            expr2 = _QueryCatalogExpression(
                f"{sensor_tag}.header.stamp.nanosec", "$geq", time_start.nanosec
            )
        elif time_end is not None and time_start is None:
            expr1 = _QueryCatalogExpression(
                f"{sensor_tag}.header.stamp.sec", "$leq", time_end.sec
            )
            expr2 = _QueryCatalogExpression(
                f"{sensor_tag}.header.stamp.nanosec", "$leq", time_end.nanosec
            )
        else:
            if not time_start or not time_end:
                raise ValueError("This is embarassing")  # will never happen
            if time_start.to_nanoseconds() > time_end.to_nanoseconds():
                raise ValueError("'time_start' must be less than 'time_end'.")

            expr1 = _QueryCatalogExpression(
                f"{sensor_tag}.header.stamp.sec",
                "$between",
                [time_start.sec, time_end.sec],
            )
            expr2 = _QueryCatalogExpression(
                f"{sensor_tag}.header.stamp.nanosec",
                "$between",
                [time_start.nanosec, time_end.nanosec],
            )
        return self.with_expression(expr1).with_expression(expr2)

    # compatibility with QueryProtocol
    def name(self) -> str:
        """Returns the top-level key for the ontology catalog query."""
        return "ontology"

    # compatibility with QueryProtocol
    def to_dict(self) -> Dict[str, Any]:
        """
        Converts all contained expressions into a single dictionary.
        Uses _QueryCombinator to merge expressions, e.g.:
        {"gps.timestamp_ns": {"$between": [...]}, "gps.satellites": {"$leq": 10}}
        """
        query_dict = _QueryCombinator(list(self._expressions)).to_dict()
        if self._include_tstamp_range:
            query_dict.update({"include_timestamp_range": self._include_tstamp_range})
        return query_dict


class QueryTopic:
    """
    A top-level query object for Topic data.
    Combines multiple expressions with a logical AND.

    This query type produces a "nested" dictionary output, with special
    handling for dictionary fields like 'user_metadata'.
    """

    __supported_query_expressions__: Tuple[Type[_QueryExpression], ...] = (
        _QueryTopicExpression,
    )

    def __init__(self, *expressions: "_QueryExpression"):
        """
        Initializes the query with an optional set of initial expressions.

        Args:
            *expressions: A variable number of _QueryTopicExpression objects.
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
        Adds a new expression to the query (fluent interface).

        This is the preferred, modern way to add any filter, including
        for nested user_metadata keys, e.g.:
        .with_expression(Topic.Q.user_metadata["firmware.version"].eq("v0.1.2"))

        Args:
            expr: A _QueryTopicExpression, Topic.Q.user_metadata["key"].eq("...").

        Returns:
            The QueryTopic instance for method chaining.
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
        Helper method to add a filter for the topic 'name' field.
        """
        return self.with_expression(_QueryTopicExpression("name", "$eq", f"{name}"))

    def with_name_match(self, name: str) -> "QueryTopic":
        """
        Helper method to add a filter for the topic 'name' field.
        The Query performs an 'in-between' search on the topic name (%name%)
        (remotely the topic name is 'sequence/topic')
        """
        return self.with_expression(
            # employs explicit _QueryTopicExpression composition for dealing with
            # special fields in data platform
            _QueryTopicExpression("name", "$match", f"{name}")
        )

    def with_ontology_tag(self, ontology_tag: str) -> "QueryTopic":
        """Helper method to add a filter for the 'ontology_tag' field."""
        return self.with_expression(
            # employs explicit _QueryTopicExpression composition for dealing with
            # special fields in data platform
            _QueryTopicExpression("ontology_tag", "$eq", ontology_tag)
        )

    def with_created_timestamp(
        self, time_start: Optional[Time] = None, time_end: Optional[Time] = None
    ) -> "QueryTopic":
        """Helper method to add a filter for the 'creation_unix_timestamp' field."""
        # .between() expects a list [start, end]
        if time_start is None and time_end is None:
            raise ValueError(
                "At least one among 'time_start' and 'time_end' is mandatory"
            )

        ts_int = time_start.to_milliseconds() if time_start else None
        te_int = time_end.to_milliseconds() if time_end else None
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
        """Returns the top-level key for the topic query."""
        return "topic"

    # compatibility with QueryProtocol
    def to_dict(self) -> Dict[str, Any]:
        """
        Converts the query to a nested dictionary.

        This method is more complex than the others because it must
        partition expressions:
        1. "Normal" fields (e.g., "name") go in a flat dictionary.
        2. "Metadata" fields (e.g., "user_metadata.mission") are
           collected and nested under a "user_metadata" key.
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
    A top-level query object for Sequence metadata.
    Combines multiple expressions with a logical AND.

    This query type produces a "flat" dictionary output.
    """

    __supported_query_expressions__: Tuple[Type[_QueryExpression], ...] = (
        _QuerySequenceExpression,
    )

    def __init__(self, *expressions: "_QueryExpression"):
        """
        Initializes the query with an optional set of initial expressions.

        Args:
            *expressions: A variable number of _QuerySequenceExpression objects.
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
        Adds a new expression to the query (fluent interface).

        Args:
            expr: A _QuerySequenceExpression, e.g., Sequence.Q.user_metadata["key"].eq("...").

        Returns:
            The QuerySequence instance for method chaining.
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
        """Helper method to add a filter for the sequence exact 'name' field."""
        return self.with_expression(
            # employs explicit _QuerySequenceExpression composition for dealing with
            # special fields in data platform
            _QuerySequenceExpression("name", "$eq", name)
        )

    def with_name_match(self, name: str) -> "QuerySequence":
        """
        Helper method to add a filter for the sequence 'name' field.
        The Query performs an 'in-between' search on the sequence name (%name%)
        """
        return self.with_expression(
            # employs explicit _QuerySequenceExpression composition for dealing with
            # special fields in data platform
            _QuerySequenceExpression("name", "$match", f"{name}")
        )

    def with_created_timestamp(
        self, time_start: Optional[Time] = None, time_end: Optional[Time] = None
    ) -> "QuerySequence":
        """Helper method to add a filter for the 'creation_unix_timestamp' field."""
        # .between() expects a list [start, end]
        if time_start is None and time_end is None:
            raise ValueError(
                "At least one among 'time_start' and 'time_end' is mandatory"
            )

        ts_int = time_start.to_milliseconds() if time_start else None
        te_int = time_end.to_milliseconds() if time_end else None
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
        """Returns the top-level key for the sequence query."""
        return "sequence"

        # compatibility with QueryProtocol

    def to_dict(self) -> Dict[str, Any]:
        """
        Converts the query to a nested dictionary.

        This method is more complex than the others because it must
        partition expressions:
        1. "Normal" fields (e.g., "name") go in a flat dictionary.
        2. "Metadata" fields (e.g., "user_metadata.mission") are
           collected and nested under a "user_metadata" key.
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
    A top-level "root" query object that combines multiple sub-queries
    (like QueryTopic, QueryOntologyCatalog) into a single request body.
    """

    def __init__(self, *queries: QueryableProtocol):
        """
        Initializes the root query.

        Args:
            *queries: A variable number of _QueryBase objects (e.g.,
                      QueryTopic(), QueryOntologyCatalog()).
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
        Serializes the entire query into the final JSON dictionary.

        It calls .name() and .to_dict() on each sub-query.
        Example output:
        {
            "topic": { ... topic filters ... },
            "ontology": { ... ontology filters ... }
        }
        """
        # Uses a dictionary comprehension to build the final object
        return {q.name(): q.to_dict() for q in self._queries}
