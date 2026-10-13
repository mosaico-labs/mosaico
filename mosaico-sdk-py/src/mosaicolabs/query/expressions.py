from typing import Any, Optional

from ..proto.v1 import query_pb2


def _value_to_proto(value: Any) -> query_pb2.Value:
    """Converts a comparison value into a `Value` message."""
    # `bool` must be checked before `int`, since it is a subclass of it
    if isinstance(value, bool):
        return query_pb2.Value(boolean=value)
    if isinstance(value, int):
        return query_pb2.Value(integer=value)
    if isinstance(value, float):
        return query_pb2.Value(float=value)
    if isinstance(value, str):
        return query_pb2.Value(text=value)
    if isinstance(value, (list, tuple)) and value:
        # Lists are homogeneous, the queryable fields reject mixed types
        first = value[0]
        if isinstance(first, bool):
            return query_pb2.Value(boolean_array=query_pb2.BooleanArray(values=value))
        if isinstance(first, int):
            return query_pb2.Value(integer_array=query_pb2.IntegerArray(values=value))
        if isinstance(first, float):
            return query_pb2.Value(float_array=query_pb2.FloatArray(values=value))
        if isinstance(first, str):
            return query_pb2.Value(text_array=query_pb2.TextArray(values=value))
    raise TypeError(f"Unsupported query value: {value!r}")


class _QueryExpression:
    """
    Base class for all atomic comparison operations in the Mosaico Query DSL.

    A `_QueryExpression` represents the smallest indivisible unit of a query.
    It is typically not instantiated directly by the user, but is instead the result
    of a terminal method call on a queryable field (e.g., `.gt()`, `.eq()`, `.between()`)
    via the **`.Q` query proxy**.

    The class manages the expression lifecycle in a Query:

    1.  **Generation**: A user calls `IMU.Q.acceleration.x.gt(9.8)`, which generates
        a [`_QueryCatalogExpression`][mosaicolabs.query.expressions._QueryCatalogExpression] (a subclass of this class).
    2.  **Validation**: A [Builder][mosaicolabs.query.builders] receives the expression
        and validates its type and key uniqueness.
    3.  **Serialization**: The builder calls `._to_condition()` on the expression to
        transform it into the protobuf `Condition` message expected by the platform.

    Attributes:
        full_path (str): The complete, dot-separated path to the target field on the platform.
        op (query_pb2.Operator): The protobuf operator (e.g., `OPERATOR_EQ`, `OPERATOR_GT`, `OPERATOR_BETWEEN`).
        value (Any): The comparison value or data structure (e.g., a constant, a list for `OPERATOR_IN`, or a range for `OPERATOR_BETWEEN`).
        aggregator (Optional[query_pb2.Aggregator]): The aggregator applied to the field before the comparison (e.g., `"max"`), if any.
    """

    def __init__(
        self,
        full_path: str,
        op: query_pb2.Operator,
        value: Any,
        aggregator: Optional[query_pb2.Aggregator] = None,
    ):
        # self.key is the full path used in the final query dict, e.g., "GPS.status".
        # Aggregated expressions get the aggregator as suffix, e.g., "GPS.status.max()",
        # so that plain and aggregated expressions on the same field don't collide
        self.key = full_path
        self.aggregator = aggregator

        self.op = op
        self.value = value

    def _to_condition(self) -> query_pb2.Condition:
        """Converts the operator and value into a `Condition` message."""
        condition = query_pb2.Condition(op=self.op)
        # `OPERATOR_EX` / `OPERATOR_NEX` carry no value
        if self.value is not None:
            condition.value.CopyFrom(_value_to_proto(self.value))
        return condition


class _QueryTopicExpression(_QueryExpression):
    """
    An atomic comparison unit specialized for the **Topic Catalog** context.

    This class is utilized exclusively by the [`QueryTopic`][mosaicolabs.query.builders.QueryTopic]
    builder to filter topics based on system attributes (like `name` or `ontology_tag`)
    and nested user metadata.

    The [`QueryTopic`][mosaicolabs.query.builders.QueryTopic] class enforces that all
    provided expressions are instances of this type to prevent cross-domain query
    contamination.

    **Internal Translation Example:**

    | User Call | Internal Translation |
    | --- | --- |
    | `QueryTopic().with_user_metadata("calibrated", eq=True)` | `_QueryTopicExpression("user_metadata.calibrated", OPERATOR_EQ, True)` |
    | `QueryTopic().with_name("camera_front")` | `_QueryTopicExpression("name", OPERATOR_EQ, "camera_front")` |
    | `QueryTopic().with_ontology_tag("imu")` | `_QueryTopicExpression("ontology_tag", OPERATOR_EQ, "imu")` |

    """

    pass


class _QuerySequenceExpression(_QueryExpression):
    """
    An atomic comparison unit specialized for the **Sequence Catalog** context.

    This class represents filters targeting high-level sequence containers.
    It is the only expression type accepted by the
    [`QuerySequence`][mosaicolabs.query.builders.QuerySequence] builder.

    It handles fields such as the sequence `name`, `created_at_ns`, or custom
    entries within the sequence's `user_metadata`.

    **Internal Translation Example:**

    | User Call | Internal Translation |
    | --- | --- |
    | `QuerySequence().with_user_metadata("project", eq="Apollo")` | `_QuerySequenceExpression("user_metadata.project", OPERATOR_EQ, "Apollo")` |
    | `QuerySequence().with_name("Apollo")` | `_QuerySequenceExpression("name", OPERATOR_EQ, "Apollo")` |
    | `QuerySequence().with_created_timestamp(time_start=1704067200_000000000)` | `_QuerySequenceExpression("created_at_ns", OPERATOR_GE, 1704067200_000000000)` |

    """

    pass


class _QueryCatalogExpression(_QueryExpression):
    """
    An atomic comparison unit specialized for **Data Ontology** (Sensor Payload) filtering.

    This expression type is used by the
    [`QueryOntologyCatalog`][mosaicolabs.query.builders.QueryOntologyCatalog] builder
    to filter actual sensor data values across the entire platform.

    Because ontology queries target specific fields within a sensor payload (e.g.,
    accelerometer readings), these expressions use fully qualified dot-notated paths
    prefixed by the ontology tag.

    **Internal Translation Example:**

    | User Call | Internal Translation |
    | --- | --- |
    | `IMU.Q.acceleration.x.gt(9.8)` | `_QueryCatalogExpression("IMU.acceleration.x", OPERATOR_GT, 9.8)` |
    | `IMU.Q.acceleration.y.gt(9.8)` | `_QueryCatalogExpression("IMU.acceleration.y", OPERATOR_GT, 9.8)` |
    | `IMU.Q.acceleration.z.gt(9.8)` | `_QueryCatalogExpression("IMU.acceleration.z", OPERATOR_GT, 9.8)` |

    """

    def get_expr_tag(self) -> str:
        """
        Returns the ontology tag the expression refers to as a string. For example:
        IMU.Q.acceleration.x.gt(9.8) -> imu
        GPS.Q.position.x.lt(1.0) -> gps
        """
        return self.key.split(".")[0]

    def _to_predicate(self) -> query_pb2.OntologyPredicate:
        """Converts the expression into an `OntologyPredicate` message."""
        return query_pb2.OntologyPredicate(
            field=self.key,
            condition=self._to_condition(),
            aggregator=self.aggregator,
        )
