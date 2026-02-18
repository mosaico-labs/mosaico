from typing import Any, Dict


class _QueryExpression:
    """
    Base class for all atomic comparison operations in the Mosaico Query DSL.

    A `_QueryExpression` represents the smallest indivisible unit of a query.
    It is typically not instantiated directly by the user, but is instead the result
    of a terminal method call on a queryable field (e.g., `.gt()`, `.eq()`, `.between()`)
    via the **`.Q` query proxy**.

    The class manages the expression lifecycle in a Query:

    1.  **Generation**: A user calls `IMU.Q.acceleration.x.gt(9.8)`, which generates
        a [`_QueryCatalogExpression`][mosaicolabs.models.query.expressions._QueryCatalogExpression] (a subclass of this class).
    2.  **Validation**: A [Builder][mosaicolabs.models.query.builders] receives the expression
        and validates its type, operator format, and key uniqueness.
    3.  **Serialization**: The builder calls `.to_dict()` on the expression to
        transform it into the specific JSON format expected by the platform.

    Attributes:
        full_path: The complete, dot-separated path to the target field on the platform.
        op: The Mosaico-compliant operator string (e.g., `"$eq"`, `"$gt"`, `"$between"`).
        value: The comparison value or data structure (e.g., a constant, a list for `$in`, or a range for `$between`).
    """

    def __init__(self, full_path: str, op: str, value: Any):
        """
        Initializes an atomic comparison.

        Args:
            full_path: The dot-separated field path used in the final query dictionary.
            op: The short-string operation identifier (must start with `$`).
            value: The constant value for the comparison.
        """
        # self.key is the full path used in the final query dict, e.g., "GPS.status"
        self.key = full_path

        self.op = op
        self.value = value

    def to_dict(self) -> Dict[str, Any]:
        """
        Converts the expression into its final dictionary format.

        Example:
            `{"gps.status.service": {"$eq": 0}}`
        """
        return {self.key: {self.op: self.value}}


class _QueryTopicExpression(_QueryExpression):
    """
    An atomic comparison unit specialized for the **Topic Catalog** context.

    This class is utilized exclusively by the [`QueryTopic`][mosaicolabs.models.query.builders.QueryTopic]
    builder to filter topics based on system attributes (like `name` or `ontology_tag`)
    and nested user metadata.

    The [`QueryTopic`][mosaicolabs.models.query.builders.QueryTopic] class enforces that all
    provided expressions are instances of this type to prevent cross-domain query
    contamination.

    **Internal Translation Example:**

    | User Call | Internal Translation |
    | --- | --- |
    | `Topic.Q.user_metadata["calibrated"].eq(True)` | `_QueryTopicExpression("user_metadata.calibrated", "$eq", True)` |
    | `QueryTopic().with_name("camera_front")` | `_QueryTopicExpression("name", "$eq", "camera_front")` |
    | `QueryTopic().with_ontology_tag("imu")` | `_QueryTopicExpression("ontology_tag", "$eq", "imu")` |

    """

    pass


class _QuerySequenceExpression(_QueryExpression):
    """
    An atomic comparison unit specialized for the **Sequence Catalog** context.

    This class represents filters targeting high-level sequence containers.
    It is the only expression type accepted by the
    [`QuerySequence`][mosaicolabs.models.query.builders.QuerySequence] builder.

    It handles fields such as the sequence `name`, `created_timestamp`, or custom
    entries within the sequence's `user_metadata`.

    **Internal Translation Example:**

    | User Call | Internal Translation |
    | --- | --- |
    | `Sequence.Q.user_metadata["project"].eq("Apollo")` | `_QuerySequenceExpression("user_metadata.project", "$eq", "Apollo")` |
    | `QuerySequence().with_name("Apollo")` | `_QuerySequenceExpression("name", "$eq", "Apollo")` |
    | `QuerySequence().with_created_timestamp(Time.from_float(1704067200.0))` | `_QuerySequenceExpression("created_timestamp", "$between", [1704067200.0, None])` |

    """

    pass


class _QueryCatalogExpression(_QueryExpression):
    """
    An atomic comparison unit specialized for **Data Ontology** (Sensor Payload) filtering.

    This expression type is used by the
    [`QueryOntologyCatalog`][mosaicolabs.models.query.builders.QueryOntologyCatalog] builder
    to filter actual sensor data values across the entire platform.

    Because ontology queries target specific fields within a sensor payload (e.g.,
    accelerometer readings), these expressions use fully qualified dot-notated paths
    prefixed by the ontology tag.

    **Internal Translation Example:**

    | User Call | Internal Translation |
    | --- | --- |
    | `IMU.Q.acceleration.x.gt(9.8)` | `_QueryCatalogExpression("imu.acceleration.x", "$gt", 9.8)` |
    | `IMU.Q.acceleration.y.gt(9.8)` | `_QueryCatalogExpression("imu.acceleration.y", "$gt", 9.8)` |
    | `IMU.Q.acceleration.z.gt(9.8)` | `_QueryCatalogExpression("imu.acceleration.z", "$gt", 9.8)` |
    | `IMU.Q.acceleration.x.gt(9.8)` | `_QueryCatalogExpression("imu.acceleration.x", "$gt", 9.8)` |

    """

    pass
