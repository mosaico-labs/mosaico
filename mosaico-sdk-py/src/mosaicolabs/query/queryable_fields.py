"""
Class-Free Queryable Fields Module.

The [`.Q` proxy][mosaicolabs.models.core.Serializable] is injected onto a
`Serializable` *class*, so building a query expression normally requires having
that class in hand (e.g. `IMU.Q.acceleration.x.gt(9.8)`). That's not always
possible: [`Unmodeled`][mosaicolabs.models.core.unmodeled.Unmodeled] data whose
schema variants you haven't resolved into a Python class have no `.Q` proxy to invoke.

The `Queryable*` classes defined in this file are the class-free escape hatch: given
just the fully-qualified, dot-notated field path (`f"{ontology_tag}.field.subfield"`)
that the `.Q` proxy would have produced internally anyway, they build the exact
same `_QueryCatalogExpression` that a resolved class's `.Q` proxy would
without needing the class at all.

Example:
    ```python
    from mosaicolabs import MosaicoClient, QueryOntologyCatalog
    from mosaicolabs.query.queryable_fields import QueryableNumeric

    with MosaicoClient.connect("localhost", 6726) as client:
        # Equivalent to `SomeResolvedClass.Q.temperature.celsius.lt(22.0)`,
        # but requires no resolved class - only the known tag and field path.
        qresponse = client.query(
            QueryOntologyCatalog().with_expression(
                QueryableNumeric("temperature_sensor.temperature.celsius").lt(22.0)
            )
        )
    ```
"""

from .expressions import _QueryCatalogExpression
from .generation.mixins import (
    _QueryableBool,
    _QueryableField,
    _QueryableNumeric,
    _QueryableString,
)

# Public types for composing queryable fields


class QueryableNumeric(_QueryableField, _QueryableNumeric):
    """
    A class-free, numeric query field addressed by its fully-qualified path.

    Supports the same comparison operators as the `.Q` proxy does for numeric
    fields: `eq()`, `neq()`, `lt()`, `leq()`, `gt()`, `geq()`, `in_()`, and
    `between()`. Values passed to any operator must be `int` or `float`.

    Use this when you know the exact server-side field path you want to filter
    on but don't have (or don't want to construct) a `Serializable` class to
    hang a `.Q` proxy off of - most commonly for
    [`Unmodeled`][mosaicolabs.models.core.unmodeled.Unmodeled] ontology data.

    Example:
        ```python
        from mosaicolabs.query.queryable_fields import QueryableNumeric

        # Equivalent to `IMU.Q.acceleration.x.gt(9.8)`, addressed by path alone.
        expr = QueryableNumeric("IMU.acceleration.x").gt(9.8)
        ```

    Args:
        path: The fully-qualified, dot-notated field path, prefixed by the
            ontology tag (e.g. `"IMU.acceleration.x"`, or
            `f"{ontology_tag}.temperature.celsius"` for an unmodeled ontology).
    """

    def __init__(self, path: str):
        super().__init__(path, _QueryCatalogExpression)


class QueryableString(_QueryableField, _QueryableString):
    """
    A class-free, string query field addressed by its fully-qualified path.

    Supports the same comparison operators as the `.Q` proxy does for string
    fields: `eq()`, `match()`, `lt()`, `leq()`, `gt()`, `geq()`, and `in_()`.
    Values passed to any operator must be `str`.

    Use this when you know the exact server-side field path you want to filter
    on but don't have (or don't want to construct) a `Serializable` class to
    hang a `.Q` proxy off of - most commonly for
    [`Unmodeled`][mosaicolabs.models.core.unmodeled.Unmodeled] ontology data.

    Example:
        ```python
        from mosaicolabs.query.queryable_fields import QueryableString

        # Equivalent to `IMU.Q.frame_id.eq("imu_link")`, addressed by path alone.
        expr = QueryableString("IMU.frame_id").eq("imu_link")
        ```

    Args:
        path: The fully-qualified, dot-notated field path, prefixed by the
            ontology tag (e.g. `"IMU.frame_id"`, or
            `f"{ontology_tag}.status.label"` for an unmodeled ontology).
    """

    def __init__(self, path: str):
        super().__init__(path, _QueryCatalogExpression)


class QueryableBool(_QueryableField, _QueryableBool):
    """
    A class-free, boolean query field addressed by its fully-qualified path.

    Supports the same comparison operators as the `.Q` proxy does for boolean
    fields: `eq()`. Values passed to any operator must be `bool`.

    Use this when you know the exact server-side field path you want to filter
    on but don't have (or don't want to construct) a `Serializable` class to
    hang a `.Q` proxy off of - most commonly for
    [`Unmodeled`][mosaicolabs.models.core.unmodeled.Unmodeled] ontology data.

    Example:
        ```python
        from mosaicolabs.query.queryable_fields import QueryableBool

        # Equivalent to `ROI.Q.do_rectify.eq(True)`, addressed by path alone.
        expr = QueryableBool("ROI.do_rectify").eq(True)
        ```

    Args:
        path: The fully-qualified, dot-notated field path, prefixed by the
            ontology tag (e.g. `"ROI.do_rectify"`, or
            `f"{ontology_tag}.status.is_online"` for an unmodeled ontology).
    """

    def __init__(self, path: str):
        super().__init__(path, _QueryCatalogExpression)
