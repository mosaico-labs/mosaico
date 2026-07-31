"""
Queryable Fields Module.

This module defines the concrete types that the `.Q` proxy of a `Serializable`
class resolves each leaf field to. When you write `IMU.Q.acceleration.x`, the
`.x` attribute you get back *is* a `QueryableNumeric` instance - one per leaf
field, one class per underlying data type (`QueryableNumeric`, `QueryableString`,
`QueryableBool`). Each class only exposes the comparison operators that make
sense for its data type (e.g. numeric fields get `.lt()`/`.gt()`/`.between()`,
string fields get `.match()`, boolean fields only get `.eq()`), so building an
expression on the wrong operator is a type error, not a runtime surprise.

Because a `Queryable*` field only needs a fully-qualified, dot-notated path
(`f"{ontology_tag}.field.subfield"`) to build its `_QueryCatalogExpression`, the
classes in this module double as a class-free escape hatch for
[Unmodeled][mosaicolabs.models.core.Unmodeled] ontology models: you can build the
exact same expression a resolved class's `.Q` proxy would produce, without
resolving (or even having) that class at all - as long as you know the field's
path and its data type.

Example:
    ```python
    from mosaicolabs import MosaicoClient, QueryOntologyCatalog
    from mosaicolabs.query.queryable_fields import QueryableNumeric

    with MosaicoClient.connect("localhost", 6726) as client:
        # Equivalent to `SomeResolvedClass.Q.temperature.celsius.lt(22.0)`,
        # but requires no resolved class - only the known tag and field path.
        qresponse = client.query(
            QueryOntologyCatalog().with_expression(
                QueryableNumeric("SomeResolvedClass.temperature.celsius").lt(22.0)
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
    The queryable type of a numeric field.

    `IMU.Q.acceleration.x` *is* a `QueryableNumeric` instance; this class is what
    you get, and what you can construct directly by path when no resolved class
    is available. It exposes the numeric operators `.eq()`, `.lt()`, ...
    Values passed to any operator must be `int` or `float`.

    Use it directly when you know the exact server-side field path you want to
    filter on but don't have (or don't want to construct) a `Serializable` class
    to hang a `.Q` proxy off of - most commonly for
    [`Unmodeled`][mosaicolabs.models.core.unmodeled.Unmodeled] ontology data.

    Example:
        ```python
        from mosaicolabs.query.queryable_fields import QueryableNumeric

        # Equivalent to `IMU.Q.acceleration.x.gt(9.8)`, addressed by path alone.
        expr = QueryableNumeric("IMU.acceleration.x").gt(9.8)
        ```

    Args:
        path (str): The fully-qualified, dot-notated field path, prefixed by the
            ontology tag (e.g. `"IMU.acceleration.x"`, or
            `f"{ontology_tag}.temperature.celsius"` for an unmodeled ontology).
    """

    def __init__(self, path: str):
        super().__init__(path, _QueryCatalogExpression)


class QueryableString(_QueryableField, _QueryableString):
    """
    The queryable type of a string field.

    `IMU.Q.frame_id` *is* a `QueryableString` instance; this class is what you
    get, and what you can construct directly by path when no resolved class is
    available. It exposes the string operators `.eq()`, `.match()`, ...
    Values passed to any operator must be `str`.

    Use it directly when you know the exact server-side field path you want to
    filter on but don't have (or don't want to construct) a `Serializable` class
    to hang a `.Q` proxy off of - most commonly for
    [`Unmodeled`][mosaicolabs.models.core.unmodeled.Unmodeled] ontology data.

    Example:
        ```python
        from mosaicolabs.query.queryable_fields import QueryableString

        # Equivalent to `IMU.Q.frame_id.eq("imu_link")`, addressed by path alone.
        expr = QueryableString("IMU.frame_id").eq("imu_link")
        ```

    Args:
        path (str): The fully-qualified, dot-notated field path, prefixed by the
            ontology tag (e.g. `"IMU.frame_id"`, or
            `f"{ontology_tag}.status.label"` for an unmodeled ontology).
    """

    def __init__(self, path: str):
        super().__init__(path, _QueryCatalogExpression)


class QueryableBool(_QueryableField, _QueryableBool):
    """
    The queryable type of a boolean field.

    `ROI.Q.do_rectify` *is* a `QueryableBool` instance; this class is what you
    get, and what you can construct directly by path when no resolved class is
    available. Booleans only support equality, so it exposes just `.eq()`.
    Values passed to the operator must be `bool`.

    Use it directly when you know the exact server-side field path you want to
    filter on but don't have (or don't want to construct) a `Serializable` class
    to hang a `.Q` proxy off of - most commonly for
    [`Unmodeled`][mosaicolabs.models.core.unmodeled.Unmodeled] ontology data.

    Example:
        ```python
        from mosaicolabs.query.queryable_fields import QueryableBool

        # Equivalent to `ROI.Q.do_rectify.eq(True)`, addressed by path alone.
        expr = QueryableBool("ROI.do_rectify").eq(True)
        ```

    Args:
        path (str): The fully-qualified, dot-notated field path, prefixed by the
            ontology tag (e.g. `"ROI.do_rectify"`, or
            `f"{ontology_tag}.status.is_online"` for an unmodeled ontology).
    """

    def __init__(self, path: str):
        super().__init__(path, _QueryCatalogExpression)
