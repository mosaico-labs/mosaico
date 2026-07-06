import datetime
import inspect
from typing import Any, ClassVar, Dict, Optional, Type, TypeVar, Union

import pyarrow as pa

from mosaicolabs.models.query.generation.internal import (
    _PYTHON_TYPE_TO_QUERYABLE,
)
from mosaicolabs.models.query.generation.mixins import (
    _make_queryable_field_type,
    _QueryableField,
    _QueryableUnsupported,
)

from ..expressions import _QueryCatalogExpression
from ..protocols import FieldMapperProtocol
from .internal import _QueryableList

# -------------------------------------------------------------------------
# Pyarrow Type to Python Type Mapping
# This dictionary maps specific PyArrow data types to their corresponding
# python types.
# -------------------------------------------------------------------------
_PYARROW_TO_PYTHON_TYPE: Dict[pa.DataType, type] = {
    # Boolean types
    pa.bool_(): bool,
    # Numeric types → use _QueryableNumeric
    pa.int8(): int,
    pa.int16(): int,
    pa.int32(): int,
    pa.int64(): int,
    pa.uint8(): int,
    pa.uint16(): int,
    pa.uint32(): int,
    pa.uint64(): int,
    pa.float16(): float,
    pa.float32(): float,
    pa.float64(): float,
    # Date/time types
    pa.date32(): datetime.date,
    pa.date64(): datetime.date,
    pa.time32("s"): datetime.time,
    pa.time32("ms"): datetime.time,
    pa.time64("us"): datetime.time,
    pa.time64("ns"): datetime.time,
    pa.timestamp("s"): datetime.datetime,
    pa.timestamp("ms"): datetime.datetime,
    pa.timestamp("us"): datetime.datetime,
    pa.timestamp("ns"): datetime.datetime,
    # String types
    pa.string(): str,
    pa.large_string(): str,
}

SUPPORTED_LIST_OPERATIONS = ["?", "!"]


def _pyarrow_to_queryable(ptype: pa.DataType):
    """
    Returns the _Queryable* mixin type, given a pyarrow type instance.
    e.g. pa.string() -> _QueryableString
    """
    return _PYTHON_TYPE_TO_QUERYABLE.get(
        _PYARROW_TO_PYTHON_TYPE.get(ptype, None),  # return none if not found
        _QueryableUnsupported,  # further safety get
    )


class _QueryProxy:
    """
    A dynamic proxy object that is attached to Ontology classes (as .Q).
    It intercepts attribute access (like .position or .status) to
    build nested query paths and provide custom error messages.

    The proxy is created by the `queryable` decorator and is available as a
    class attribute named ``Q``.

    Example:
        ```python
        from mosaicolabs import IMU
        imu_query_proxy = IMU.Q
        imu_query_proxy.acceleration.x >= 1.234

        # This is a _QueryProxy instance
        imu_query_proxy.acceleration

        # This is a _QueryableField instance
        imu_query_proxy.acceleration.x
        ```
    """

    def __init__(self, full_path: str, field_map: Dict[str, Any]):
        """
        Initializes the dynamic proxy.

        Args:
            full_path (str): The query path *so far* (e.g., "GPS" or "GPS.position").
            field_map (Dict[str, Any]): A nested dict of valid child fields.
                - Values are _QueryableField for simple fields.
                - Values are other dicts for nested structs.
        """
        # Use mangled names (double underscore) to hide them from __getattr__
        # and prevent recursion loops.
        self.__path__ = full_path
        self.__map__ = field_map

    def _create_queriable_field(
        self, full_path: str, field_type: pa.DataType
    ) -> _QueryableField:
        """
        Builds and returns a queryable field instance for a leaf node.

        Args:
            full_path (str): The fully-qualified query path for the field (e.g., ``"GPS.position.lat"``).
            field_type (pa.DataType): The PyArrow type of the field, used to select the
                appropriate ``_Queryable*`` mixin (e.g., ``_QueryableNumeric`` for floats).

        Returns:
            A dynamically-created ``_QueryableField`` subclass instance, ready to be
            used in query expressions.
        """

        mixin = _pyarrow_to_queryable(field_type)

        cls = _make_queryable_field_type(mixin)

        # Instantiate the dynamically created class with its path
        return cls(full_path=full_path, expr_cls=_QueryCatalogExpression)

    def _add_list_expression(
        self, list_expression: str
    ) -> Union["_QueryProxy", _QueryableField]:
        """
        Appends a list access expression to the current path and returns the next QueryProxy in case
        of _QueryableList or field in case of pa.ListType.

        Supported expressions are:
        - ``"?"`` — any-element quantifier (matches if *any* element satisfies the condition).
        - ``"!"`` — all-element quantifier (matches if *all* elements satisfy the condition).
        - A digit string (e.g. ``"0"``) — index access for a specific element.

        Args:
            list_expression (str): The access expression to append.

        Returns:
            - ``_QueryProxy`` when the list contains a _QueryableList (contains nested map)
            - ``_QueryableField`` when it contains a pa.ListType (contains basic datatype)

        Raises:
            ValueError: If ``list_expression`` is not a supported operator and not a digit.
            TypeError: If the current field is not a list type (``pa.ListType`` or ``_QueryableList``).
        """

        # Check that list expression is supported
        if (
            list_expression not in SUPPORTED_LIST_OPERATIONS
            and not list_expression.isdigit()
        ):
            raise ValueError(f"{list_expression} operation is not supported for lists")

        # Check that current map is a List (either QueryableList or pa.ListType)
        if not isinstance(self.__map__, (_QueryableList, pa.ListType)):
            raise TypeError(
                f"Field '{self.__path__}' is not a list. Cannot be indexed."
            )

        # Append list operation among allowed ("?", "!", [i]) to overall path
        path_w_list_expr = f"{self.__path__}[{list_expression}]"

        # Here two cases can happen:
        #   1) List contains a complex struct -> return a QueryProxy downcasting the _QueryableList to dict and allow dot notiation to work
        #   2) List contains a basic type -> create queryable field
        if isinstance(self.__map__, _QueryableList):
            return _QueryProxy(
                full_path=path_w_list_expr,
                field_map=dict(
                    self.__map__
                ),  # Downcast __map__ to avoid confusing it as a _QueryableList
            )
        elif isinstance(self.__map__, pa.ListType):
            field_type = self.__map__.value_type
            queriable_field = self._create_queriable_field(path_w_list_expr, field_type)

            return queriable_field

        else:  # this should not be possible
            raise TypeError(
                f"{self.__map__} is not a {pa.ListType.__name__} or {_QueryableList.__name__}"
            )

    def __getattr__(self, name: str) -> Any:
        """
        Called at runtime when accessing an attribute (e.g., GPS.Q.position).

        Args:
            name (str): The name of the attribute being accessed (e.g., "position").

        Returns:
            Union[_QueryProxy, _QueryableField]: Either a new QueryProxy for a nested struct, or a
            _QueryableField for a simple field.

        Raises:
            AttributeError: If the 'name' is not a valid field in the map,
                            providing a helpful error message.
        """

        if isinstance(self.__map__, (pa.ListType, _QueryableList)):
            raise AttributeError(
                f"Field '{self.__path__}' is a list. "
                f"Use .any(), .all(), or [i] to select elements before accessing sub-fields."
            )

        if name not in self.__map__:
            # Attribute is invalid. Raise a helpful error.
            raise AttributeError(
                f"Invalid field '{name}' for path '{self.__path__}'. "
                f"Available fields: {self.queryable_fields}"
            )

        # Retrieve the child object from the map
        child = self.__map__[name]

        if isinstance(child, (dict, pa.ListType)):
            # This is a nested struct (e.g., 'position').
            # Return a *new* QueryProxy instance for this deeper path.
            return _QueryProxy(
                full_path=f"{self.__path__}.{name}",  # e.g., "gps.position"
                field_map=child,  # The nested field map
            )
        else:
            # This is a simple field (a _QueryableField instance).
            # Return it directly.
            # (e.g., accessing IMU.Q.acceleration.x returns _QueryableField("IMU.Q.acceleration.x"))

            field_type = child

            # Instantiate the dynamically created class with its path
            queriable_field = self._create_queriable_field(
                f"{self.__path__}.{name}", field_type
            )

            return queriable_field

    def __getitem__(self, key) -> Union["_QueryProxy", _QueryableField]:
        """
        Accesses a specific element of the list field by integer index.

        Args:
            key (int): The zero-based index of the element to access.

        Returns:
            Union[_QueryProxy, _QueryableField]: A new proxy or queryable field rooted at
            the indexed element (e.g., path becomes ``"tags[0]"``).

        Raises:
            TypeError: If ``key`` is not an integer.
        """

        if not isinstance(key, int):
            raise TypeError(
                f"List index must be an integer, got '{type(key).__name__}'"
            )

        return self._add_list_expression(f"{key}")

    def any(self) -> Union["_QueryProxy", _QueryableField]:
        """
        Returns a proxy or field scoped to the *any-element* quantifier (``[?]``).

        Use this when the condition should match if **at least one** element in the list
        satisfies it.

        Example::

            RobotPath.Q.poses.any().position.x.gt(1.0)

        Returns:
            Union[_QueryProxy, _QueryableField]: A new proxy or queryable field whose path
            ends with ``[?]`` (e.g., ``"poses[?]"``).

        Raises:
            TypeError: If the current field is not a list.
        """
        return self._add_list_expression("?")

    def all(self):
        """
        Returns a proxy or field scoped to the *all-elements* quantifier (``[!]``).

        Use this when the condition should match only if **every** element in the list
        satisfies it.

        Example::

            RobotPath.Q.poses.all().position.x.gt(1.0)

        Returns:
            Union[_QueryProxy, _QueryableField]: A new proxy or queryable field whose path
            ends with ``[!]`` (e.g., ``"poses[!]"``).

        Raises:
            TypeError: If the current field is not a list.
        """

        return self._add_list_expression("!")

    @property
    def queryable_fields(self):
        result = []
        for key, val in self.__map__.items():
            if isinstance(
                val, (dict, pa.ListType)
            ):  # nested struct, _QueryableList or pa.ListType
                result.append(key)
            elif isinstance(val, pa.DataType):  # (pa.DataType, expr_cls)
                field_type = val
                if _pyarrow_to_queryable(field_type) is not _QueryableUnsupported:
                    result.append(key)
        return result


# --- The General _QueryProxyMixin ---
class _QueryProxyMixin:
    """
    A mixin class that provides query proxy generation capabilities
    to any class that defines a PyArrow '__msco_pyarrow_struct__' and provides a
    root query prefix (like a '__ontology_tag__').

    The query proxy is available as a class attribute named ``Q``.
    """

    # Class variable, because it is expected to use like: 'IMU.Q.acceleration.x >= 1.234'
    Q: ClassVar[_QueryProxy]
    """The query proxy for the model."""

    @staticmethod
    def _inject_query_proxy(
        class_type: Type,
        mapper: FieldMapperProtocol,
        query_prefix: Optional[str] = None,
    ):
        """
        Static helper to build and inject the .Q query proxy.
        This is called by the default case or by custom subclasses.
        """
        # Build the nested field map using the provided mapper
        query_prefix, field_map = mapper.build_map(
            class_type,
            path_prefix=query_prefix,
        )

        # Create the root QueryProxy instance
        root_proxy = _QueryProxy(
            full_path=query_prefix,
            field_map=field_map,
        )

        # Attach the live proxy instance to the class
        setattr(class_type, "Q", root_proxy)


# Use a generic type to instruct the interpreter that the decorator returns the very same type
# This helps the discovery of the fields of pydantic classes decorated via @queryable()
T = TypeVar("T")


def queryable(
    mapper_type: Type[FieldMapperProtocol],
    prefix: Optional[str] = None,
    **kwargs,
):
    """
    Class decorator to build and inject the .Q proxy.

    Args:
        mapper_type (Type[FieldMapperProtocol]): The type of mapper to use.
        query_expression_type (Type[_QueryExpression]): The type of query expression to use.
        prefix (Optional[str]): The prefix to use for the query.
        **kwargs: Additional keyword arguments to pass to the mapper.
    """

    def decorator(cls: Type[T]) -> Type[T]:
        # Determine the query prefix
        # Call the injection helper
        _QueryProxyMixin._inject_query_proxy(cls, mapper_type(**kwargs), prefix)
        return cls

    return decorator


def is_model_queryable(model: Type[Any]) -> bool:
    """
    Checks if the given model is a class that inherits from QueryableModel.
    """
    # 1. Ensure 'model' is actually a class (type) and not an instance.
    if not inspect.isclass(model):
        return False

    # 2. Check if it inherits from QueryableModel at any level.
    return issubclass(model, _QueryProxyMixin)
