import inspect
from typing import Any, ClassVar, Dict, Optional, Type, TypeVar, Union

import pyarrow as pa

from ..expressions import _QueryCatalogExpression
from ..protocols import FieldMapperProtocol
from .internal import (
    _PYARROW_TO_PYTHON_BASE_TYPE,
    _PYTHON_TYPE_TO_QUERYABLE_MIXIN,
    _QueryableList,
)
from .mixins import (
    _make_queryable_field_type,
    _QueryableField,
    _QueryableUnsupported,
)

SUPPORTED_LIST_OPERATIONS = ["?", "!"]
SUPPORTED_PYARROW_LIST_TYPES = (pa.ListType, pa.LargeListType, pa.FixedSizeListType)


def _pyarrow_to_queryable_mixin(ptype: pa.DataType):
    """
    Returns the _Queryable* mixin type, given a pyarrow type instance.
    e.g. pa.string() -> _QueryableString
    """
    return _PYTHON_TYPE_TO_QUERYABLE_MIXIN.get(
        _PYARROW_TO_PYTHON_BASE_TYPE.get(ptype, None),  # return none if not found
        _QueryableUnsupported,  # further safety get
    )


def _make_field(full_path: str, field_type: pa.DataType) -> _QueryableField:
    """
    Builds and returns a queryable field instance for a leaf node.

    Args:
        full_path (str): The fully-qualified query path for the field (e.g., ``"GPS.position.lat"``).
        field_type (pa.DataType): The PyArrow type of the field, used to select the
            appropriate ``_Queryable*`` mixin (e.g., ``_QueryableNumeric`` for floats).

    Returns:
        _QueryableField: A dynamically-created ``_QueryableField`` subclass instance, ready to be
            used in query expressions.
    """
    cls = _make_queryable_field_type(_pyarrow_to_queryable_mixin(field_type))
    return cls(full_path=full_path, expr_cls=_QueryCatalogExpression)


def _innermost_type(field_type: pa.DataType) -> pa.DataType:
    """
    Returns the type of the elements of a list, at any depth for a list of lists.
    """
    while isinstance(field_type, SUPPORTED_PYARROW_LIST_TYPES):
        field_type = field_type.value_type
    return field_type


def _is_dunder(name: str) -> bool:
    """
    Whether `name` is a special attribute (e.g. `__deepcopy__`), never a field.

    The `__getattr__` of the proxies must reject them before reading the proxy
    state: `copy` and `pickle` look them up on instances created without
    `__init__`, where reading `__path__` would call `__getattr__` again forever.
    """
    return name.startswith("__") and name.endswith("__")


class _QueryNode:
    """
    Base class of the nodes of the ``.Q`` proxy tree: holds the path and the schema
    of the node, and describes its queryable fields.
    """

    def __init__(
        self, full_path: str, field_schema: Union[Dict[str, pa.DataType], pa.DataType]
    ):
        """
        Initializes the node.

        Args:
            full_path (str): The query path *so far* (e.g., "GPS" or "GPS.position").
            field_schema (Union[Dict[str, pa.DataType], pa.DataType]): A nested schema of
                valid child fields.
                - Values are pa.DataType for simple fields.
                - Values are other dicts for nested structs.
        """
        # Use mangled names (double underscore) to hide them from __getattr__
        # and prevent recursion loops.
        self.__path__ = full_path
        self.__schema__ = field_schema

    @property
    def _queryable_fields(self) -> list[str]:
        """
        Returns the list of the queryable fields supported by this mapping.

        Returns:
            List[str]: A list of strings describing the queryable fields
        """

        result = []
        if isinstance(self.__schema__, dict):
            for key, val in self.__schema__.items():
                if isinstance(
                    val, (dict, *SUPPORTED_PYARROW_LIST_TYPES)
                ):  # nested struct, _QueryableList or pa.ListType or pa.LargeListType or pa.FixedSizeListType
                    result.append(key)
                elif isinstance(val, pa.DataType):
                    if _pyarrow_to_queryable_mixin(val) is not _QueryableUnsupported:
                        result.append(key)
        # It is a pa.DataType: must check to be queryable
        elif _pyarrow_to_queryable_mixin(self.__schema__) is not _QueryableUnsupported:
            result.append(self.__schema__)
        return result

    @property
    def _queryable_schema(self) -> Union[Dict[str, Any], None]:
        """
        Returns the schema of the queryable fields supported by this mapping.

        The schema mirrors the hierarchical structure of ``self.__schema__``. Nested
        mappings are represented as nested dictionaries, while leaf nodes are
        represented as tuples containing the names of the supported Python types for
        that field.

        Returns:
            Union[Dict[str, Any], None]: A nested dictionary describing the queryable structure. Intermediate
                nodes are dictionaries, and leaf nodes are tuples of type names.
        """

        return self._infer_queryable_schema(self.__schema__)

    def _infer_queryable_schema(self, schema) -> Union[Dict[str, Any], None]:
        """
        Recursively infers the schema of a queryable mapping.

        Traverses a nested mapping and preserves its structure. Dictionary nodes are
        recursively expanded, while leaf objects are converted into tuples
        containing the names of their supported Python types, as defined by their
        ``__mixin_supported_types__`` attribute.

        Args:
            schema: The nested schema mapping.

        Returns:
            Union[Dict[str, Any], None]: A nested dictionary preserving the original hierarchy, where leaf values
                are tuples of supported type names.
        """
        if isinstance(schema, _QueryableList):  # A List of structs
            return {
                "type": _QueryableList.__name__,
                "supported_types": (list.__name__,),
                "struct": {
                    k: self._infer_queryable_schema(v) for k, v in schema.items()
                },
            }
        elif isinstance(schema, dict):  # A Nested stuct (pa.StructType)
            return {k: self._infer_queryable_schema(v) for k, v in schema.items()}
        elif isinstance(
            schema,
            SUPPORTED_PYARROW_LIST_TYPES,
        ):  # A base list (pa.ListType or pa.LargeListType or pa.FixedSizeListType)
            return {
                "type": type(schema).__name__,
                "supported_types": (list.__name__,),
            }
        # A base type (pa.DataType)
        queryable_type = _pyarrow_to_queryable_mixin(schema)
        if queryable_type is _QueryableUnsupported:
            return None
        return {
            "type": queryable_type.__name__,
            "supported_types": tuple(
                t.__name__ for t in queryable_type.__mixin_supported_types__
            ),
        }


class _QueryProxy(_QueryNode):
    """
    A dynamic proxy object that is attached to Ontology classes (as .Q).
    It intercepts attribute access (like .position or .status) to
    build nested query paths and provide custom error messages.

    The proxy is created by the `queryable` decorator and is available as a
    class attribute named ``Q``.

    Example:
        ```python
        from mosaicolabs import IMU
        # This is a _QueryProxy instance
        imu_query_proxy = IMU.Q

        # This is another (nested) _QueryProxy instance
        imu_query_proxy.acceleration

        # This is a _QueryableField instance
        imu_query_proxy.acceleration.x

        # This is a _QueryExpression instance
        imu_query_proxy.acceleration.x.geq(1.234)
        ```
    """

    def __getattr__(self, name: str) -> Any:
        """
        Called at runtime when accessing an attribute (e.g., GPS.Q.position).

        Args:
            name (str): The name of the attribute being accessed (e.g., "position").

        Returns:
            The node of the child path: a proxy for a nested struct or a list, or a
                field for a simple field.

        Raises:
            AttributeError: If the 'name' is not a valid field in the schema,
                            providing a helpful error message.
        """
        if _is_dunder(name):
            raise AttributeError(name)
        if not isinstance(self.__schema__, dict) or name not in self.__schema__:
            # Attribute is invalid. Raise a helpful error.
            raise AttributeError(
                f"Invalid field '{name}' for path '{self.__path__}'. "
                f"Available fields: {self._queryable_fields}"
            )

        return self._child(f"{self.__path__}.{name}", self.__schema__[name])

    def _child(
        self,
        full_path: str,
        field_schema: Union[Dict[str, pa.DataType], pa.DataType],
    ) -> Any:
        """
        Returns the node of a child path, according to its schema.

        Args:
            full_path (str): The query path of the child (e.g., "GPS.position").
            field_schema (Union[Dict[str, pa.DataType], pa.DataType]): The schema of the child.
        """
        if isinstance(field_schema, _QueryableList):  # List of structs
            return _QueryStructListProxy(full_path, field_schema)
        if isinstance(field_schema, SUPPORTED_PYARROW_LIST_TYPES):  # List of base types
            return _QueryValueListProxy(full_path, field_schema)
        if isinstance(field_schema, dict):  # Nested struct
            return _QueryProxy(full_path, field_schema)
        # Simple field (e.g., IMU.Q.acceleration.x)
        return _make_field(full_path, field_schema)


class _FlattenedProxy(_QueryProxy):
    """
    The proxy of a struct reached through a list without an index specifier (e.g.
    `RobotPath.Q.poses.position`): each of its fields holds the values of all the
    elements of the list, so it can only be aggregated.
    """

    def _child(
        self,
        full_path: str,
        field_schema: Union[Dict[str, pa.DataType], pa.DataType],
    ) -> Any:
        if isinstance(field_schema, dict):  # Nested struct or list of structs
            return _FlattenedProxy(full_path, dict(field_schema))
        return _FlattenedField(full_path, _innermost_type(field_schema))


class _ListAggregatorsMixin:
    """
    The aggregators of the values of a list, taken as a whole: `.max()`, `.min()`
    and `.range()` are computed on the elements of all the lists of the topic,
    `.count()` and `.rate()` on the samples, whatever the length of their lists.
    """

    def _elements(self) -> _QueryableField:
        """
        Returns the field holding the elements of the list.
        """
        raise NotImplementedError

    def max(self) -> Any:
        """
        Aggregates the list to its greatest element over the topic.

        Returns:
            The queryable field of the elements, whose operators compare the
                aggregated value.

        Raises:
            TypeError: If the list is inside a list selected with an index specifier.
        """
        return self._elements().max()

    def min(self) -> Any:
        """
        Aggregates the list to its smallest element over the topic.

        Returns:
            The queryable field of the elements, whose operators compare the
                aggregated value.

        Raises:
            TypeError: If the list is inside a list selected with an index specifier.
        """
        return self._elements().min()

    def range(self) -> Any:
        """
        Aggregates the list to the range (`max - min`) of its elements over the topic.

        Returns:
            The queryable field of the elements, whose operators compare the
                aggregated value.

        Raises:
            TypeError: If the list is inside a list selected with an index specifier.
        """
        return self._elements().range()

    def count(self) -> Any:
        """
        Aggregates the list to its number of samples over the topic, whatever the
        length of their lists.

        Returns:
            A numeric queryable field, whose operators compare the aggregated value.

        Raises:
            TypeError: If the list is inside a list selected with an index specifier.
        """
        return self._elements().count()

    def rate(self) -> Any:
        """
        Aggregates the list to its number of samples per second (Hz) over the topic,
        whatever the length of their lists.

        Returns:
            A numeric queryable field, whose operators compare the aggregated value.

        Raises:
            TypeError: If the list is inside a list selected with an index specifier.
        """
        return self._elements().rate()


class _FlattenedField(_ListAggregatorsMixin):
    """
    A field reached through a list without an index specifier (e.g.
    `RobotPath.Q.poses.position.z`): it holds the values of all the elements of the
    list, so it can only be aggregated.

    Example:
        ```python
        # Every pose of the topic below 1 meter
        RobotPath.Q.poses.position.z.max().lt(1.0)
        ```
    """

    def __init__(self, full_path: str, value_type: pa.DataType):
        self.__path__ = full_path
        self.__value_type__ = value_type

    def _elements(self) -> _QueryableField:
        return _make_field(self.__path__, self.__value_type__)

    def __getattr__(self, name: str) -> Any:
        if _is_dunder(name):
            raise AttributeError(name)
        raise AttributeError(
            f"Field '{self.__path__}' is a list field: aggregate it with .max(), "
            f".min(), .range(), .count() or .rate(), or select the elements of the "
            f"list with .any(), .all() or [i]."
        )


class _QueryListProxy(_QueryNode):
    """
    Base class of the proxies of a list, whose elements are selected with
    `.any()`, `.all()` or `[i]`.
    """

    def _select(self, full_path: str) -> Union[_QueryProxy, _QueryableField]:
        """
        Returns the node of the selected elements of the list.

        Args:
            full_path (str): The query path with the list access expression (e.g., "RobotPath.poses[?]").
        """
        raise NotImplementedError

    def _add_list_expression(
        self, list_expression: str
    ) -> Union[_QueryProxy, _QueryableField]:
        """
        Appends a list access expression to the current path and returns the node of
        the selected elements.

        Supported expressions are:
        - ``"?"`` — any-element quantifier (matches if *any* element satisfies the condition).
        - ``"!"`` — all-element quantifier (matches if *all* elements satisfy the condition).
        - A digit string (e.g. ``"0"``) — index access for a specific element.

        Args:
            list_expression (str): The access expression to append.

        Raises:
            ValueError: If ``list_expression`` is not a supported operator and not a digit.
            TypeError: If the list is inside a list selected with an index specifier.
        """
        if "[" in self.__path__:
            raise TypeError(
                f"Field '{self.__path__}' is inside a list selected with .any(), "
                f".all() or [i]: its elements cannot be selected."
            )
        if (
            list_expression not in SUPPORTED_LIST_OPERATIONS
            and not list_expression.isdigit()
        ):
            raise ValueError(f"{list_expression} operation is not supported for lists")

        return self._select(f"{self.__path__}[{list_expression}]")

    def __getattr__(self, name: str) -> Any:
        if _is_dunder(name):
            raise AttributeError(name)
        raise AttributeError(
            f"Field '{self.__path__}' is a list. "
            f"Use .any(), .all(), or [i] to select elements before accessing sub-fields."
        )

    def __iter__(self):
        # Without it, Python would iterate by calling __getitem__ with 0, 1, 2, ...
        # which never raises IndexError, i.e. forever
        raise TypeError(
            f"Field '{self.__path__}' is a list and cannot be iterated. "
            f"Use .any(), .all(), or [i] to select elements."
        )

    def __getitem__(self, key) -> Union[_QueryProxy, _QueryableField]:
        """
        Accesses a specific element of the list field by integer index.

        Args:
            key (int): The zero-based index of the element to access.

        Returns:
            Union[_QueryProxy, _QueryableField]: Either a new `_QueryProxy` for a nested struct, or a
                `_QueryableField` for a simple field.

        Raises:
            TypeError: If ``key`` is not an integer, or the list is inside a list
                selected with an index specifier.
        """

        if not isinstance(key, int):
            raise TypeError(
                f"List index must be an integer, got '{type(key).__name__}'"
            )

        return self._add_list_expression(f"{key}")

    def any(self) -> Union[_QueryProxy, _QueryableField]:
        """
        Returns a proxy or field scoped to the *any-element* quantifier (``[?]``).

        Use this when the condition should match if **at least one** element in the list
        satisfies it.

        Example::

            RobotPath.Q.poses.any().position.x.gt(1.0)

        Returns:
            Union[_QueryProxy, _QueryableField]: Either a new `_QueryProxy` for a nested struct, or a
                `_QueryableField` for a simple field.

        Raises:
            TypeError: If the list is inside a list selected with an index specifier.
        """
        return self._add_list_expression("?")

    def all(self) -> Union[_QueryProxy, _QueryableField]:
        """
        Returns a proxy or field scoped to the *all-elements* quantifier (``[!]``).

        Use this when the condition should match only if **every** element in the list
        satisfies it.

        Example::

            RobotPath.Q.poses.all().position.x.gt(1.0)

        Returns:
            Union[_QueryProxy, _QueryableField]: Either a new `_QueryProxy` for a nested struct, or a
                `_QueryableField` for a simple field.

        Raises:
            TypeError: If the list is inside a list selected with an index specifier.
        """
        return self._add_list_expression("!")


class _QueryValueListProxy(_QueryListProxy, _ListAggregatorsMixin):
    """
    The proxy of a list of values (e.g. `RobotJoint.Q.positions`).

    Besides selecting the elements with `.any()`, `.all()` or `[i]`, the list can be
    aggregated as a whole with `.max()`, `.min()`, `.range()`, `.count()` and
    `.rate()`.

    Example:
        ```python
        # Every joint position of the topic below 3.14
        RobotJoint.Q.positions.max().lt(3.14)
        ```
    """

    def _select(self, full_path: str) -> _QueryableField:
        return _make_field(full_path, self.__schema__.value_type)

    def _elements(self) -> _QueryableField:
        return _make_field(self.__path__, _innermost_type(self.__schema__))


class _QueryStructListProxy(_QueryListProxy):
    """
    The proxy of a list of structs (e.g. `RobotPath.Q.poses`).

    Besides selecting the elements with `.any()`, `.all()` or `[i]`, the fields of
    the structs can be accessed directly to aggregate them over all the elements
    of the list (e.g. `RobotPath.Q.poses.position.z.max()`).
    """

    def _select(self, full_path: str) -> _QueryProxy:
        # Downcast __schema__ to avoid confusing it as a _QueryableList
        return _QueryProxy(full_path, dict(self.__schema__))

    def __getattr__(self, name: str) -> Any:
        if _is_dunder(name) or name not in self.__schema__:
            return super().__getattr__(name)
        return getattr(_FlattenedProxy(self.__path__, dict(self.__schema__)), name)


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
        # Build the nested field schema using the provided mapper
        query_prefix, field_schema = mapper.build_schema(
            class_type,
            path_prefix=query_prefix,
        )

        # Create the root QueryProxy instance
        root_proxy = _QueryProxy(
            full_path=query_prefix,
            field_schema=field_schema,
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
