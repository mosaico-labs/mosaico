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

    def __init__(
        self, full_path: str, field_schema: Union[Dict[str, pa.DataType], pa.DataType]
    ):
        """
        Initializes the dynamic proxy.

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
            _QueryableField: A dynamically-created ``_QueryableField`` subclass instance, ready to be
                used in query expressions.
        """

        mixin = _pyarrow_to_queryable_mixin(field_type)

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
            Union[_QueryProxy, _QueryableField]: Either a new `_QueryProxy` for a nested struct, or a
                `_QueryableField` for a simple field.

        Raises:
            ValueError: If ``list_expression`` is not a supported operator and not a digit.
            TypeError: If the current field is not a list type (``pa.ListType`` or ``_QueryableList``).
        """

        # Check that current schema is a List (either _QueryableList, pa.ListType, pa.LargeListType, pa.FixedSizeListType)
        if not isinstance(
            self.__schema__,
            (_QueryableList, *SUPPORTED_PYARROW_LIST_TYPES),
        ):
            raise TypeError(
                f"Field '{self.__path__}' is not a list. Cannot be indexed."
            )

        # Check that list expression is supported
        if (
            list_expression not in SUPPORTED_LIST_OPERATIONS
            and not list_expression.isdigit()
        ):
            raise ValueError(f"{list_expression} operation is not supported for lists")

        # Append list operation among allowed ("?", "!", [i]) to overall path
        path_w_list_expr = f"{self.__path__}[{list_expression}]"

        # Here two cases can happen:
        #   1) List contains a complex struct -> return a QueryProxy downcasting the _QueryableList to dict and allow dot notiation to work
        #   2) List contains a basic type -> create queryable field
        if isinstance(self.__schema__, _QueryableList):  # List of structs
            return _QueryProxy(
                full_path=path_w_list_expr,
                field_schema=dict(
                    self.__schema__
                ),  # Downcast __schema__ to avoid confusing it as a _QueryableList
            )
        elif isinstance(
            self.__schema__, SUPPORTED_PYARROW_LIST_TYPES
        ):  # List of base types
            field_type = self.__schema__.value_type
            queriable_field = self._create_queriable_field(path_w_list_expr, field_type)

            return queriable_field

        else:  # this should not be possible
            raise TypeError(
                f"{self.__schema__} is not a {pa.ListType.__name__} or {_QueryableList.__name__}"
            )

    def __getattr__(self, name: str) -> Any:
        """
        Called at runtime when accessing an attribute (e.g., GPS.Q.position).

        Args:
            name (str): The name of the attribute being accessed (e.g., "position").

        Returns:
            Union[_QueryProxy, _QueryableField]: Either a new `_QueryProxy` for a nested struct, or a
                `_QueryableField` for a simple field.

        Raises:
            AttributeError: If the 'name' is not a valid field in the schema,
                            providing a helpful error message.
        """

        if isinstance(
            self.__schema__,
            (_QueryableList, *SUPPORTED_PYARROW_LIST_TYPES),
        ):
            raise AttributeError(
                f"Field '{self.__path__}' is a list. "
                f"Use .any(), .all(), or [i] to select elements before accessing sub-fields."
            )

        if not isinstance(self.__schema__, dict) or name not in self.__schema__:
            # Attribute is invalid. Raise a helpful error.
            raise AttributeError(
                f"Invalid field '{name}' for path '{self.__path__}'. "
                f"Available fields: {self._queryable_fields}"
            )

        # Retrieve the child object from the schema
        child = self.__schema__[name]

        if isinstance(child, (dict, *SUPPORTED_PYARROW_LIST_TYPES)):
            # This is a nested struct (e.g., 'position').
            # Return a *new* QueryProxy instance for this deeper path.
            # NOTE: instances of 'dict' include _QueryableList also
            return _QueryProxy(
                full_path=f"{self.__path__}.{name}",  # e.g., "GPS.position"
                field_schema=child,  # The nested field schema
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
            Union[_QueryProxy, _QueryableField]: Either a new `_QueryProxy` for a nested struct, or a
                `_QueryableField` for a simple field.

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
            Union[_QueryProxy, _QueryableField]: Either a new `_QueryProxy` for a nested struct, or a
                `_QueryableField` for a simple field.

        Raises:
            TypeError: If the current field is not a list.
        """
        return self._add_list_expression("?")

    def all(self) -> Union["_QueryProxy", _QueryableField]:
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
            TypeError: If the current field is not a list.
        """

        return self._add_list_expression("!")

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
