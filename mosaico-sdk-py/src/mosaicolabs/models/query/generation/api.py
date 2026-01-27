from typing import (
    ClassVar,
    Dict,
    Optional,
    Type,
    Any,
    TypeVar,
)
import inspect

from ..expressions import _QueryExpression
from ..protocols import FieldMapperProtocol
from .mixins import _QueryableUnsupported


class _QueryProxy:
    """
    A dynamic proxy object that is attached to Ontology classes (as .Q).
    It intercepts attribute access (like .position or .status) to
    build nested query paths and provide custom error messages.
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

    def __getattr__(self, name: str) -> Any:
        """
        Called at runtime when accessing an attribute (e.g., GPS.Q.position).

        Args:
            name (str): The name of the attribute being accessed (e.g., "position").

        Returns:
            Any: Either a new QueryProxy for a nested struct, or a
                 _QueryableField for a simple field.

        Raises:
            AttributeError: If the 'name' is not a valid field in the map,
                            providing a helpful error message.
        """
        if name not in self.__map__:
            # Attribute is invalid. Raise a helpful error.
            raise AttributeError(
                f"Invalid field '{name}' for path '{self.__path__}'. "
                f"Available fields: {self.queryable_fields}"
            )

        # Retrieve the child object from the map
        child = self.__map__[name]

        if isinstance(child, dict):
            # This is a nested struct (e.g., 'position').
            # Return a *new* QueryProxy instance for this deeper path.
            return _QueryProxy(
                full_path=f"{self.__path__}.{name}",  # e.g., "gps.position"
                field_map=child,  # The nested field map
            )
        else:
            # This is a simple field (a _QueryableField instance).
            # Return it directly.
            # (e.g., accessing .status returns _QueryableField("gps.status"))
            return child

    @property
    def queryable_fields(self):
        return list(
            key
            for key, val in self.__map__.items()
            if not isinstance(val, _QueryableUnsupported)
        )


# --- The General QueryableModel ---
class _QueryableModel:
    """
    A mixin class that provides query proxy generation capabilities
    to any class that defines a PyArrow '__msco_pyarrow_struct__' and provides a
    root query prefix (like a '__ontology_tag__').
    """

    # Class variable, because it is expected to use like: 'IMU.Q.acceleration.x >= 1.234'
    Q: ClassVar[_QueryProxy]

    @staticmethod
    def _inject_query_proxy(
        class_type: Type,
        mapper: FieldMapperProtocol,
        query_expression_type: Type[_QueryExpression],
        query_prefix: Optional[str] = None,
    ):
        """
        Static helper to build and inject the .Q query proxy.
        This is called by the default case or by custom subclasses.
        """
        # Build the nested field map using the provided mapper
        query_prefix, field_map = mapper.build_map(
            class_type,
            query_expression_type=query_expression_type,
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
    query_expression_type: Type[_QueryExpression],
    prefix: Optional[str] = None,
    **kwargs,
):
    """
    Class decorator to build and inject the .Q proxy.
    """

    def decorator(cls: Type[T]) -> Type[T]:
        # Determine the query prefix
        # Call the injection helper
        _QueryableModel._inject_query_proxy(
            cls, mapper_type(**kwargs), query_expression_type, prefix
        )
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
    return issubclass(model, _QueryableModel)
