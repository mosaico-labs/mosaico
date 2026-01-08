from typing import (
    Optional,
    Type,
    Any,
    Dict,
    Tuple,
    Union,
    get_args,
    get_origin,
)
import inspect

import pydantic
from .mixins import (
    _QueryableUnsupported,
    _QueryableField,
)

from .internal import _PYTHON_TYPE_TO_QUERYABLE

from ..expressions import _QueryExpression


def _is_optional(field_type):
    origin = get_origin(field_type)
    return origin is Union and None in getattr(field_type, "__args__", ())


# --- Default Pydantic Implementation ---
class PydanticFieldMapper:
    """
    The default FieldMapper, which builds a map by inspecting
    Pydantic BaseModel fields.
    """

    def _get_base_type(self, field_type: Optional[Type]) -> Type | None:
        """
        Recursively unwraps type hints like Optional[T] to get the base type T.

        - Optional[int]     -> int
        - str               -> str
        - dict              -> dict
        - list, tuple       -> None (as lists are unsupported)
        - Union[int, str]   -> None (as complex Unions are unsupported)
        """
        if field_type is None:
            return None

        origin = get_origin(field_type)

        # Handle Optional[T] (which is Union[T, NoneType])
        if origin is Union:
            args = get_args(field_type)
            # Check if it's exactly Union[T, NoneType]
            if len(args) == 2 and args[1] is type(None):
                # It's Optional[T]. Recurse on T.
                return self._get_base_type(args[0])
            else:
                # It's a complex Union (e.g., Union[int, str]), unsupported.
                return None

        # Handle other generics (list, dict, Type[T], etc.)
        if origin is not None:
            # Explicitly unsupported generics
            if origin in (list, tuple):
                return None

            # Explicitly supported generics
            if origin is dict:
                return dict

            # Other generics (like Type[T]) are unsupported
            return None

        # Base case: Not a generic, it's a simple type.
        if inspect.isclass(field_type):
            # This is a simple type like int, str, or a BaseModel
            return field_type

        # Default to unsupported
        return None

    def build_map(
        self,
        class_type: Type,
        query_expression_type: Type[_QueryExpression],
        path_prefix: Optional[str] = None,
    ) -> Tuple[str, Dict[str, Any]]:
        """
        Builds the queryable field map for a given Pydantic class.

        This method identifies the root path (if not provided) and then
        iterates over all model fields, recursively building a map for
        nested Pydantic models and creating queryable field objects
        for simple types.
        """
        field_map = {}
        # Guard clause: This mapper only works on Pydantic models.
        if not issubclass(class_type, pydantic.BaseModel):
            raise ValueError(
                f"PydanticFieldMapper can only process pydantic.BaseModel subclasses. Got '{class_type}'."
            )

        # Establish the root path for this class
        # If path_prefix is None (i.e., this is the top-level call),
        # use the class's name as the default.
        path_prefix = (
            path_prefix if path_prefix is not None else class_type.__name__.lower()
        )

        # Iterate over all fields defined in the Pydantic model
        for field_name, field_info in class_type.model_fields.items():
            # Construct the full dot-notation path (e.g., "type.field")
            full_path = f"{path_prefix}.{field_name}" if path_prefix else field_name

            # Get the raw type annotation (e.g., str, Optional[int], MyNestedModel)
            field_type = field_info.annotation

            # Unwrap the type hint to get the base type (e.g., int from Optional[int])
            # For unsupported types (list, dict), base_type will be None.
            base_type = self._get_base_type(field_type)

            # Handle nested Pydantic models (recursion)
            if (
                base_type
                and inspect.isclass(base_type)
                and issubclass(base_type, pydantic.BaseModel)
            ):
                # If the field is another Pydantic model, recurse.
                # The returned path is the prefix (e.g., "Topic.sub_model"),
                # and the sub_map is its field map.
                _, sub_map = self.build_map(base_type, query_expression_type, full_path)
                field_map[field_name] = sub_map

            # Handle types
            else:
                # We have a simple, unwrapped type (int, str, bool).
                # Look up the corresponding query mixin (e.g., _QueryableNumeric)
                # If not found, default to _QueryableUnsupported.
                mixin = _PYTHON_TYPE_TO_QUERYABLE.get(base_type, _QueryableUnsupported)

                # TODO: Better implement the optional logic being incomplete

                # # Dynamically create a queryable class for this field
                # # If the field is optional, then a further base class is added
                # # providing 'existence' (ex/nex) operators
                # if mixin is not _QueryableUnsupported and _is_optional(field_type):
                #     q_cls = type(
                #         f"{mixin.__name__}Field",
                #         (mixin, _QueryableOptionalBase, _QueryableField),
                #         {},
                #     )
                # else:
                #     q_cls = type(f"{mixin.__name__}Field", (mixin, _QueryableField), {})
                q_cls = type(f"{mixin.__name__}Field", (mixin, _QueryableField), {})

                # Instantiate it with its full query path
                field_map[field_name] = q_cls(
                    full_path=full_path,
                    expr_cls=query_expression_type,  # <-- Use the arg
                )

            # # 3. Handle Unsupported Types (lists, dicts, complex unions, etc.)
            # else:
            #     # base_type is None, meaning _get_base_type found an
            #     # unsupported type.
            #     mixin = _QueryableUnsupported
            #     q_cls = type(f"{mixin.__name__}Field", (mixin, _QueryableField), {})
            #     field_map[field_name] = q_cls(
            #         full_path=full_path, expr_cls=self._query_expression_type
            #     )

        # Return the established path and the completed map for this level
        return path_prefix, field_map
