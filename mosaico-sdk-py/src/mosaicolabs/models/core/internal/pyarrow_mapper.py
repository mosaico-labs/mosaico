from typing import Any, Dict, Optional, Tuple, Type

import pyarrow as pa

# --- Import the query builder components ---
from mosaicolabs.models.query.expressions import _QueryExpression
from mosaicolabs.models.query.generation.internal import (
    _QueryableList,
)


class PyarrowFieldMapper:
    """
    A custom FieldMapper that builds the map by inspecting
    PyArrow `__msco_pyarrow_struct__` attributes.
    """

    def build_map(
        self,
        class_type: type,
        query_expression_type: Type[_QueryExpression],
        path_prefix: Optional[str] = None,
    ) -> Tuple[str, Dict[str, Any]]:
        """
        Builds the queryable field map for a given Ontology Model, via pyarrow
        struct inspection.

        This method identifies the root path (if not provided) and then
        iterates over all model fields, recursively building a map for
        nested Pydantic models and creating queryable field objects
        for simple types.
        """
        from ..message import Message

        cls_pa_fields = []
        if class_type.__msco_pyarrow_struct__ is not Message.__msco_pyarrow_struct__:
            # Convert the PyArrow struct to a standard list of pa.Field objects
            cls_pa_fields = list(class_type.__msco_pyarrow_struct__)
        combined_struct = pa.struct(
            # Add always Message fields to queryable fields of Data Catalog types
            list(Message.__msco_pyarrow_struct__) + cls_pa_fields
        )
        # Make sure we have a valid path prefix
        path_prefix = path_prefix or class_type.__ontology_tag__ or class_type.__name__
        # start fields mapping
        return path_prefix, self._build_map_recursive(
            combined_struct,
        )

    def _build_map_recursive(self, struct_type: pa.StructType) -> Dict[str, Any]:
        field_map = {}

        for field in struct_type:
            # Construct the full path for this field (e.g. "telemetry.speed")

            if isinstance(field.type, pa.StructType):
                # If the field is a nested struct, recurse into it
                field_map[field.name] = self._build_map_recursive(field.type)

            elif isinstance(field.type, (pa.ListType, pa.LargeListType)):
                list_value_type = field.type.value_type

                # List type is another struct
                if isinstance(list_value_type, pa.StructType):
                    field_map[field.name] = _QueryableList(
                        self._build_map_recursive(list_value_type)
                    )
                else:
                    # Set the PyArrow type as dict value
                    field_map[field.name] = field.type

            else:
                # If it's a base field (not a list or nested struct):
                # Set the PyArrow type as dict value
                field_map[field.name] = field.type

            # If it's a list type, skip it for now (no query support yet)
            # Lists can be added later with special handling if needed.

        return field_map
