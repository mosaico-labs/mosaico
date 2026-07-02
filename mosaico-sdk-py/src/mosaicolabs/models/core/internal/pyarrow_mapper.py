from typing import Any, Dict, Optional, Tuple

import pyarrow as pa

# --- Import the query builder components ---
from mosaicolabs.models.query.generation.internal import (
    _QueryableList,
)


class PyarrowFieldMapper:
    """
    A custom FieldMapper that builds the map by inspecting
    PyArrow `__msco_pyarrow_struct__` attributes. The map is the
    unrolled version of the ontology PyArrow schema mapping
    the ontology field names to their respective `pyarrow.DataType()`
    """

    def build_map(
        self,
        class_type: type,
        path_prefix: Optional[str] = None,
    ) -> Tuple[str, Dict[str, Any]]:
        """
        Builds the pyarrow.Datatype map for a given Ontology Model, via pyarrow
        struct inspection.

        This method iterates over all model fields, recursively building a map for
        nested Pydantic models and associating the field name with its `pyarrow.DataType()`.

        As an example, passing the
          `Pose` ontology it would result in:

          {
           'timestamp_ns': DataType(int64),
           'header':
                {
                 'timestamp': {...},
                 'frame_id': DataType(string),
                 'sample_counter': DataType(uint64)
                },
            'position':
                {
                 'header': {...},
                 'covariance': ListType(list<item: double>),
                 'covariance_type': DataType(int16),
                 'x': DataType(double),
                 'y': DataType(double),
                 'z': DataType(double)
                },
            'orientation':
            {
                'header': {...},
                'covariance': ListType(list<item: double>),
                'covariance_type': DataType(int16),
                'x': DataType(double),
                'y': DataType(double),
                'z': DataType(double),
                'w': DataType(double)
            }
          }
        """
        from ..message import Message

        cls_pa_fields = []
        if class_type.__msco_pyarrow_struct__ is not Message.__msco_pyarrow_struct__:
            # Convert the PyArrow struct to a standard list of pa.Field objects
            cls_pa_fields = list(class_type.__msco_pyarrow_struct__)
        combined_struct = pa.struct(
            # Add always Message fields to add then in the resulting pyarrow.Datatype map
            list(Message.__msco_pyarrow_struct__) + cls_pa_fields
        )
        # Make sure we have a valid path prefix
        path_prefix = path_prefix or class_type.__ontology_tag__ or class_type.__name__
        # start fields mapping
        return path_prefix, self._build_map_recursive(
            combined_struct,
        )

    def _build_map_recursive(self, struct_type: pa.StructType) -> Dict[str, Any]:
        """
        Recursivelly unrolls the passed Pyarrow Struct resulting creating the nested map
        where the keys are the name of the considered pyarrow field
        and the values may are:

          - a `dict` if it is a pa.StructType
          - a `dict` masked as a `_QueryableList` if it is a pa.ListType
          - a `pyarrow.DataType()` otherwise.

        """
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
