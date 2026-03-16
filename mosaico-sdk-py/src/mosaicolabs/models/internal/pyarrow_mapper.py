import datetime
from typing import Any, Dict, Optional, Tuple, Type

import pyarrow as pa

# --- Import the query builder components ---
from ..query.expressions import _QueryExpression
from ..query.generation.internal import _PYTHON_TYPE_TO_QUERYABLE
from ..query.generation.mixins import (
    _make_queryable_field_type,
    _QueryableUnsupported,
)

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


def _pyarrow_to_queryable(ptype: pa.DataType):
    """
    Returns the _Queryable* mixin type, given a pyarrow type instance.
    e.g. pa.string() -> _QueryableString
    """
    return _PYTHON_TYPE_TO_QUERYABLE.get(
        _PYARROW_TO_PYTHON_TYPE.get(ptype, None),  # return none if not found
        _QueryableUnsupported,  # further safety get
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
        from mosaicolabs.models.message import Message

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
        # create a member variable to hold the query expression type
        self._query_expression_type = query_expression_type
        # start fields mapping
        return path_prefix, self._build_map_recursive(
            combined_struct,
            path_prefix,
        )

    def _build_map_recursive(
        self, struct_type: pa.StructType, path_prefix: str
    ) -> Dict[str, Any]:
        field_map = {}

        for field in struct_type:
            # Construct the full path for this field (e.g. "telemetry.speed")
            full_path = f"{path_prefix}.{field.name}"

            if isinstance(field.type, pa.StructType):
                # If the field is a nested struct, recurse into it
                field_map[field.name] = self._build_map_recursive(field.type, full_path)

            elif not isinstance(field.type, (pa.ListType, pa.LargeListType)):
                # If it's a base field (not a list or nested struct):
                # - find the appropriate mixin based on data type
                # - dynamically create a subclass combining the mixin + queryable field
                mixin = _pyarrow_to_queryable(field.type)

                # TODO: Better implement the optional logic being incomplete

                # # Dynamically create a composite class for this field.
                # # If the field is optional, then a further base class is added
                # # providing 'existence' (ex/nex) operators
                # if mixin is not _QueryableUnsupported and field.nullable is True:
                #     cls = type(
                #         f"{mixin.__name__}Field",
                #         (mixin, _QueryableOptionalBase, _QueryableField),
                #         {},
                #     )
                # else:
                #     cls = type(f"{mixin.__name__}Field", (mixin, _QueryableField), {})
                cls = _make_queryable_field_type(mixin)

                # Instantiate the dynamically created class with its path
                field_map[field.name] = cls(
                    full_path=full_path, expr_cls=self._query_expression_type
                )

            # If it's a list type, skip it for now (no query support yet)
            # Lists can be added later with special handling if needed.

        return field_map
