from typing import Any, Dict, List, Optional, Type

import pyarrow as pa

from mosaicolabs.enum.serialization_format import SerializationFormat

from .internal.helpers import _fix_empty_dicts
from .serializable import Serializable


def _diff_against_struct(
    data: Dict[str, Any], struct: pa.StructType, path: str = ""
) -> List[str]:
    """
    Recursively compares a raw payload dict against a pyarrow StructType,
    returning a list of human-readable mismatch descriptions (empty if none).
    """
    errors: List[str] = []
    schema_fields = {field.name: field for field in struct}
    provided_keys = set(data.keys())
    schema_keys = set(schema_fields.keys())

    for name in sorted(schema_keys - provided_keys):
        if not schema_fields[name].nullable:
            errors.append(f"missing required field '{path}{name}'")

    for name in sorted(provided_keys - schema_keys):
        errors.append(f"unknown field '{path}{name}' (not present in schema)")

    for name in sorted(provided_keys & schema_keys):
        field = schema_fields[name]
        value = data[name]
        if value is None or not isinstance(field.type, pa.StructType):
            continue
        if not isinstance(value, dict):
            errors.append(
                f"field '{path}{name}' expected a nested object matching "
                f"'{field.type}', got {type(value).__name__}"
            )
        else:
            errors.extend(_diff_against_struct(value, field.type, f"{path}{name}."))

    return errors


class Unmodeled(
    Serializable,
    skip_schema_generation=True,
    skip_query_proxy_ingestion=True,
):
    raw_data: Dict[str, Any]

    def model_post_init(self, context: Any) -> None:
        super().model_post_init(context)
        errors = _diff_against_struct(self.raw_data, self.__msco_pyarrow_struct__)
        if errors:
            raise ValueError(
                f"'{type(self).__name__}' raw_data does not match its declared "
                f"pyarrow schema:\n" + "\n".join(f"  - {e}" for e in errors)
            )

    def _encode(self):
        return self.raw_data

    @classmethod
    def _decode(cls, *_, **kwargs):
        fixed_kwargs = _fix_empty_dicts(kwargs) if kwargs else {}
        return cls(raw_data=fixed_kwargs)


def make_unmodeled_ontology_class(
    class_name: str,
    ontology_tag: Optional[str],
    serialization_format: SerializationFormat,
    pyarrow_schema: pa.StructType,
) -> Type[Unmodeled]:

    return type(
        class_name,
        (Unmodeled,),
        {
            "__ontology_tag__": ontology_tag,
            "__serialization_format__": serialization_format,
            "__msco_pyarrow_struct__": pyarrow_schema,
        },
        skip_schema_generation=True,  # keep skip_query_proxy_ingestion=False (default)
    )
