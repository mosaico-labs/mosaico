from typing import Optional

import pyarrow as pa

from mosaicolabs.enum.serialization_format import SerializationFormat

from .serializable import Serializable
from .unmodeled import make_unmodeled_ontology_class


def get_or_make_ontology_class(
    ontology_tag: str,
    class_name: Optional[str] = None,
    schema: Optional[pa.StructType] = None,
    serialization_format: Optional[SerializationFormat] = None,
):
    DataClass = Serializable._get_class_type(ontology_tag)
    # Check if this ontology tag is wrapped by an ontology model class.
    # If not, treat as unmodeled class and wrap around a dynamic created class
    if DataClass is None:
        if schema is None:
            raise ValueError(
                f"No ontology registered with tag '{ontology_tag}'. "
                f"Available tags: {Serializable._list_registered()}. "
                "Try passing a pyarrow schema for inferring a fallback ontology type."
            )
        # NOTE: This is done only once, per each schema. Once the new dynamic class is created,
        # it is added in the Serializable factory and the next time `Serializable._get_class_type(ontology_tag) != None`
        DataClass = make_unmodeled_ontology_class(
            class_name=class_name or ontology_tag,
            ontology_tag=ontology_tag,
            serializazion_format=serialization_format or SerializationFormat.Default,
            pyarrow_schema=schema,
        )

    return DataClass
