from typing import Any, Dict, Optional, Type

import pyarrow as pa

from mosaicolabs.enum.serialization_format import SerializationFormat

from .serializable import Serializable


class Unmodeled(
    Serializable,
    skip_schema_generation=True,
    skip_query_proxy_generation=True,
):
    raw_data: Dict[str, Any]

    def _encode(self):
        return self.raw_data

    @classmethod
    def _decode(cls, *_, **kwargs):
        return cls(raw_data=kwargs)


def make_unmodeled_ontology_class(
    class_name: str,
    ontology_tag: Optional[str],
    serializazion_format: SerializationFormat,
    pyarrow_schema: pa.StructType,
) -> Type[Unmodeled]:

    return type(
        class_name,
        (Unmodeled,),
        {
            "__ontology_tag__": ontology_tag,
            "__serialization_format__": serializazion_format,
            "__msco_pyarrow_struct__": pyarrow_schema,
        },
    )
