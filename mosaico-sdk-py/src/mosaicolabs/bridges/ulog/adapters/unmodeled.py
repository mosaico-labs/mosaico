"""
The default ULog adapter: every topic without a hand-written adapter is stored as an
`Unmodeled` ontology.
"""

from typing import Any, Dict, Type, TypeVar

import pyarrow as pa

from mosaicolabs.models.core.unmodeled import Unmodeled

from ..adapter_base import ULogAdapterBase

UnmodeledT = TypeVar("UnmodeledT", bound=Unmodeled)

# Process-wide: topics with the same ontology class share one adapter class, so the key
# must tell schema variants apart (see `get_or_create`)
_UNMODELED_ADAPTERS_REGISTRY: Dict[str, Type["UnmodeledAdapter"]] = {}


class UnmodeledAdapter(ULogAdapterBase[UnmodeledT]):
    """
    Adapter storing a ULog topic as an `Unmodeled` ontology with its original schema.

    The ontology schema is the raw ULog one (`ULogSchemaConverter.to_pyarrow`), so the
    translation is the identity. One adapter class is created per ontology class via
    `get_or_create`, keyed by its `__registry_key__`, which already tells apart schema
    variants of the same format name.
    """

    @classmethod
    def translate_batch(cls, batch: pa.RecordBatch, **kwargs: Any) -> pa.RecordBatch:
        # Nothing to translate: the ontology *is* the raw ULog schema
        return batch

    @classmethod
    def get_or_create(cls, ontology_type: Type[UnmodeledT]) -> Type["UnmodeledAdapter"]:
        """Returns the adapter for `ontology_type`, creating and registering it on first use."""
        # "<format>", or "<format>__<schema fingerprint>" for a schema variant
        key = ontology_type.__registry_key__
        adapter = _UNMODELED_ADAPTERS_REGISTRY.get(key)
        if adapter is None:
            adapter = type(
                f"{ontology_type.__name__}Adapter",
                (cls,),
                {"__mosaico_ontology_type__": ontology_type},
            )
            _UNMODELED_ADAPTERS_REGISTRY[key] = adapter
        return adapter
