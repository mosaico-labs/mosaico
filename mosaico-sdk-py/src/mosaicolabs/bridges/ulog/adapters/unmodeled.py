"""
The default ULog adapter: every topic without a hand-written adapter is stored as an
`Unmodeled` ontology. Same factory pattern as `bridges/mcap/adapters/unmodeled.py`.
"""

from typing import Any, Dict, Type, TypeVar

import pyarrow as pa

from mosaicolabs.models.core.unmodeled import Unmodeled

from ..adapter_base import ULogAdapterBase
from ..ulog_message import ULogMessage

UnmodeledT = TypeVar("UnmodeledT", bound=Unmodeled)

# Module-level registry, as in MCAP: topics with the same ontology class share one
# adapter class. Process-wide, so the key must tell schema variants apart (see below).
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
    def translate(cls, msg: ULogMessage, **kwargs: Any) -> pa.RecordBatch:
        # Nothing to translate: the loader already built the batch with the exact
        # schema of the Unmodeled class (the ontology *is* the raw ULog schema)
        if msg.data is None:
            raise ValueError(f"No data for topic '{msg.topic_name}'.")
        return msg.data

    @classmethod
    def get_or_create(cls, ontology_type: Type[UnmodeledT]) -> Type["UnmodeledAdapter"]:
        """Returns the adapter for `ontology_type`, creating and registering it on first use."""
        # Same as mcap UnmodeledAdapter.get_or_create, with a different key: MCAP uses
        # schema name + encoding; here the ontology's registry key, which is
        # "<format>" or "<format>__<schema fingerprint>" for a schema variant
        # (as the ROS bridge does)
        key = ontology_type.__registry_key__
        adapter = _UNMODELED_ADAPTERS_REGISTRY.get(key)
        if adapter is None:
            # A new subclass bound to this ontology class, created with type() as in MCAP
            adapter = type(
                f"{ontology_type.__name__}Adapter",
                (cls,),
                {"__mosaico_ontology_type__": ontology_type},
            )
            _UNMODELED_ADAPTERS_REGISTRY[key] = adapter
        return adapter
