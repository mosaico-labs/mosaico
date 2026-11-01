"""
Adapter contract and reserved topic metadata of the ULog bridge.

Mirrors `bridges/mcap/adapter_base.py`:
    - `ULogSchemaMetadata` <-> `MCAPSchemaMetadata` (reserved `_ulog_` / `_mcap_` key);
    - `ULogAdapterBase`    <-> `MCAPAdapterBase` (both on top of `BridgeAdapterBase`).
"""

from abc import abstractmethod
from typing import Any, ClassVar, Dict, Type, TypeVar

import pyarrow as pa

from mosaicolabs.models.core import Serializable

from ..base_schema_metadata import BaseSchemaMetadata
from ..bridge_adapter_base import BridgeAdapterBase
from .ulog_message import ULogMessage


class ULogSchemaMetadata(BaseSchemaMetadata):
    """
    Mosaico's reserved ``_ulog_`` topic-metadata namespace.

    It keeps what is needed to write the topic back as ULog: the format name, its
    instance, the original `F` definition (padding included) and the definitions of the
    nested formats. Bridge-internal fields (e.g. `source_file`, dropped-row counts) are
    added via `update`.
    """

    # Same pattern as MCAPSchemaMetadata: the key lives in one place, and the
    # required keys are type-checked by BaseSchemaMetadata at creation time
    KEY: ClassVar[str] = "_ulog_"

    REQUIRED_KEYS: ClassVar[Dict[str, Type]] = {
        "message_name": str,  # ULog format name, e.g. "sensor_accel"
        "multi_id": int,  # instance of the format (0, 1, ...)
        "format_def": str,  # original `F` definition, padding included
        "nested_formats": dict,  # {format name: definition} of the nested formats
    }


OntologyT = TypeVar("OntologyT", bound=Serializable)


class ULogAdapterBase(BridgeAdapterBase[OntologyT, dict]):
    """
    Abstract base class for converting ULog topics to Mosaico ontology types.

    ULog is columnar, so an adapter translates a whole topic at once: `translate`
    receives a `ULogMessage` whose `RecordBatch` has the raw ULog schema and returns a
    `RecordBatch` with the Mosaico topic schema of `__mosaico_ontology_type__`.
    """

    __mosaico_ontology_type__: Type[OntologyT]

    # Same role as MCAPAdapterBase.translate, but batch-in / batch-out instead of
    # one MCAPMessage -> one Message
    @classmethod
    @abstractmethod
    def translate(cls, msg: ULogMessage, **kwargs: Any) -> pa.RecordBatch:
        """Translates the topic rows in `msg.data` into the ontology schema."""

    # Required by BridgeAdapterBase; it will be implemented with the extraction
    # (Mosaico -> .ulg), which is out of scope for now
    @classmethod
    def to_native(cls, mosaico_data, **kwargs) -> dict:
        raise NotImplementedError(
            f"{cls.__name__}: Mosaico -> ULog is not supported yet."
        )

    # Same as MCAPAdapterBase.ontology_data_type: the class passed to topic_create
    @classmethod
    def ontology_data_type(cls) -> Type[OntologyT]:
        """Returns the Ontology class type associated with this adapter."""
        return cls.__mosaico_ontology_type__
