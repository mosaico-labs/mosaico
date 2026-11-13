"""
Adapter contract and reserved metadata of the ULog bridge.
"""

from abc import abstractmethod
from typing import Any, ClassVar, Dict, Type, TypeVar

import pyarrow as pa

from mosaicolabs.models.core import Serializable

from ..base_schema_metadata import BaseSchemaMetadata
from ..bridge_adapter_base import BridgeAdapterBase


class ULogSchemaMetadata(BaseSchemaMetadata):
    """
    Mosaico's reserved ``_ulog_`` topic-metadata namespace.

    It keeps what is needed to write the topic back as ULog: the format name, its
    instance, the original `F` definition (padding included) and the definitions of the
    nested formats. Bridge-internal fields (e.g. `source_file`, dropped-row counts) are
    added via `update`.
    """

    KEY: ClassVar[str] = "_ulog_"

    REQUIRED_KEYS: ClassVar[Dict[str, Type]] = {
        "message_name": str,  # ULog format name, e.g. "sensor_accel"
        "multi_id": int,  # instance of the format (0, 1, ...)
        "format_def": str,  # original `F` definition, padding included
        "nested_formats": dict,  # {format name: definition} of the nested formats
    }


class ULogSequenceMetadata(BaseSchemaMetadata):
    """
    Mosaico's reserved ``_ulog_`` sequence-metadata namespace.

    It holds the file-level content of the `.ulg` file (see
    `ULogLoader.sequence_metadata`): the info and info-multiple records with their types,
    the initial and default parameters, the ingestion stats and, when the log carries a
    GPS time, the offset between its boot-relative timestamps and UTC.
    """

    KEY: ClassVar[str] = "_ulog_"


OntologyT = TypeVar("OntologyT", bound=Serializable)


class ULogAdapterBase(BridgeAdapterBase[OntologyT, dict]):
    """
    Abstract base class for converting ULog topics to Mosaico ontology types.

    ULog is columnar, so an adapter translates many rows at once: `translate_batch`
    receives a `RecordBatch` with the `timestamp_ns` column plus the raw ULog schema of
    the topic, and returns a `RecordBatch` with the Mosaico topic schema of
    `__mosaico_ontology_type__`.
    """

    __mosaico_ontology_type__: Type[OntologyT]

    @classmethod
    @abstractmethod
    def translate_batch(cls, batch: pa.RecordBatch, **kwargs: Any) -> pa.RecordBatch:
        """Translates a batch of rows of the topic into the ontology schema."""

    # Required by BridgeAdapterBase: writing `.ulg` files is not supported
    @classmethod
    def to_native(cls, mosaico_data, **kwargs) -> dict:
        raise NotImplementedError(
            f"{cls.__name__}: Mosaico -> ULog is not supported yet."
        )

    @classmethod
    def ontology_data_type(cls) -> Type[OntologyT]:
        """Returns the Ontology class type associated with this adapter."""
        return cls.__mosaico_ontology_type__
