from typing import Dict, Generic, Type, TypeVar

from google.protobuf.message import Message as ProfobufMsg

from mosaicolabs.models.core.unmodeled import Unmodeled

from ..adapter_base import (
    MCAPAdapterBase,
    compute_mcap_msg_type,
)

UnmodeledT = TypeVar("UnmodeledT", bound=Unmodeled)
McapT = TypeVar("McapT", Dict, ProfobufMsg)

_UNMODELED_ADAPTERS_REGISTRY: Dict[str, Type["UnmodeledAdapter"]] = {}


class UnmodeledAdapter(MCAPAdapterBase[UnmodeledT, McapT], Generic[UnmodeledT, McapT]):
    """
    Adapter for translating MCAP messages to Mosaico `Unmodeled` subclasses.
    """

    """TODO: add documentation on this class variable"""  # FIXME: change name
    __mcap_type__: Type[McapT]

    @classmethod
    def from_dict(cls, mcap_data: dict) -> UnmodeledT:
        """
        Converts the raw dictionary data into the specific Mosaico `Unmodeled` type.

        Args:
            mcap_data (dict): The raw dictionary from the ROS message.

        Returns:
            T: The constructed `Unmodeled` subclass instance wrapping `mcap_data`.
        """
        return cls.__mosaico_ontology_type__(raw_data=mcap_data)

    @classmethod
    def to_mcap(cls, mosaico_data: UnmodeledT) -> McapT:
        """TODO"""
        return cls.__mcap_type__(**mosaico_data.raw_data)

    @classmethod
    def get_or_create(
        cls,
        ontology_type: Type[UnmodeledT],
        mcap_type: Type[McapT],
        schema_name: str,
        schema_encoding: str,
    ) -> Type["UnmodeledAdapter"]:
        """
        Gets or create an unmodeled adapter for the provided ontology type, schema name and schema encoding.
        If the adapter is found within _UNMODELED_ADAPTERS_REGISTRY it is returned immediately.
        Conversely, it is first registered and then returned.
        """
        key = compute_mcap_msg_type(schema_name, schema_encoding)
        adapter = _UNMODELED_ADAPTERS_REGISTRY.get(key)

        if adapter is None:
            adapter = type(
                f"{ontology_type.__name__}Adapter",
                (cls,),
                {
                    "__mosaico_ontology_type__": ontology_type,
                    "__mcap_type__": mcap_type,
                    "schema_name": schema_name,
                    "schema_encoding": schema_encoding,
                },
            )
            _UNMODELED_ADAPTERS_REGISTRY[key] = adapter

        return adapter
