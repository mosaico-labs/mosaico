from typing import Any, ClassVar, Dict, Generic, Type, TypeVar

from mosaicolabs.bridges.mcap.mcap_message import MCAPMessage
from mosaicolabs.models.core.message import Message
from mosaicolabs.models.core.unmodeled import Unmodeled

from ..adapter_base import MCAPAdapterBase

T = TypeVar("T", bound=Unmodeled)

_UNMODELED_ADAPTERS_REGISTRY: Dict[str, Type["UnmodeledAdapter"]] = {}


class UnmodeledAdapter(MCAPAdapterBase[T], Generic[T]):
    """
    Adapter for translating MCAP messages to Mosaico `Unmodeled` subclasses.
    """

    schema_name: ClassVar[str]
    schema_encoding: ClassVar[str]
    skip_encoding_check: ClassVar[bool] = True

    __mosaico_ontology_type__: Type[T]

    @classmethod
    def translate(
        cls,
        msg: MCAPMessage,
        **kwargs: Any,
    ) -> Message:
        """
        Main entry point for translating a high-level `MCAPMessage`.

        Args:
            msg (MCAPMessage): The source MCAP message yielded by the loader.
            **kwargs: Additional context for the translation.

        Returns:
            Message: A Mosaico `Message` containing the normalized `Unmodeled` payload.
        """
        return super().translate(msg, **kwargs)

    @classmethod
    def from_dict(cls, mcap_data: dict) -> T:
        """
        Converts the raw dictionary data into the specific Mosaico `Unmodeled` type.

        Args:
            mcap_data (dict): The raw dictionary from the ROS message.

        Returns:
            T: The constructed `Unmodeled` subclass instance wrapping `mcap_data`.
        """
        return cls.__mosaico_ontology_type__(raw_data=mcap_data)

    @classmethod
    def get_or_create(
        cls, ontology_type: Type[T], schema_name: str, schema_encoding: str
    ) -> Type["UnmodeledAdapter"]:
        """
        Gets or create an unmodeled adapter for the provided ontology type, schema name and schema encoding.
        If the adapter is found within _UNMODELED_ADAPTERS_REGISTRY it is returned immediately.
        Conversely, it is first registered and then returned.
        """
        key = ontology_type.__registry_key__ or ontology_type.ontology_tag()
        adapter = _UNMODELED_ADAPTERS_REGISTRY.get(key)
        if adapter is None:
            adapter = type(
                f"{ontology_type.__name__}Adapter",
                (UnmodeledAdapter,),
                {
                    "__mosaico_ontology_type__": ontology_type,
                    "schema_name": schema_name,
                    "schema_encoding": schema_encoding,
                },
            )
            _UNMODELED_ADAPTERS_REGISTRY[key] = adapter

        return adapter
