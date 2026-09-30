from typing import Dict, Generic, Optional, Tuple, Type, TypeVar

from mosaicolabs.models.core.unmodeled import Unmodeled

from ..adapter_base import (
    MCAPAdapterBase,
    McapT,
    compute_mcap_msg_type,
)

UnmodeledT = TypeVar("UnmodeledT", bound=Unmodeled)

_UNMODELED_ADAPTERS_REGISTRY: Dict[str, Type["UnmodeledAdapter"]] = {}


class UnmodeledAdapter(MCAPAdapterBase[UnmodeledT, McapT], Generic[UnmodeledT, McapT]):
    """
    Adapter for translating MCAP messages to Mosaico `Unmodeled` subclasses.

    This is the only MCAP adapter provided by the library. MCAP is a container
    format that can carry messages of any schema and encoding, so the structure
    of a message cannot be known in advance and no dedicated adapters can be
    shipped for it. Every MCAP message is therefore stored in Mosaico as an
    `Unmodeled` payload, keeping its decoded content as raw data.

    Rebuilding the original MCAP message from the data saved in Mosaico requires
    the original object type to fill with that data, in the same way as the ROS
    `UnmodeledAdapter` relies on a `Typestore` holding the original message
    types. For this purpose, the adapter defines the `__mcap_class__` class
    variable, which holds the class used to reconstruct the original object. It
    is set when each new `UnmodeledAdapter` is created.
    """

    __mcap_class__: Type[McapT]
    """The native MCAP message class (a protobuf `Message` class or `Dict`) instantiated by
    [`to_mcap`][mosaicolabs.bridges.mcap.adapters.unmodeled.UnmodeledAdapter.to_mcap]."""

    @classmethod
    def from_dict(cls, mcap_data: dict, publish_time_ns: int) -> UnmodeledT:
        """
        Converts the raw dictionary data into the specific Mosaico `Unmodeled` type.

        Args:
            mcap_data (dict): The decoded MCAP message payload. It is updated in place with
                the `publish_time_ns` key.
            publish_time_ns (int): Message publish time (nanoseconds).

        Returns:
            UnmodeledT: The constructed `Unmodeled` subclass instance wrapping `mcap_data`.
        """

        mcap_data.update({"publish_time_ns": publish_time_ns})

        return cls.__mosaico_ontology_type__(raw_data=mcap_data)

    @classmethod
    def to_mcap(cls, mosaico_data: UnmodeledT) -> Tuple[McapT, Optional[int]]:
        """
        Rebuilds the native MCAP message from the `raw_data` of an `Unmodeled` instance.

        Args:
            mosaico_data (UnmodeledT): The `Unmodeled` instance to convert. The `publish_time_ns`
                key is popped from its `raw_data`.

        Returns:
            Tuple[McapT, Optional[int]]: The `__mcap_class__` instance built from `raw_data` and the
                publish time (nanoseconds), or `None` if not present.
        """

        publish_time_ns = mosaico_data.raw_data.pop("publish_time_ns", None)

        return cls.__mcap_class__(**mosaico_data.raw_data), publish_time_ns

    @classmethod
    def get_or_create(
        cls,
        ontology_type: Type[UnmodeledT],
        mcap_class: Type[McapT],
        schema_name: str,
        schema_encoding: str,
    ) -> Type["UnmodeledAdapter"]:
        """
        Gets or creates an unmodeled adapter for the provided schema name and schema encoding.

        If an adapter for the pair is already registered it is returned immediately;
        otherwise a new subclass is created, registered and returned.

        Args:
            ontology_type (Type[UnmodeledT]): The `Unmodeled` subclass produced by the adapter.
            mcap_class (Type[McapT]): The native MCAP message class rebuilt by `to_mcap()`.
            schema_name (str): The MCAP schema name handled by the adapter.
            schema_encoding (str): The MCAP schema encoding handled by the adapter.

        Returns:
            Type[UnmodeledAdapter]: The registered adapter class.
        """
        key = compute_mcap_msg_type(schema_name, schema_encoding)
        adapter = _UNMODELED_ADAPTERS_REGISTRY.get(key)

        if adapter is None:
            adapter = type(
                f"{ontology_type.__name__}Adapter",
                (cls,),
                {
                    "__mosaico_ontology_type__": ontology_type,
                    "__mcap_class__": mcap_class,
                    "schema_name": schema_name,
                    "schema_encoding": schema_encoding,
                },
            )
            _UNMODELED_ADAPTERS_REGISTRY[key] = adapter

        return adapter
