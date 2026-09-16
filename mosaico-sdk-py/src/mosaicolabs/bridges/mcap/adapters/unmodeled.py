from typing import Any, Callable, ClassVar, Dict, Generic, Type, TypeVar, cast

from google.protobuf.message import Message as ProfobufMsg

from mosaicolabs.models.core.unmodeled import Unmodeled

from ..adapter_base import (
    MCAPAdapterBase,
    compute_mcap_msg_type,
)

UnmodeledT = TypeVar("UnmodeledT", bound=Unmodeled)
McapT = TypeVar("McapT", Dict, ProfobufMsg)

_UNMODELED_ADAPTERS_REGISTRY: Dict[str, Type["UnmodeledAdapter"]] = {}


def _to_mcap_protobuf(
    mosaico_data: UnmodeledT, mcap_class: Type[ProfobufMsg]
) -> ProfobufMsg: ...


def _to_mcap_jsonschema(mosaico_data: UnmodeledT, mcap_class: Type[Dict]) -> Dict: ...


class UnmodeledAdapter(MCAPAdapterBase[UnmodeledT, McapT], Generic[UnmodeledT, McapT]):
    """
    Adapter for translating MCAP messages to Mosaico `Unmodeled` subclasses.

    A single class handles every schema encoding: `schema_encoding` is set at
    creation time by `get_or_create()`, and `to_mcap` dispatches to the
    matching per-encoding classmethod via `_TO_MCAP_STRATEGIES`.
    """

    # Each strategy is concretely typed to its own encoding's mcap wire type
    # (ProfobufMsg or Dict), so the table as a whole can't be expressed as a
    # single Callable[[UnmodeledT, Type[McapT]], McapT] — it's a runtime,
    # string-keyed union of signatures. `to_mcap` casts the result back to
    # McapT at the one point it's consumed.
    _TO_MCAP_STRATEGIES: ClassVar[Dict[str, Callable[[Any, Type[Any]], Any]]] = {
        "protobuf": _to_mcap_protobuf,
        "jsonschema": _to_mcap_jsonschema,
    }

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
    def to_mcap(cls, mosaico_data: UnmodeledT, mcap_class: Type[McapT]) -> McapT:
        strategy = cls._TO_MCAP_STRATEGIES.get(cls.schema_encoding)

        if not strategy:
            raise RuntimeError(
                f"Unsupported schema encoding '{cls.schema_encoding}' for Unmodeled adapter."
            )

        return cast(McapT, strategy(mosaico_data, mcap_class))

    @classmethod
    def get_or_create(
        cls, ontology_type: Type[UnmodeledT], schema_name: str, schema_encoding: str
    ) -> Type["UnmodeledAdapter"]:
        """
        Gets or create an unmodeled adapter for the provided ontology type, schema name and schema encoding.
        If the adapter is found within _UNMODELED_ADAPTERS_REGISTRY it is returned immediately.
        Conversely, it is first registered and then returned.
        """
        key = compute_mcap_msg_type(schema_name, schema_encoding)
        adapter = _UNMODELED_ADAPTERS_REGISTRY.get(key)

        if adapter is None:
            if schema_encoding not in cls._TO_MCAP_STRATEGIES:
                raise RuntimeError(
                    f"Unsupported schema encoding '{schema_encoding}' for Unmodeled adapter."
                )

            adapter = type(
                f"{ontology_type.__name__}Adapter",
                (cls,),
                {
                    "__mosaico_ontology_type__": ontology_type,
                    "schema_name": schema_name,
                    "schema_encoding": schema_encoding,
                },
            )
            _UNMODELED_ADAPTERS_REGISTRY[key] = adapter

        return adapter
