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

    # @classmethod
    # def to_mcap(
    #     cls,
    #     mosaico_data: Union[Message, T],
    #     typestore: Typestore,
    #     ros_msg_type: Optional[str] = None,
    # ) -> Any:
    #     """
    #     Converts a Mosaico `Unmodeled` subclass (or a ``Message`` wrapping one) into the
    #     corresponding ROS message.

    #     Args:
    #         mosaico_data (Union[Message, T]): A ``Message`` wrapping an `Unmodeled` instance, or a raw instance.
    #         typestore (Typestore): The rosbags typestore for target type resolution.
    #         ros_msg_type (Optional[str]): Override for the output ROS type. Defaults to
    #             the adapter's default ROS type if `None`.

    #     Returns:
    #         Any: The constructed ROS message, or raises an error if:

    #             - the ros_msg_type is unsupported by adapter (TypeError)
    #             - the ros_msg_type or default type are unsupported by typestore (TypeError)
    #     """
    #     # Resolve ROS message to translate Mosaico message to if not defined in input
    #     resolved_rosmsg_type = ros_msg_type or cls.get_default_ros_msg()
    #     if not cls.is_rosmsg_type_valid(resolved_rosmsg_type):
    #         raise TypeError(
    #             f"Adapter {cls.__name__} does not support {resolved_rosmsg_type}"
    #         )

    #     # Checking presence in typestore of requested message
    #     RosUnmodeled = typestore.types.get(resolved_rosmsg_type)
    #     if RosUnmodeled is None:
    #         raise TypeError(f"Typestore does not contain {resolved_rosmsg_type}")

    #     # Unpacking Mosaico message / type
    #     unmodeled_data, _ = cls.unpack_mosaico_msg(mosaico_data)

    #     # Filling the data
    #     rosbag_msgdef = typestore.get_msgdef(resolved_rosmsg_type)

    #     try:
    #         return RosUnmodeled(
    #             **pack_unmodeled(
    #                 unmodeled_data.raw_data, rosbag_msgdef.fields, typestore
    #             )
    #         )
    #     except KeyError as ke:
    #         raise KeyError(
    #             "Error occurred while creating ROS message. It is probably due to a schema mismatch or a partially adapted message type."
    #             f" Please check if the adapter for the msgtype `{resolved_rosmsg_type}` does implement the `to_ros()` method."
    #             f" Inner err: {ke}"
    #         ) from ke
    #     except Exception as e:
    #         raise RuntimeError(
    #             f"Unexpected error occurred while creating ROS message: {e}"
    #         ) from e

    # @classmethod
    # def schema_metadata(
    #     cls, typestore: Typestore, ros_msg_type: str, ros_version: int
    # ) -> Optional[dict]:
    #     """
    #     Extract the ROS message specific schema metadata, if any.

    #     Args:
    #         typestore (Typestore): The rosbags typestore for target type resolution.
    #         ros_msg_type (str): The ROS message type to extract metadata for.
    #         ros_version (int): The ROS version (1 or 2) to consider for metadata extraction.

    #     Returns:
    #         Optional[dict]: A dictionary containing the schema metadata, or None if not applicable.
    #     """
    #     return super().schema_metadata(typestore, ros_msg_type, ros_version)

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
