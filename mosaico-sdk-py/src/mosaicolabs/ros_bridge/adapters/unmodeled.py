from typing import Any, Dict, Generic, Optional, Tuple, Type, TypeVar, Union

from rosbags.typesys.store import Typestore

from mosaicolabs.models.core.message import Message
from mosaicolabs.models.core.unmodeled import Unmodeled
from mosaicolabs.ros_bridge.ros_message import ROSMessage

from ..adapter_base import ROSAdapterBase

T = TypeVar("T", bound=Unmodeled)

_UNMODELED_ADAPTERS_REGISTRY: Dict[str, Type["UnmodeledAdapter"]] = {}


class UnmodeledAdapter(ROSAdapterBase[T], Generic[T]):
    """
    Adapter for translating ROS messages to Mosaico `Unmodeled` subclasses.
    """

    ros_msgtype: str | Tuple[str, ...] = ()

    __mosaico_ontology_type__: Type[T]

    @classmethod
    def translate(
        cls,
        ros_msg: ROSMessage,  # ROSMessage
        **kwargs: Any,
    ) -> Message:
        """
        Main entry point for translating a high-level `ROSMessage`.

        Args:
            ros_msg: The source ROS message yielded by the loader.
            **kwargs: Additional context for the translation.

        Returns:
            A Mosaico `Message` containing the normalized `Pose` payload.
        """
        return super().translate(ros_msg, **kwargs)

    @classmethod
    def from_dict(cls, ros_data: dict) -> T:
        return cls.__mosaico_ontology_type__(raw_data=ros_data)

    @classmethod
    def to_ros(
        cls,
        mosaico_data: Union[Message, T],
        typestore: Typestore,
        ros_msg_type: Optional[str] = None,
    ) -> Any:
        """
        Converts a Mosaico `Unmodeled` subclass (or a ``Message`` wrapping one) into the
        corresponding ROS message.
        """
        raise NotImplementedError(
            f"The input ros message type {ros_msg_type} is supported but not implemented"
        )

    @classmethod
    def schema_metadata(
        cls,
        typestore: Typestore,
        ros_msg_type: str,
        **kwargs,
    ) -> Optional[dict]:
        """
        Extract the ROS message specific schema metadata, if any.
        """
        return super().schema_metadata(typestore, ros_msg_type, **kwargs)

    @classmethod
    def get_or_create(
        cls, ontology_type: Type[T], msgtype: str
    ) -> Type["UnmodeledAdapter"]:
        """
        TODO
        """
        key = ontology_type.__registry_key__ or ontology_type.ontology_tag()
        adapter = _UNMODELED_ADAPTERS_REGISTRY.get(key)
        if adapter is None:
            adapter = type(
                f"{ontology_type.__name__}Adapter",
                (UnmodeledAdapter,),
                {"__mosaico_ontology_type__": ontology_type, "ros_msgtype": msgtype},
            )
            _UNMODELED_ADAPTERS_REGISTRY[key] = adapter

        return adapter
