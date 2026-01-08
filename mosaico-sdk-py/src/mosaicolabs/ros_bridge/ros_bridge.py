from typing import Dict, Generic, Optional, Type, Any, TypeVar

from mosaicolabs.models import Message, Serializable

from .adapter_base import ROSAdapterBase
from .ros_message import ROSMessage

T = TypeVar("T", bound=Serializable)


class ROSBridge(Generic[T]):
    """
    A central registry and API for ROS message to Ontology data translation.

    Manages all registered adapters and provides the main translation method.
    """

    # Maps ROS Message Type (e.g., sensor_msgs.msg.Imu) to its Adapter Class
    _adapters: Dict[str, Type[ROSAdapterBase]] = {}

    @classmethod
    def get_adapters(cls):
        return cls._adapters

    @classmethod
    def register_adapter(cls, adapter_class: Type[ROSAdapterBase]):
        """
        Registers an adapter for a specific ROS message type.
        This is the primary hook for user custom data/adapters.
        """
        ros_types = adapter_class.ros_msgtype

        # Normalize to tuple
        if isinstance(ros_types, str):
            ros_types = (ros_types,)

        for ros_type in ros_types:
            if ros_type in cls._adapters:
                raise ValueError(
                    f"Adapter for ROS message type '{ros_type}' is already registered."
                )
            cls._adapters[ros_type] = adapter_class

    @classmethod
    def get_adapter(cls, ros_msg_type: str) -> Optional[Type[ROSAdapterBase]]:
        """Retrieves the correct adapter for the given ROS message type."""
        return cls._adapters.get(ros_msg_type)

    @classmethod
    def is_msgtype_adapted(cls, ros_msg_type: str) -> bool:
        return ros_msg_type in cls._adapters

    @classmethod
    def is_adapted(cls, mosaico_cls: T) -> bool:
        return any(
            val.ontology_data_type() == mosaico_cls for val in cls._adapters.values()
        )

    # --- Main Bridge API ---

    @classmethod
    def from_ros_message(cls, ros_msg: ROSMessage, **kwargs: Any) -> Optional[Message]:
        """
        The main public method to translate any registered ROS message.
        """
        adapter_class = cls.get_adapter(ros_msg.msg_type)
        if adapter_class is None:
            return None

        # Delegate the translation to the specific adapter
        return adapter_class.translate(ros_msg, **kwargs)


def register_adapter(cls):
    """
    Decorator to register a ROSAdapter in the ROSBridge.
    """
    ROSBridge.register_adapter(cls)
    return cls
