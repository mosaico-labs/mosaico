from typing import Dict, Generic, Optional, Type, Any, TypeVar

from mosaicolabs.models import Message, Serializable

from .adapter_base import ROSAdapterBase
from .ros_message import ROSMessage

T = TypeVar("T", bound=Serializable)


class ROSBridge(Generic[T]):
    """
    A central registry and API for ROS message to Mosaico Ontology translation.

    The `ROSBridge` serves as the orchestration hub for the ROS Bridge system. It maintains
     a global registry of all available `ROSAdapterBase` implementations and provides
     the high-level API used to transform raw ROS message containers into strongly-typed
     Mosaico `Message` objects.


    ### Key Responsibilities
    * **Adapter Discovery**: Provides methods to lookup adapters based on ROS message type strings (e.g., `sensor_msgs/msg/Imu`).
    * **Type Validation**: Checks if a given ROS type or Mosaico Ontology class is currently supported by the bridge.
    * **Execution Dispatch**: Acts as the primary entry point for the injection pipeline to delegate translation tasks to specific specialized adapters.

    Attributes:
        _adapters (Dict[str, Type[ROSAdapterBase]]): A private class-level dictionary
            mapping canonical ROS message type strings to their respective adapter classes.
    """

    # Maps ROS Message Type (e.g., sensor_msgs.msg.Imu) to its Adapter Class
    _adapters: Dict[str, Type[ROSAdapterBase]] = {}

    @classmethod
    def get_adapters(cls):
        return cls._adapters

    @classmethod
    def _register_adapter(cls, adapter_class: Type[ROSAdapterBase]):
        """
        Internal helper for registering an adapter class for one or more specific ROS message types.

        It populates the internal registry, allowing the bridge to automatically handle
        new message types during bag ingestion. Users must use the @register_adapter decorator instead.

        Args:
            adapter_class: A class inheriting from `ROSAdapterBase` that defines the
                translation logic and target ROS types.

        Raises:
            ValueError: If an adapter is already registered for any of the ROS types
                defined in the `adapter_class`.
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
        """
        Retrieves the registered adapter class for a given ROS message type.

        Args:
            ros_msg_type: The full ROS message type string (e.g., "sensor_msgs/msg/Image").

        Returns:
            The corresponding `ROSAdapterBase` subclass if found, otherwise `None`.
        """
        return cls._adapters.get(ros_msg_type)

    @classmethod
    def is_msgtype_adapted(cls, ros_msg_type: str) -> bool:
        """
        Checks if a specific ROS message type has a registered translator.

        Returns:
            bool: True if the type is supported, False otherwise.
        """
        return ros_msg_type in cls._adapters

    @classmethod
    def is_adapted(cls, mosaico_cls: T) -> bool:
        """
        Checks if a specific Mosaico Ontology class has a registered adapter.

        Args:
            mosaico_cls: The Mosaico class to check (e.g., `Image`, `Imu`).

        Returns:
            bool: True if an adapter exists for this class, False otherwise.
        """
        return any(
            val.ontology_data_type() == mosaico_cls for val in cls._adapters.values()
        )

    # --- Main Bridge API ---

    @classmethod
    def from_ros_message(cls, ros_msg: ROSMessage, **kwargs: Any) -> Optional[Message]:
        """
        The high-level API for translating raw ROS message containers.

        This method identifies the appropriate adapter based on the `msg_type` inside the
        `ROSMessage` and invokes its `translate` method. It is the core function called
        by the `RosbagInjector` during the ingestion loop.

        Example:
            ```python
            # Within an ingestion loop
            mosaico_msg = ROSBridge.from_ros_message(raw_ros_container)
            if mosaico_msg:
                writer.push(mosaico_msg)
            ```

        Args:
            ros_msg: The `ROSMessage` container produced by the `ROSLoader`.
            **kwargs: Arbitrary context arguments passed directly to the adapter's translate method.

        Returns:
            A fully constructed Mosaico `Message` if an adapter is available, otherwise `None`.
        """
        adapter_class = cls.get_adapter(ros_msg.msg_type)
        if adapter_class is None:
            return None

        # Delegate the translation to the specific adapter
        return adapter_class.translate(ros_msg, **kwargs)


def register_adapter(cls: Type["ROSAdapterBase"]) -> Type["ROSAdapterBase"]:
    """
    A class decorator for streamlined adapter registration.

    This is the recommended way to register adapters in a production environment,
    as it couples the adapter definition directly with its registration in the bridge.

    Example:
        ```python
        from mosaicolabs.ros_bridge import register_adapter, ROSAdapterBase

        @register_adapter
        class MySensorAdapter(ROSAdapterBase):
            ros_msgtype = "sensor_msgs/msg/Temperature"
            # ...
        ```

    Args:
        cls: The adapter class to register.

    Returns:
        The same class, unmodified, after successful registration.
    """
    ROSBridge._register_adapter(cls)
    return cls
