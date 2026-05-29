from typing import Any, Dict, Generic, Optional, Type, TypeVar

from rosbags.typesys import Stores, get_typestore

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

    # Maps ROS Message Type (e.g., sensor_msgs.msg.Imu) to its default Adapter Class
    _default_adapters: Dict[str, Type[ROSAdapterBase]] = {}
    # Maps Mosaico Message Type (e.g., Imu) to its default Adapter Class
    _default_mosaico_adapters: Dict[str, Type[ROSAdapterBase]] = {}

    @classmethod
    def get_default_adapters(cls):
        return cls._default_adapters

    @classmethod
    def get_default_mosaico_adapters(cls):
        return cls._default_mosaico_adapters

    @classmethod
    def _register_default_adapter(
        cls, adapter_class: Type[ROSAdapterBase], is_default: bool = False
    ):
        """
        Internal helper for registering a default adapter class for one or more specific ROS message types.

        It populates the internal registry, allowing the bridge to automatically handle
        new message types during bag ingestion. Users must use the @register_default_adapter decorator instead.

        Args:
            adapter_class: A class inheriting from `ROSAdapterBase` that defines the
                translation logic and target ROS types.

        Raises:
            ValueError: If an adapter is already registered for any of the ROS types
                defined in the `adapter_class`.
        """
        ros_types = adapter_class.ros_msgtype
        adapter_class.__is_default_adapter__ = is_default

        # Normalize to tuple
        if isinstance(ros_types, str):
            ros_types = (ros_types,)

        for ros_type in ros_types:
            if ros_type in cls._default_adapters:
                raise ValueError(
                    f"Adapter for ROS message type '{ros_type}' is already registered."
                )
            cls._default_adapters[ros_type] = adapter_class

        if not is_default:
            return

        ontology_tag = adapter_class.ontology_data_type().ontology_tag()

        if ontology_tag in cls._default_mosaico_adapters:
            found_adapter = cls._default_mosaico_adapters.get(ontology_tag)

            raise ValueError(
                f"{ontology_tag} already maps {found_adapter.__name__} adapter and cannot therefore map also '{adapter_class.__name__}'. \
                    Are both adapters defined as default?"
            )

        cls._default_mosaico_adapters[ontology_tag] = adapter_class

    @classmethod
    def get_default_adapter(cls, ros_msg_type: str) -> Optional[Type[ROSAdapterBase]]:
        """
        Retrieves the registered adapter class for a given ROS message type.

        Args:
            ros_msg_type: The full ROS message type string (e.g., "sensor_msgs/msg/Image").

        Returns:
            The corresponding `ROSAdapterBase` subclass if found, otherwise `None`.
        """
        return cls._default_adapters.get(ros_msg_type)

    @classmethod
    def get_default_mosaico_adapter(
        cls, mosaico_type: str
    ) -> Optional[Type[ROSAdapterBase]]:
        """
        Retrieves the default adapter for a given Mosaico ontology type tag.

        Only adapters registered with ``is_default=True`` are returned; these are
        the canonical choices for Mosaico → ROS translation.

        Args:
            mosaico_type: The ontology tag string (e.g., ``"imu"``, ``"image"``).

        Returns:
            The corresponding ``ROSAdapterBase`` subclass if one is registered,
            otherwise ``None``.
        """
        return cls._default_mosaico_adapters.get(mosaico_type)

    @classmethod
    def is_msgtype_adapted(cls, ros_msg_type: str) -> bool:
        """
        Checks if a specific ROS message type has a registered translator.

        Returns:
            bool: True if the type is supported, False otherwise.
        """
        return ros_msg_type in cls._default_adapters

    @classmethod
    def is_mosaico_type_adapted(cls, mosaico_type: str) -> bool:
        """
        Checks whether a Mosaico ontology type has a registered default adapter
        for reverse translation (Mosaico → ROS).

        Args:
            mosaico_type: The ontology tag string to check.

        Returns:
            ``True`` if a default adapter exists for this type, ``False`` otherwise.
        """
        return mosaico_type in cls._default_mosaico_adapters

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
            val.ontology_data_type() == mosaico_cls
            for val in cls._default_adapters.values()
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
        adapter_class = cls.get_default_adapter(ros_msg.msg_type)
        if adapter_class is None:
            return None

        # Delegate the translation to the specific adapter
        return adapter_class.translate(ros_msg, **kwargs)

    @classmethod
    def from_mosaico_message(
        cls, mosaico_msg: Message, store: Stores, ros_msg_type: Optional[str] = None
    ):  # TODO: is this useful?

        adapter_class = cls._default_mosaico_adapters.get(mosaico_msg.ontology_tag())
        if adapter_class is None:
            return None

        # Delegate the translation to the specific adapter
        return adapter_class.to_ros(mosaico_msg, get_typestore(store), ros_msg_type)


def register_default_adapter(is_default: bool = False):
    """
    A class decorator for streamlined default adapter registration.

    This is the recommended way to register adapters in a production environment,
    as it couples the adapter definition directly with its registration in the bridge.

    Example:
        ```python
        from mosaicolabs.ros_bridge import register_default_adapter, ROSAdapterBase

        @register_default_adapter(is_default: bool = False):
        class MySensorAdapter(ROSAdapterBase):
            ros_msgtype = "sensor_msgs/msg/Temperature"
            # ...
        ```

    Args:
        cls: The adapter class to register.
        is_default: flag indicating that this adapter should be used when traslating from Mosaico to ROS

    Returns:
        The same class, unmodified, after successful registration.
    """

    def wrapper(cls: Type["ROSAdapterBase"]) -> Type["ROSAdapterBase"]:
        ROSBridge._register_default_adapter(cls, is_default)
        return cls

    return wrapper
