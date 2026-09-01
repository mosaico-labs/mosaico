from typing import (
    Any,
    Dict,
    Generic,
    Optional,
    Type,
    TypeVar,
)

from mosaicolabs.models.core import Message, Serializable

from .adapter_base import MCAPAdapterBase, compute_mcap_msg_type
from .mcap_message import MCAPMessage

T = TypeVar("T", bound=Serializable)


class MCAPBridge(Generic[T]):
    """
    A central registry and API for MCAP message to Mosaico Ontology translation.

    The `MCAPBridge` serves as the orchestration hub for the MCAP Bridge system. It maintains
     a global registry of all available `MCAPAdapterBase` implementations and provides
     the high-level API used to transform raw MCAP message containers into strongly-typed
     Mosaico `Message` objects.


    ### Key Responsibilities
    * **Adapter Discovery**: Provides methods to lookup adapters based on MCAP message's schema name and schema encoding type strings (e.g., `sensor_msgs.Imu`, `jsonschema`).
    * **Type Validation**: Checks if a given MCAP type (schema_name__schema_encoding) or Mosaico Ontology class is currently supported by the bridge.
    * **Execution Dispatch**: Acts as the primary entry point for the injection pipeline to delegate translation tasks to specific specialized adapters.

    Attributes:
        _adapters (Dict[str, Type[MCAPAdapterBase]]): A private class-level dictionary
            mapping canonical ROS message type strings to their respective adapter classes.
    """

    # Maps MCAP Message type schema_name__schema_encoding (e.g., sensor_msgs.Imu__jsonschema) to its default Adapter Class
    _default_adapters: Dict[str, Type[MCAPAdapterBase]] = {}
    # Maps Mosaico Message Type (e.g., Imu) to its default Adapter Class # TODO: understand whether these in MCAP<->Mosaico are useful
    # _default_mosaico_adapters: Dict[str, Type[MCAPAdapterBase]] = {}

    @classmethod
    def get_default_adapters(cls):
        return cls._default_adapters

    # @classmethod
    # def get_default_mosaico_adapters(cls):
    #     return cls._default_mosaico_adapters

    @classmethod
    def _register_default_adapter(cls, adapter_class: Type[MCAPAdapterBase]):
        """
        Internal helper for registering a default adapter class for one or more specific MCAP message types.

        It populates the internal registry, allowing the bridge to automatically handle
        new message types during mcap ingestion. Users must use the @register_default_adapter decorator instead.

        Args:
            adapter_class (Type[MCAPAdapterBase]): A class inheriting from `MCAPAdapterBase` that defines the
                translation logic and target MCAP types schema_name__schema_encoding.

        Raises:
            ValueError: If an adapter is already registered for any of the MCAP types schema_name__schema_encoding
                defined in the `adapter_class`.
        """
        mcap_msg_type = compute_mcap_msg_type(
            adapter_class.schema_name, adapter_class.schema_encoding
        )

        if mcap_msg_type in cls._default_adapters:
            raise ValueError(
                f"Adapter for ROS message type '{mcap_msg_type}' is already registered."
            )

        cls._default_adapters[mcap_msg_type] = adapter_class

        # if not is_default:
        #     return

        # ontology_tag = adapter_class.ontology_data_type().ontology_tag()

        # found_adapter = cls._default_mosaico_adapters.get(ontology_tag)

        # if found_adapter is not None:
        #     raise ValueError(
        #         f"{ontology_tag} already maps {found_adapter.__name__} adapter and cannot therefore map also '{adapter_class.__name__}'. \
        #             Are both adapters defined as default?"
        #     )

        # cls._default_mosaico_adapters[ontology_tag] = adapter_class

    @classmethod
    def get_default_adapter(
        cls, schema_name: str, schema_encoding: str
    ) -> Optional[Type[MCAPAdapterBase]]:
        """
        Retrieves the registered adapter class for a given MCAP message type. The MCAP message type is
        retriven using compute_mcap_msg_type(schema_name, schema_encoding)

        Args:
            schema_name (str): The MCAP message schema name (e.g. "sensor_msgs.Image").
            schema_encoding (str): The MCAP message schema encoding (e.g. "jsonschema").

        Returns:
            Optional[Type[MCAPAdapterBase]]: The corresponding `MCAPAdapterBase` subclass if found, otherwise `None`.
        """
        mcap_msg_type = compute_mcap_msg_type(schema_name, schema_encoding)

        return cls._default_adapters.get(mcap_msg_type)

    # @classmethod
    # def get_default_mosaico_adapter(
    #     cls, mosaico_type: str
    # ) -> Optional[Type[MCAPAdapterBase]]:
    #     """
    #     Retrieves the default adapter for a given Mosaico ontology type tag.

    #     Only adapters registered with ``is_default=True`` are returned; these are
    #     the canonical choices for Mosaico → ROS translation.

    #     Args:
    #         mosaico_type (str): The ontology tag string (e.g., ``"imu"``, ``"image"``).

    #     Returns:
    #         Optional[Type[MCAPAdapterBase]]: The corresponding ``MCAPAdapterBase`` subclass if one is registered,
    #             otherwise ``None``.
    #     """
    #     return cls._default_mosaico_adapters.get(mosaico_type)

    @classmethod
    def is_msgtype_adapted(cls, mcap_msg_type: str) -> bool:
        """
        Checks if a specific ROS message type has a registered translator. The MCAP message type
        can be retriven using compute_mcap_msg_type(schema_name, schema_encoding)

        Args:
            mcap_msg_type (str): The full MCAP message type string (e.g., "sensor_msg.Imu__protobuf").

        Returns:
            bool: True if the type is supported, False otherwise.
        """
        return mcap_msg_type in cls._default_adapters

    # @classmethod
    # def is_mosaico_type_adapted(cls, mosaico_type: str) -> bool:
    #     """
    #     Checks whether a Mosaico ontology type has a registered default adapter
    #     for reverse translation (Mosaico → ROS).

    #     Args:
    #         mosaico_type (str): The ontology tag string to check.

    #     Returns:
    #         bool: ``True`` if a default adapter exists for this type, ``False`` otherwise.
    #     """
    #     return mosaico_type in cls._default_mosaico_adapters

    @classmethod
    def is_adapted(cls, mosaico_cls: T) -> bool:
        """
        Checks if a specific Mosaico Ontology class has a registered adapter.

        Args:
            mosaico_cls (Type[Message]): The Mosaico class to check (e.g., `Image`, `Imu`).

        Returns:
            bool: True if an adapter exists for this class, False otherwise.
        """
        return any(
            val.ontology_data_type() == mosaico_cls
            for val in cls._default_adapters.values()
        )

    # --- Main Bridge API ---

    @classmethod
    def from_mcap_message(
        cls, mcap_msg: MCAPMessage, **kwargs: Any
    ) -> Optional[Message]:
        """
        The high-level API for translating raw MCAP message containers.

        This method identifies the appropriate adapter based on the `mcap_msg_type` inside
        the `MCAPMessage` and invokes its `translate` method. It is the core function called
        by the `RosbagInjector` during the ingestion loop.

        Example:
            ```python
            # Within an ingestion loop
            mosaico_msg = MCAPBridge.from_ros_message(raw_ros_container)
            if mosaico_msg:
                writer.push(mosaico_msg)
            ```

        Args:
            mcap_msg (MCAPMessage): The `MCAPMessage` container produced by the `ROSLoader`.
            **kwargs: Arbitrary context arguments passed directly to the adapter's translate method.

        Returns:
            Optional[Message]: A fully constructed Mosaico `Message` if an adapter is available, otherwise `None`.
        """
        mcap_msg_type = compute_mcap_msg_type(
            mcap_msg.schema_name, mcap_msg.schema_encoding
        )
        adapter_class = cls.get_default_adapter(mcap_msg_type)
        if adapter_class is None:
            return None

        # Delegate the translation to the specific adapter
        return adapter_class.translate(mcap_msg, **kwargs)

    # @classmethod
    # def from_mosaico_message(cls, mosaico_msg: Message) -> Optional[LibMcapMessage]: # TODO: LibMcapMessage should be renamed
    #     """
    #     The high-level API for translating a Mosaico `Message` into a MCAP message from mcap library.

    #     This method identifies the appropriate adapter based on the message's ontology
    #     tag and invokes its `to_mcap` method.

    #     Args:
    #         mosaico_msg (Message): The Mosaico `Message` to translate.
    #         store (Stores): The rosbags typestore identifier used to resolve the target ROS type.
    #         ros_msg_type (Optional[str]): Override for the output ROS type. If `None`,
    #             the adapter's default ROS type is used.

    #     Returns:
    #         Optional[MsgType]: The constructed ROS message if a default adapter is registered
    #             for the message's ontology tag, otherwise `None`.
    #     """
    #     adapter_class = cls._default_mosaico_adapters.get(mosaico_msg.ontology_tag())
    #     if adapter_class is None:
    #         return None

    #     # Delegate the translation to the specific adapter
    #     return adapter_class.to_ros(mosaico_msg, get_typestore(store), ros_msg_type)


def register_default_adapter(
    cls: Type[MCAPAdapterBase],
) -> Type[MCAPAdapterBase]:
    """
    A class decorator for streamlined default adapter registration.

    This is the recommended way to register adapters in a production environment,
    as it couples the adapter definition directly with its registration in the bridge.

    Example:
        ```python
        from mosaicolabs.bridges.ros import register_default_adapter, MCAPAdapterBase

        @register_default_adapter(is_default=False)
        class MySensorAdapter(MCAPAdapterBase):
            ros_msgtype = "sensor_msgs/msg/Temperature"
            # ...
        ```

    Args:
        is_default (bool): flag indicating that this adapter should be used when traslating from Mosaico to ROS

    Returns:
        Callable[[type["MCAPAdapterBase"]], type["MCAPAdapterBase"]]: The same class, unmodified,
            after successful registration.
    """

    MCAPBridge._register_default_adapter(cls)

    return cls
