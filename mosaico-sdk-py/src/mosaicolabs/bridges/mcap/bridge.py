from typing import (
    Any,
    ClassVar,
    Dict,
    Generic,
    Optional,
    Tuple,
    Type,
    TypeVar,
)

from mcap.well_known import SchemaEncoding

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
        _default_adapters (Dict[str, Type[MCAPAdapterBase]]): A private class-level dictionary
            mapping canonical MCAP message type strings (schema_name__schema_encoding) to their
            respective adapter classes.
    """

    # Maps MCAP Message type schema_name__schema_encoding (e.g., sensor_msgs.Imu__jsonschema) to its default Adapter Class
    _default_adapters: Dict[str, Type[MCAPAdapterBase]] = {}

    # Mosaico supported encodings for MCAP
    __mosaico_supported_schema_encodings__: ClassVar[Tuple[str, ...]] = (
        SchemaEncoding.Protobuf,
        # SchemaEncoding.JSONSchema, # not yet supported
        # SchemaEncoding.ROS2, # not yet supported
        # SchemaEncoding.ROS2IDL, # not yet supported
        # SchemaEncoding.ROS1, # not yet supported
    )

    @classmethod
    def get_default_adapters(cls):
        return cls._default_adapters

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
                defined in the `adapter_class` or the defined adapter schema encoding is not among the supported ones.
        """

        # Check that adapter encoding exists and is contained within __mosaico_supported_schema_encodings__
        if not adapter_class.skip_encoding_check:
            if getattr(adapter_class, "schema_encoding", None) is None:
                raise ValueError(
                    f"{adapter_class.__name__} must defined a non-empty schema_encoding"
                )

            if (
                adapter_class.schema_encoding
                not in cls.__mosaico_supported_schema_encodings__
            ):
                raise ValueError(
                    f"{adapter_class.__name__} does not define a supported schema_encoding.\
                      Supported encoding are: {cls.__mosaico_supported_schema_encodings__}"
                )

        mcap_msg_type = compute_mcap_msg_type(
            adapter_class.schema_name, adapter_class.schema_encoding
        )

        if mcap_msg_type in cls._default_adapters:
            raise ValueError(
                f"Adapter for MCAP message type '{mcap_msg_type}' is already registered."
            )

        cls._default_adapters[mcap_msg_type] = adapter_class

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

    @classmethod
    def is_msgtype_adapted(cls, mcap_msg_type: str) -> bool:
        """
        Checks if a specific MCAP message type has a registered translator. The MCAP message type
        can be retriven using compute_mcap_msg_type(schema_name, schema_encoding)

        Args:
            mcap_msg_type (str): The full MCAP message type string (e.g., "sensor_msg.Imu__protobuf").

        Returns:
            bool: True if the type is supported, False otherwise.
        """
        return mcap_msg_type in cls._default_adapters

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
        by the `MCAPInjector` during the ingestion loop.

        Example:
            ```python
            # Within an ingestion loop
            mosaico_msg = MCAPBridge.from_mcap_message(raw_mcap_msg)
            if mosaico_msg:
                writer.push(mosaico_msg)
            ```

        Args:
            mcap_msg (MCAPMessage): The `MCAPMessage` container produced by the `MCAPLoader`.
            **kwargs: Arbitrary context arguments passed directly to the adapter's translate method.

        Returns:
            Optional[Message]: A fully constructed Mosaico `Message` if an adapter is available, otherwise `None`.
        """

        if not mcap_msg.schema_name or not mcap_msg.schema_encoding:
            return None

        adapter_class = cls.get_default_adapter(
            mcap_msg.schema_name, mcap_msg.schema_encoding
        )
        if adapter_class is None:
            return None

        # Delegate the translation to the specific adapter
        return adapter_class.translate(mcap_msg, **kwargs)


def register_default_adapter(
    cls: Type[MCAPAdapterBase],
) -> Type[MCAPAdapterBase]:
    """
    A class decorator for streamlined default adapter registration.

    This is the recommended way to register adapters in a production environment,
    as it couples the adapter definition directly with its registration in the bridge.

    Example:
        ```python
        from mosaicolabs.bridges.mcap import register_default_adapter, MCAPAdapterBase

        @register_default_adapter
        class MySensorAdapter(MCAPAdapterBase):
            schema_name = "sensor_msgs/Temperature"
            schema_encoding = "protobuf"
            # ...
        ```

    Args:
        cls (Type[MCAPAdapterBase]): The adapter class to register, identified by its
            `schema_name` and `schema_encoding`.

    Returns:
        Type[MCAPAdapterBase]: The same class, unmodified, after successful registration.
    """

    MCAPBridge._register_default_adapter(cls)

    return cls
