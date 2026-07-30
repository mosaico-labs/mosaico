"""
Standard ROS Message Adapters.

This module provides adapters for translating standard ROS messages (std_msgs)
into Mosaico ontology types. Instead of manually defining a class for every
single primitive type (Int8, String, Bool, etc.), we use a dynamic factory pattern.

Architecture:
 -  `_ROS_MSGTYPE_MSCO_BASE_TYPE_MAP` defines the relationship between a ROS
    message type string (e.g., "std_msgs/msg/String") and the corresponding
    Mosaico Serializable class (e.g., `String`).
 -  `GenericStdAdapter` implements the common `translate` and `from_dict` logics
    shared by all standard types (wrapping the 'data' field).
 -  At module load time, we iterate through the mapping, dynamically create
    a unique subclass of `GenericStdAdapter` for each type and register it
    in the ROSBridge.
"""

from __future__ import annotations

import dataclasses
from typing import TYPE_CHECKING, Any, Dict, Optional, Tuple, Type, Union

if TYPE_CHECKING:
    from rosbags.typesys.store import MsgType

from rosbags.typesys.store import Typestore

from mosaicolabs.models.core import Message, Serializable
from mosaicolabs.models.data import (
    Boolean,
    Floating32,
    Floating64,
    Header,
    Integer8,
    Integer16,
    Integer32,
    Integer64,
    String,
    Time,
    Unsigned8,
    Unsigned16,
    Unsigned32,
    Unsigned64,
)

from ..adapter_base import ROSAdapterBase
from ..ros_bridge import register_default_adapter
from ..ros_message import ROSMessage
from .builtin_interfaces import TimeAdapter
from .helpers import _validate_msgdata

# ---------------------------------------------------------------------------
# Type Mapping Configuration
# ---------------------------------------------------------------------------
# This dictionary is the single source of truth for standard type support.
# Adding a new mapping here automatically generates the corresponding adapter.

_ROS_MSGTYPE_MSCO_BASE_TYPE_MAP: Dict[str, Type[Serializable]] = {
    # String Types
    "std_msgs/msg/String": String,
    # Integer Types (Signed)
    "std_msgs/msg/Int8": Integer8,
    "std_msgs/msg/Int16": Integer16,
    "std_msgs/msg/Int32": Integer32,
    "std_msgs/msg/Int64": Integer64,
    # Integer Types (Unsigned)
    "std_msgs/msg/UInt8": Unsigned8,
    "std_msgs/msg/UInt16": Unsigned16,
    "std_msgs/msg/UInt32": Unsigned32,
    "std_msgs/msg/UInt64": Unsigned64,
    # Floating Point Types
    "std_msgs/msg/Float32": Floating32,
    "std_msgs/msg/Float64": Floating64,
    # Boolean
    "std_msgs/msg/Bool": Boolean,
}


# ---------------------------------------------------------------------------
# Logic Template
# ---------------------------------------------------------------------------


class GenericStdAdapter(ROSAdapterBase[Serializable]):
    """
    Template for dynamic factory-based adaptation of standard ROS primitive messages.

    This class provides the core translation logic for the `std_msgs` family. To avoid manual
    definition of dozens of repetitive classes (e.g., `Int8Adapter`, `StringAdapter`), the ROS
    Bridge employs a **Dynamic Factory Pattern**.

    **Supported ROS Types:**

    - [`std_msgs/msg/String`](https://docs.ros2.org/foxy/api/std_msgs/msg/String.html)
    - [`std_msgs/msg/Int8`](https://docs.ros2.org/foxy/api/std_msgs/msg/Int8.html)
    - [`std_msgs/msg/Int16`](https://docs.ros2.org/foxy/api/std_msgs/msg/Int16.html)
    - [`std_msgs/msg/Int32`](https://docs.ros2.org/foxy/api/std_msgs/msg/Int32.html)
    - [`std_msgs/msg/Int64`](https://docs.ros2.org/foxy/api/std_msgs/msg/Int64.html)
    - [`std_msgs/msg/UInt8`](https://docs.ros2.org/foxy/api/std_msgs/msg/UInt8.html)
    - [`std_msgs/msg/UInt16`](https://docs.ros2.org/foxy/api/std_msgs/msg/UInt16.html)
    - [`std_msgs/msg/UInt32`](https://docs.ros2.org/foxy/api/std_msgs/msg/UInt32.html)
    - [`std_msgs/msg/UInt64`](https://docs.ros2.org/foxy/api/std_msgs/msg/UInt64.html)
    - [`std_msgs/msg/Float32`](https://docs.ros2.org/foxy/api/std_msgs/msg/Float32.html)
    - [`std_msgs/msg/Float64`](https://docs.ros2.org/foxy/api/std_msgs/msg/Float64.html)
    - [`std_msgs/msg/Bool`](https://docs.ros2.org/foxy/api/std_msgs/msg/Bool.html)

    ### Architecture & Dynamic Generation
    At module load time, the SDK iterates through a configuration mapping
    (`_ROS_MSGTYPE_MSCO_BASE_TYPE_MAP`) and programmatically generates concrete
    subclasses of `GenericStdAdapter`.

    Each generated subclass is:

    1.  **Injected** with a specific `ros_msgtype` (e.g., `"std_msgs/msg/String"`).
    2.  **Injected** with a specific target `__mosaico_ontology_type__` (e.g., `String`).
    3.  **Registered** automatically in the [`ROSBridge`][mosaicolabs.ros_bridge.ROSBridge]
        using the `@register_default_adapter` mechanism.

    ### "Adaptation" Strategy
    Following the philosophy of **"Adaptation, Not Just Parsing,"** these adapters do
    not simply extract raw values. They perform:

    - **Schema Enforcement**: Validating that the ROS message contains the mandatory
      `'data'` field.
    - **Strong Typing**: Wrapping the primitive value into a Mosaico [`Serializable`][mosaicolabs.models.core.Serializable]
      object with its own metadata and queryable headers.
    - **Temporal Alignment**: Preserving nanosecond-precise timestamps and optional
      frame information from the source bag file.

    Example:
        ```python
        # Logic effectively generated by the factory:
        class StringStdAdapter(GenericStdAdapter):
            ros_msgtype = "std_msgs/msg/String"
            __mosaico_ontology_type__ = String

        # Usage within the Bridge:
        ros_msg = ROSMessage(
            timestamp=1707760800.123456789,
            topic="/log",
            msg_type="std_msgs/msg/String",
            data={"data": "System OK"}
        )
        mosaico_string = StringStdAdapter.translate(ros_msg)
        ```
    """

    # These attributes are placeholders. They are populated in the dynamic
    # subclasses generated below.
    ros_msgtype: str | Tuple[str, ...]
    __mosaico_ontology_type__: Type[Serializable]
    _REQUIRED_KEYS = ("data",)

    @classmethod
    def translate(cls, ros_msg: ROSMessage, **kwargs: Any) -> Message:
        """
        Translates a standard ROS message to a Mosaico Message.

        Standard messages typically contain a 'data' field and metadata.
        This method extracts the header/timestamp and wraps the payload using
        the specific ontology type defined for this adapter class.

        Args:
            ros_msg (ROSMessage): The ROS message to translate.
            **kwargs: Additional keyword arguments for translation.

        Returns:
            Message: The translated message containing the adapter's ontology type instance.

        Raises:
            Exception: Wraps any translation error with context (topic name, timestamp).
        """
        return super().translate(ros_msg, **kwargs)

    @classmethod
    def from_dict(cls, ros_data: dict) -> Serializable:
        """
        Converts the raw dictionary data into the specific Mosaico type.

        Args:
            ros_data (dict): The raw dictionary from the ROS message.

        Returns:
            Serializable: The constructed Mosaico ontology instance.

        Raises:
            ValueError: If the 'data' key is missing from `ros_data`.
        """
        _validate_msgdata(cls, ros_data)
        return cls.__mosaico_ontology_type__(
            data=ros_data["data"],
        )

    @classmethod
    def to_ros(
        cls,
        mosaico_data: Union[Message, Serializable],
        typestore: Typestore,
        ros_msg_type: Optional[str] = None,
    ) -> MsgType:
        """
        Converts a Mosaico scalar wrapper (or a ``Message`` wrapping one) into the
        corresponding ``std_msgs`` ROS message.

        Args:
            mosaico_data (Union[Message, Serializable]): A ``Message`` wrapping a scalar ``Serializable`` (e.g. ``String``,
                ``Integer32``), or the raw scalar instance directly.
            typestore (Typestore): The rosbags typestore for target type resolution.
            ros_msg_type (Optional[str]): Override for the output ROS type. If ``None``, defaults
                to ``cls.get_default_ros_msg()``.

        Returns:
            MsgType: The constructed ``std_msgs`` ROS message, or raises an error if:

                - the ros_msg_type is unsupported by adapter (TypeError)
                - the ros_msg_type or default type are unsupported by typestore (TypeError)
        """

        # Resolve ROS message to translate Mosaico message to if not defined in input
        resolved_rosmsg_type = ros_msg_type or cls.get_default_ros_msg()
        if not cls.is_rosmsg_type_valid(resolved_rosmsg_type):
            raise TypeError(
                f"Adapter {cls.__name__} does not support {resolved_rosmsg_type}"
            )

        # Checking presence in typestore of requested message
        if typestore.types.get(resolved_rosmsg_type) is None:
            raise TypeError(f"Typestore does not contain {resolved_rosmsg_type}")

        # Unpacking Mosaico message / type
        std_data, _ = cls.unpack_mosaico_msg(mosaico_data)

        # Filling the data (this is the only case where you can actually use a resolved_rosmsg_type)
        # since all the ros data structures contain only 'data' as parameter
        RosStdMsg = typestore.types[resolved_rosmsg_type]

        return RosStdMsg(data=std_data.data)

    @classmethod
    def schema_metadata(
        cls, typestore: Typestore, ros_msg_type: str, ros_version: int
    ) -> Optional[dict]:
        """
        Extract the ROS message specific schema metadata, if any.

        Args:
            typestore (Typestore): The rosbags typestore for target type resolution.
            ros_msg_type (str): The ROS message type to extract metadata for.
            ros_version (int): The ROS version (1 or 2) to consider for metadata extraction.

        Returns:
            Optional[dict]: A dictionary containing the schema metadata, or None if not applicable.
        """
        return super().schema_metadata(typestore, ros_msg_type, ros_version)


# ---------------------------------------------------------------------------
# Dynamic Factory Loop
# ---------------------------------------------------------------------------
# This loop iterates over the mapping configuration and generates a concrete,
# registered adapter class for each supported type.

for ros_type, msco_type in _ROS_MSGTYPE_MSCO_BASE_TYPE_MAP.items():
    # Generate a descriptive class name (e.g., "StringStdAdapter")
    adapter_name = f"{msco_type.__name__}StdAdapter"

    # Define the class attributes that make this adapter unique
    class_attrs = {
        "ros_msgtype": ros_type,
        "__mosaico_ontology_type__": msco_type,
    }

    # Dynamically create the new class
    # - Name: adapter_name
    # - Base: (GenericStdAdapter,)
    # - Attributes: class_attrs
    new_adapter_cls = type(adapter_name, (GenericStdAdapter,), class_attrs)

    # Register the new class with the global adapter registry
    # This makes it available to the ROS Bridge for automatic resolution.
    register_default_adapter(is_default=True)(new_adapter_cls)


@register_default_adapter(is_default=True)
class HeaderAdapter(ROSAdapterBase[Header]):
    """
    Adapter for translating ROS Header messages to Mosaico `Header`.

    **Supported ROS Types:**

    - [`std_msgs/msg/Header`](https://docs.ros2.org/foxy/api/std_msgs/msg/Header.html)

    Example:
        ```python
        # Internal usage within the ROS Bridge
        ros_msg = ROSMessage(
            timestamp=17000,
            topic="/header",
            msg_type="std_msgs/msg/Header",
            data = {
                stamp:
                {
                    "sec": 1000,
                    "nanosec": 1000000000
                },
                frame_id: "base_link"
            }
        )
        # Automatically resolves to a flat Mosaico Header with attached metadata
        mosaico_header = HeaderAdapter.translate(ros_msg)
        ```
    """

    ros_msgtype: str | Tuple[str, ...] = ("std_msgs/msg/Header",)

    __mosaico_ontology_type__: Type[Header] = Header
    _REQUIRED_KEYS = ("stamp", "frame_id")

    @classmethod
    def translate(
        cls,
        ros_msg: ROSMessage,  # ROSMessage
        **kwargs: Any,
    ) -> Message:
        """
        Main entry point for translating a high-level `ROSMessage`.

        Args:
            ros_msg (ROSMessage): The source ROS message yielded by the loader.
            **kwargs: Additional context for the translation.

        Returns:
            Message: The translated Mosaico `Message` containing the normalized `Header` payload.
        """
        return super().translate(ros_msg, **kwargs)

    @classmethod
    def from_dict(cls, ros_data: dict) -> Header:
        """
        Parses a dictionary to extract a `Header` object.

        Example (ROS2 does not have seq field):
            ```python
            ros_data = {
                "stamp": {
                    "sec": 1000,
                    "nanosec": 1000000000
                },
                "frame_id": "base_link"
            }
            # Automatically resolves to a flat Mosaico Header with attached metadata
            mosaico_header = HeaderAdapter.from_dict(ros_data)
            ```

        Args:
            ros_data (dict): The raw dictionary from the ROS message.

        Returns:
            Header: The constructed Mosaico Header object.

        Raises:
            ValueError: If required keys are missing.
        """

        _validate_msgdata(cls, ros_data)
        return Header(
            timestamp=TimeAdapter.from_dict(ros_data["stamp"]),
            frame_id=ros_data.get("frame_id"),
            sample_counter=ros_data.get("seq"),
        )

    @classmethod
    def to_ros(
        cls,
        mosaico_data: Union[Message, Header],
        typestore: Typestore,
        ros_msg_type: Optional[str] = None,
    ) -> MsgType:
        """
        Converts a Mosaico ``Header`` (or a ``Message`` wrapping one) into a
        ``std_msgs/msg/Header`` message.

        Supported output types (selectable via *ros_msg_type*):
        - ``std_msgs/msg/Header``

        Args:
            mosaico_data (Union[Message, Header]): A ``Message`` wrapping a ``Header`` instance, or a raw ``Header``.
            typestore (Typestore): The rosbags typestore for target type resolution.
            ros_msg_type (Optional[str]): Override for the output ROS type. Defaults to
                ``std_msgs/msg/Header`` if ``None``.

        Returns:
            MsgType: A ``std_msgs/msg/Header`` instance, or raises an error if:
                - the ros_msg_type is unsupported by adapter (TypeError)
                - the ros_msg_type or default type are unsupported by typestore (TypeError)
                - the ros_msg_type or default type are supported but translation is not implemented (NotImplementedError)
        """

        # Resolve ROS message to translate Mosaico message to if not defined in input
        resolved_rosmsg_type = ros_msg_type or cls.get_default_ros_msg()
        if not cls.is_rosmsg_type_valid(resolved_rosmsg_type):
            raise TypeError(
                f"Adapter {cls.__name__} does not support {resolved_rosmsg_type}"
            )

        # Checking presence in typestore of requested message
        if typestore.types.get(resolved_rosmsg_type) is None:
            raise TypeError(f"Typestore does not contain {resolved_rosmsg_type}")

        # Unpacking Mosaico message / type
        header_data, _ = cls.unpack_mosaico_msg(mosaico_data)

        # Filling the data
        RosHeader = typestore.types["std_msgs/msg/Header"]

        ros_stamp = header_data.timestamp or Time(seconds=0, nanoseconds=0)
        ros_frame_id = header_data.frame_id or ""

        if not dataclasses.is_dataclass(
            RosHeader
        ):  # Necessary to avoid warning from pylance
            raise TypeError(
                "std_msgs/msg/Header did not return a dataclass from typestore"
            )

        # Handling ROS1 that has seq in Header
        if "seq" in RosHeader.__dataclass_fields__:
            ros_header = RosHeader(
                seq=0,
                stamp=TimeAdapter.to_ros(ros_stamp, typestore),
                frame_id=ros_frame_id,
            )
        else:
            ros_header = RosHeader(
                stamp=TimeAdapter.to_ros(ros_stamp, typestore), frame_id=ros_frame_id
            )

        if resolved_rosmsg_type == "std_msgs/msg/Header":
            return ros_header

        raise NotImplementedError(
            f"The input ros message type {ros_msg_type} is supported but not implemented"
        )
