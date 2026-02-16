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

from typing import Any, Dict, Optional, Type, Tuple
from mosaicolabs.models.data import (
    Floating32,
    Floating64,
    Integer64,
    String,
    Integer8,
    Integer16,
    Integer32,
    Boolean,
    Unsigned16,
    Unsigned32,
    Unsigned64,
    Unsigned8,
)
from mosaicolabs.models import Message, Serializable

from ..adapter_base import ROSAdapterBase
from ..ros_message import ROSMessage, ROSHeader
from ..ros_bridge import register_adapter
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
        using the `@register_adapter` mechanism.

    ### "Adaptation" Strategy
    Following the philosophy of **"Adaptation, Not Just Parsing,"** these adapters do
    not simply extract raw values. They perform:

    - **Schema Enforcement**: Validating that the ROS message contains the mandatory
      `'data'` field.
    - **Strong Typing**: Wrapping the primitive value into a Mosaico [`Serializable`][mosaicolabs.models.serializable.Serializable]
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
        """
        return super().translate(ros_msg, **kwargs)

    @classmethod
    def from_dict(cls, ros_data: dict) -> Serializable:
        """
        Converts the raw dictionary data into the specific Mosaico type.
        """
        _validate_msgdata(cls, ros_data)
        return cls.__mosaico_ontology_type__(
            data=ros_data["data"],
        )

    @classmethod
    def schema_metadata(cls, ros_data: dict, **kwargs: Any) -> Optional[dict]:
        """Standard types do not carry additional schema metadata."""
        return None


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
    register_adapter(new_adapter_cls)
