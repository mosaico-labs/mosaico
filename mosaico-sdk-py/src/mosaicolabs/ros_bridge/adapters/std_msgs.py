"""
Standard ROS Message Adapters.

This module provides adapters for translating standard ROS messages (std_msgs)
into Mosaico ontology types. Instead of manually defining a class for every
single primitive type (Int8, String, Bool, etc.), we use a dynamic factory pattern.

Architecture:
 -  `_ROS_MSGTYPE_MSCO_BASE_TYPE_MAP` defines the relationship between a ROS
    message type string (e.g., "std_msgs/msg/String") and the corresponding
    Mosaico Serializable class (e.g., `String`).
 -  `_GenericStdAdapter` implements the common `translate` and `from_dict` logics
    shared by all standard types (wrapping the 'data' field).
 -  At module load time, we iterate through the mapping, dynamically create
    a unique subclass of `_GenericStdAdapter` for each type and register it
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


class _GenericStdAdapter(ROSAdapterBase[Serializable]):
    """
    Base implementation for standard ROS message adapters.

    This class serves as a template. It is NOT registered directly.
    Instead, dynamic subclasses are created from this template, with specific
    `ros_msgtype` and `__mosaico_ontology_type__` attributes injected.
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
        # Extract optional ROS header if present (std_msgs usually don't have one,
        # but custom variants might).
        header: Optional[ROSHeader] = getattr(ros_msg, "header", None)
        message_header = header.translate() if header else None

        # Optimization: We directly use `cls.from_dict` which uses the
        # class-bound `__mosaico_ontology_type__`. No runtime lookup is needed.
        if ros_msg.data is None:
            raise Exception(
                f"'data' attribute in ROSMessage is None. Cannot translate! Ros topic '{ros_msg.topic}' @time: {ros_msg.timestamp}"
            )
        try:
            return Message(
                message_header=message_header,
                timestamp_ns=getattr(ros_msg, "timestamp"),
                data=cls.from_dict(getattr(ros_msg, "data")),
            )
        except Exception as e:
            raise Exception(
                f"Raised Exception while translating ros topic '{ros_msg.topic}' @time: {ros_msg.timestamp}.\nInner err: '{e}'"
            )

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
    # - Base: (_GenericStdAdapter,)
    # - Attributes: class_attrs
    new_adapter_cls = type(adapter_name, (_GenericStdAdapter,), class_attrs)

    # Register the new class with the global adapter registry
    # This makes it available to the ROS Bridge for automatic resolution.
    register_adapter(new_adapter_cls)
