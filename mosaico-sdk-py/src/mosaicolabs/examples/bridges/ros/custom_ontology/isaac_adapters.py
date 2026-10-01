from __future__ import annotations

from typing import TYPE_CHECKING, Any, Optional, Tuple, Type, Union

from rosbags.typesys.store import Typestore

from mosaicolabs import Message
from mosaicolabs.bridges.ros import (
    ROSAdapterBase,
    ROSMessage,
    register_default_adapter,
)
from mosaicolabs.bridges.ros.adapters import HeaderAdapter
from mosaicolabs.bridges.ros.adapters.helpers import _validate_msgdata

from .isaac import EncoderTicks

if TYPE_CHECKING:
    from rosbags.typesys.store import MsgType


@register_default_adapter(is_default=True)
class EncoderTicksAdapter(ROSAdapterBase[EncoderTicks]):
    """
    Adapter for translating NVIDIA Isaac EncoderTicks messages to Mosaico.

    This adapter bridges the `isaac_ros_nova_interfaces` ROS package into the
    custom Mosaico `EncoderTicks` model, handling hardware-to-platform mapping.

    **Supported ROS Type:**
    - `isaac_ros_nova_interfaces/msg/EncoderTicks`
    """

    ros_msgtype: str | Tuple[str, ...] = ("isaac_ros_nova_interfaces/msg/EncoderTicks",)
    __mosaico_ontology_type__: Type[EncoderTicks] = EncoderTicks

    # Validation keys used by _validate_msgdata
    _REQUIRED_KEYS = ("left_ticks", "right_ticks", "encoder_timestamp")

    @classmethod
    def translate(cls, ros_msg: ROSMessage, **kwargs: Any) -> Message:
        """
        Translates a ROS EncoderTicks message into a Mosaico Message container.

        Args:
            ros_msg (ROSMessage): The raw container provided by the ROSLoader.
            **kwargs: Additional translation context.

        Returns:
            Message: A Mosaico Message containing the translated EncoderTicks data.
        """
        return super().translate(ros_msg, **kwargs)

    @classmethod
    def from_dict(cls, ros_data: dict) -> EncoderTicks:
        """
        Maps the raw ROS dictionary to the EncoderTicks Pydantic model.

        This method performs field validation and type reconstruction.
        """
        _validate_msgdata(cls, ros_data)
        return EncoderTicks(
            left_ticks=ros_data["left_ticks"],
            right_ticks=ros_data["right_ticks"],
            encoder_timestamp=ros_data["encoder_timestamp"],
        )

    @classmethod
    def to_ros(
        cls,
        mosaico_data: Union[Message, EncoderTicks],
        typestore: Typestore,
        ros_msg_type: Optional[str] = None,
    ) -> MsgType:
        """
        Converts a Mosaico ``EncoderTicks`` (or a ``Message`` wrapping one) into the
        corresponding ROS EncoderTicks message.

        Supported output types (selectable via *ros_msg_type*):

        - ``isaac_ros_nova_interfaces/msg/EncoderTicks``

        Args:
            mosaico_data (Union[Message, EncoderTicks]): A ``Message`` wrapping a ``EncoderTicks`` instance, or a raw ``EncoderTicks``.
            typestore (Typestore): The rosbags typestore for target type resolution.
            ros_msg_type (Optional[str]): Override for the output ROS type. Defaults to
                ``isaac_ros_nova_interfaces/msg/EncoderTicks`` if ``None``.

        Returns:
            MsgType: The constructed ROS message, or raises an error if:

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
        encoder_ticks_data, ms_header = cls.unpack_mosaico_msg(mosaico_data)

        # Filling the data
        RosEncoderTick = typestore.types["isaac_ros_nova_interfaces/msg/EncoderTicks"]

        encoder_tick = RosEncoderTick(
            header=HeaderAdapter.to_ros(ms_header, typestore),
            left_ticks=encoder_ticks_data.left_ticks,
            right_ticks=encoder_ticks_data.right_ticks,
            encoder_timestamp=encoder_ticks_data.encoder_timestamp,
        )

        if resolved_rosmsg_type == "isaac_ros_nova_interfaces/msg/EncoderTicks":
            return encoder_tick

        raise NotImplementedError(
            f"The input ros message type {ros_msg_type} is supported but not implemented"
        )
