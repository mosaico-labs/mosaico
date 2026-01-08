from typing import Any, Optional, Tuple, Type
from mosaicolabs.models import Message

from ..data_ontology.frame_transform import FrameTransform
from ..adapter_base import ROSAdapterBase
from ..ros_message import ROSMessage
from ..ros_bridge import register_adapter

from .geometry_msgs import TransformAdapter
from .helpers import _validate_msgdata


@register_adapter
class FrameTransformAdapter(ROSAdapterBase):
    ros_msgtype: str | Tuple[str, ...] = "tf2_msgs/msg/TFMessage"

    __mosaico_ontology_type__: Type[FrameTransform] = FrameTransform
    _REQUIRED_KEYS = ("transforms",)

    @classmethod
    def translate(
        cls,
        ros_msg: ROSMessage,  # ROSMessage
        **kwargs: Any,
    ) -> Message:
        """
        Translates a ROS message into a Mosaico Message.

        Returns:
            Message: The translated message containing a `FrameTransform` object.

        Raises:
            Exception: Wraps any translation error with context (topic name, timestamp).
        """
        if ros_msg.data is None:
            raise Exception(
                f"'data' attribute in ROSMessage is None. Cannot translate! Ros topic '{ros_msg.topic}' @time: {ros_msg.timestamp}"
            )
        try:
            return Message(
                message_header=ros_msg.header.translate() if ros_msg.header else None,
                timestamp_ns=ros_msg.timestamp,
                data=cls.from_dict(ros_msg.data),
            )
        except Exception as e:
            raise Exception(
                f"Raised Exception while translating ros topic '{ros_msg.topic}' @time: {ros_msg.timestamp}.\nInner err: '{e}'"
            )

    @classmethod
    def from_dict(cls, ros_data: dict) -> FrameTransform:
        """
        Converts the raw dictionary data into the specific Mosaico type.
        """
        _validate_msgdata(cls, ros_data)
        return FrameTransform(
            transforms=[
                TransformAdapter.from_dict(ros_transf_dict)
                for ros_transf_dict in list(ros_data["transforms"])
            ],
        )

    @classmethod
    def schema_metadata(cls, ros_data: dict, **kwargs: Any) -> Optional[dict]:
        """
        Extract the ROS message specific schema metadata, if any.
        """
        return None
