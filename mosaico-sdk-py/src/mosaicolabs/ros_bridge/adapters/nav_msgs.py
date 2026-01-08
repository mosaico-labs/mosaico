from typing import Any, Optional, Tuple, Type

from mosaicolabs.models.data import MotionState
from mosaicolabs.models import Message

from .geometry_msgs import PoseAdapter, TwistAdapter
from ..adapter_base import ROSAdapterBase
from ..ros_message import ROSMessage
from ..ros_bridge import register_adapter

from .helpers import _make_header, _validate_msgdata


@register_adapter
class OdometryAdapter(ROSAdapterBase[MotionState]):
    ros_msgtype: str | Tuple[str, ...] = "nav_msgs/msg/Odometry"

    __mosaico_ontology_type__: Type[MotionState] = MotionState
    _REQUIRED_KEYS = ("pose", "twist", "child_frame_id")

    @classmethod
    def translate(
        cls,
        ros_msg: ROSMessage,  # ROSMessage
        **kwargs: Any,
    ) -> Message:
        """
        Translates a ROS message into a Mosaico Message.

        Returns:
            Message: The translated message containing a `MotionState` object.

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
    def from_dict(cls, ros_data: dict) -> MotionState:
        _validate_msgdata(cls, ros_data)
        return MotionState(
            header=_make_header(ros_data.get("header")),
            target_frame_id=ros_data["child_frame_id"],
            pose=PoseAdapter.from_dict(ros_data["pose"]),
            velocity=TwistAdapter.from_dict(ros_data["twist"]),
        )

    @classmethod
    def schema_metadata(cls, ros_data: dict, **kwargs: Any) -> Optional[dict]:
        """
        Extract the ROS message specific schema metadata, if any.
        """
        return None
