from typing import Any, Optional, Tuple, Type

from mosaicolabs.models import Message
from mosaicolabs.models.sensors import RobotJoint

from ..adapter_base import ROSAdapterBase
from ..ros_bridge import register_adapter
from ..ros_message import ROSMessage

from .helpers import _make_header, _validate_msgdata


@register_adapter
class RobotJointAdapter(ROSAdapterBase[RobotJoint]):
    ros_msgtype: str | Tuple[str, ...] = "sensor_msgs/msg/JointState"
    __mosaico_ontology_type__: Type[RobotJoint] = RobotJoint
    _REQUIRED_KEYS = ("name", "position", "velocity", "effort")

    @classmethod
    def translate(
        cls,
        ros_msg: ROSMessage,  # ROSMessage
        **kwargs: Any,
    ) -> Message:
        """
        Translates a ROS message into a Mosaico Message.

        Returns:
            Message: The translated message containing a `RobotJoint` object.

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
    def from_dict(cls, ros_data: dict) -> RobotJoint:
        """
        Converts the raw dictionary data into the specific Mosaico type.
        """
        _validate_msgdata(cls, ros_data)
        return RobotJoint(
            header=_make_header(ros_data.get("header")),
            names=ros_data["name"],
            positions=ros_data["position"],
            velocities=ros_data["velocity"],
            efforts=ros_data["effort"],
        )

    @classmethod
    def schema_metadata(cls, ros_data: dict, **kwargs: Any) -> Optional[dict]:
        """
        Extract the ROS message specific schema metadata, if any.
        """
        return None
