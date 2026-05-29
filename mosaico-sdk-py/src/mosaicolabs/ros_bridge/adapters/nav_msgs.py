from typing import TYPE_CHECKING, Any, Optional, Tuple, Type, Union

from rosbags.typesys.store import Typestore

if TYPE_CHECKING:
    from rosbags.typesys.store import MsgType

from mosaicolabs.models import Message
from mosaicolabs.models.data import MotionState

from ..adapter_base import ROSAdapterBase
from ..ros_bridge import register_default_adapter
from ..ros_message import ROSMessage
from .geometry_msgs import PoseAdapter, TwistAdapter
from .helpers import _validate_msgdata


@register_default_adapter(is_default=True)
class OdometryAdapter(ROSAdapterBase[MotionState]):
    """
    Adapter for translating ROS Odometry messages to Mosaico `MotionState`.

    **Supported ROS Types:**

    - [`nav_msgs/msg/Odometry`](https://docs.ros2.org/foxy/api/nav_msgs/msg/Odometry.html)

    Example:
        ```python
        ros_msg = ROSMessage(
            timestamp=17000,
            topic="/odometry",
            msg_type="nav_msgs/msg/Odometry",
            data=
            {
                "header": {"frame_id": "map", "stamp": {"sec": 17000, "nanosec": 0}},
                "pose": {
                    "position": {"x": 1.0, "y": 2.0, "z": 0.0},
                    "orientation": {"x": 0, "y": 0, "z": 0, "w": 1}
                },
                "twist": {
                    "linear": {"x": 0.0, "y": 0.0, "z": 0.0},
                    "angular": {"x": 0.0, "y": 0.0, "z": 0.0}
                },
                "child_frame_id": "base_link"
            }
        )
        # Automatically resolves to a flat Mosaico MotionState with attached metadata
        mosaico_odometry = OdometryAdapter.translate(ros_msg)
        ```
    """

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
        return super().translate(ros_msg, **kwargs)

    @classmethod
    def from_dict(cls, ros_data: dict) -> MotionState:
        """
        Parses a dictionary to extract a `MotionState` object.

        Example:
            ```python
            ros_data=
            {
                "header": {"frame_id": "map", "stamp": {"sec": 17000, "nanosec": 0}},
                "pose": {
                    "position": {"x": 1.0, "y": 2.0, "z": 0.0},
                    "orientation": {"x": 0, "y": 0, "z": 0, "w": 1}
                },
                "twist": {
                    "linear": {"x": 0.0, "y": 0.0, "z": 0.0},
                    "angular": {"x": 0.0, "y": 0.0, "z": 0.0}
                },
                "child_frame_id": "base_link"
            }
            # Automatically resolves to a flat Mosaico MotionState with attached metadata
            mosaico_odometry = OdometryAdapter.from_dict(ros_data)
            ```

        Args:
            ros_data (dict): The raw dictionary from the ROS message.

        Returns:
            MotionState: The constructed Mosaico MotionState object.

        Raises:
            ValueError: If the recursive 'pose' key exists but is not a dict, or if required keys are missing.
        """
        _validate_msgdata(cls, ros_data)
        return MotionState(
            target_frame_id=ros_data["child_frame_id"],
            pose=PoseAdapter.from_dict(ros_data["pose"]),
            velocity=TwistAdapter.from_dict(ros_data["twist"]),
        )

    @classmethod
    def to_ros(
        cls,
        mosaico_data: Union[Message, MotionState],
        typestore: Typestore,
        input_ros_msg_type: Optional[str] = None,
    ) -> "Optional[MsgType]":
        """
        Converts a Mosaico ``MotionState`` (or a ``Message`` wrapping one) into a
        ``nav_msgs/msg/Odometry`` message.

        Args:
            mosaico_data: A ``Message`` wrapping a ``MotionState`` instance, or a raw ``MotionState``.
            typestore: The rosbags typestore for target type resolution.
            input_ros_msg_type: Override for the output ROS type. Only
                ``nav_msgs/msg/Odometry`` is supported.

        Returns:
            A ``nav_msgs/msg/Odometry`` instance, or ``None`` if the type is
            unsupported or absent from the typestore.
        """

        # Resolve ROS message to translate Mosaico message to if not defined in input
        resolved_rosmsg_type = input_ros_msg_type or cls.get_default_ros_msg()
        if not cls.is_rosmsg_type_valid(resolved_rosmsg_type):
            return None

        # Checking presence in typestore of requested message
        if typestore.types.get(resolved_rosmsg_type) is None:
            return None

        # Unpacking Mosaico message / type
        motion_data, header_ms = cls.unpack_mosaico_msg(mosaico_data)

        # Filling the data
        RosOdometry = typestore.types["nav_msgs/msg/Odometry"]

        if resolved_rosmsg_type == "nav_msgs/msg/Odometry":
            return RosOdometry(
                header=header_ms.to_ros(typestore),
                child_frame_id=motion_data.target_frame_id,
                pose=PoseAdapter.to_ros(
                    motion_data.pose, typestore, "geometry_msgs/msg/PoseWithCovariance"
                ),
                twist=TwistAdapter.to_ros(
                    motion_data.velocity,
                    typestore,
                    "geometry_msgs/msg/TwistWithCovariance",
                ),
            )

        return None

    @classmethod
    def schema_metadata(cls, ros_data: dict, **kwargs: Any) -> Optional[dict]:
        """
        Extract the ROS message specific schema metadata, if any.
        """
        return None
