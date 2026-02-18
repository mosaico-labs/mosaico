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
