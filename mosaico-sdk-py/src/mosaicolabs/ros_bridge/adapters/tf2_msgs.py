from typing import Any, Optional, Tuple, Type
from mosaicolabs.models import Message

from ..data_ontology import FrameTransform
from ..adapter_base import ROSAdapterBase
from ..ros_message import ROSMessage
from ..ros_bridge import register_adapter

from .geometry_msgs import TransformAdapter
from .helpers import _validate_msgdata


@register_adapter
class FrameTransformAdapter(ROSAdapterBase):
    """
    Adapter for translating ROS TF2 messages to Mosaico `FrameTransform`.

    **Supported ROS Types:**

    - [`tf2_msgs/msg/TFMessage`](https://docs.ros2.org/foxy/api/tf2_msgs/msg/TFMessage.html)

    Example:
        ```python
        ros_msg = ROSMessage(
            timestamp=17000,
            topic="/tf",
            msg_type="tf2_msgs/msg/TFMessage",
            data={
                "transforms": [
                    {
                        "header": {
                            "stamp": {
                                "sec": 17000,
                                "nanosec": 0,
                            },
                            "frame_id": "map",
                            "child_frame_id": "base_link",
                        },
                        "transform": {
                            "translation": {
                                "x": 0.0,
                                "y": 0.0,
                                "z": 0.0,
                            },
                            "rotation": {
                                "x": 0.0,
                                "y": 0.0,
                                "z": 0.0,
                                "w": 1.0,
                            },
                        },
                    }
                ]
            },
        )
        # Automatically resolves to a flat Mosaico FrameTransform with attached metadata
        mosaico_frame_transform = FrameTransformAdapter.translate(ros_msg)
        ```
    """

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
        return super().translate(ros_msg, **kwargs)

    @classmethod
    def from_dict(cls, ros_data: dict) -> FrameTransform:
        """
        Converts the raw dictionary data into the specific Mosaico type.

        Example:
            ```python
            ros_data={
                "transforms": [
                    {
                        "header": {
                            "stamp": {
                                "sec": 17000,
                                "nanosec": 0,
                            },
                            "frame_id": "map",
                            "child_frame_id": "base_link",
                        },
                        "transform": {
                            "translation": {
                                "x": 0.0,
                                "y": 0.0,
                                "z": 0.0,
                            },
                            "rotation": {
                                "x": 0.0,
                                "y": 0.0,
                                "z": 0.0,
                                "w": 1.0,
                            },
                        },
                    }
                ]
            }
            # Automatically resolves to a flat Mosaico FrameTransform with attached metadata
            mosaico_frame_transform = FrameTransformAdapter.from_dict(ros_data)
            ```
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
