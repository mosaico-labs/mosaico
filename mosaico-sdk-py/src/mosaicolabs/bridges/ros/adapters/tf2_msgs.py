from __future__ import annotations

from typing import TYPE_CHECKING, Any, Optional, Tuple, Type, Union

from rosbags.typesys.store import Typestore

if TYPE_CHECKING:
    from rosbags.typesys.store import MsgType

from mosaicolabs.models.core import Message

from ..adapter_base import ROSAdapterBase
from ..bridge import register_default_adapter
from ..data_ontology import FrameTransform
from ..ros_message import ROSMessage
from .geometry_msgs import TransformAdapter
from .helpers import _validate_msgdata


@register_default_adapter(is_default=True)
class FrameTransformAdapter(ROSAdapterBase[FrameTransform]):
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
                        },
                        "child_frame_id": "base_link",
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
                        },
                        "child_frame_id": "base_link",
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
    def to_ros(
        cls,
        mosaico_data: Union[Message, FrameTransform],
        typestore: Typestore,
        ros_msg_type: Optional[str] = None,
    ) -> MsgType:
        """
        Converts a Mosaico ``FrameTransform`` (or a ``Message`` wrapping one) into a
        ``tf2_msgs/msg/TFMessage``.

        Args:
            mosaico_data (Union[Message, FrameTransform]): A ``Message`` wrapping a ``FrameTransform``, or a raw ``FrameTransform``.
            typestore (Typestore): The rosbags typestore for target type resolution.
            ros_msg_type (Optional[str]): Override for the output ROS type. Only
                ``tf2_msgs/msg/TFMessage`` is supported.

        Returns:
            MsgType: The constructed ``tf2_msgs/msg/TFMessage``, or raises an error if:

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
        frame_transform_data, _ = cls.unpack_mosaico_msg(mosaico_data)

        # Filling the data
        RosTFMessage = typestore.types["tf2_msgs/msg/TFMessage"]

        tf_transforms: Any = [
            TransformAdapter.to_ros(
                transform, typestore, "geometry_msgs/msg/TransformStamped"
            )
            for transform in frame_transform_data.transforms
        ]

        if resolved_rosmsg_type == "tf2_msgs/msg/TFMessage":
            return RosTFMessage(transforms=tf_transforms)

        raise NotImplementedError(
            f"The input ros message type {ros_msg_type} is supported but not implemented"
        )

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
