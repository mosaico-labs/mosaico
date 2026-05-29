from typing import TYPE_CHECKING, Any, Optional, Tuple, Type, Union

from rosbags.typesys.store import Typestore

if TYPE_CHECKING:
    from rosbags.typesys.store import MsgType

from mosaicolabs.models import Message

from ..adapter_base import ROSAdapterBase
from ..data_ontology import FrameTransform
from ..ros_bridge import register_default_adapter
from ..ros_message import ROSMessage
from .geometry_msgs import QuaternionAdapter, TransformAdapter, Vector3Adapter
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
                        "child_frame_id": "base_link"
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
                        "child_frame_id": "base_link"
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
        input_ros_msg_type: Optional[str] = None,
    ) -> "Optional[MsgType]":
        """
        Converts a Mosaico ``FrameTransform`` (or a ``Message`` wrapping one) into a
        ``tf2_msgs/msg/TFMessage``.

        Args:
            mosaico_data: A ``Message`` wrapping a ``FrameTransform``, or a raw ``FrameTransform``.
            typestore: The rosbags typestore for target type resolution.
            input_ros_msg_type: Override for the output ROS type. Only
                ``tf2_msgs/msg/TFMessage`` is supported.

        Returns:
            A ``tf2_msgs/msg/TFMessage`` instance, or ``None`` if the type is
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
        frame_transform_data, ms_header = cls.unpack_mosaico_msg(mosaico_data)

        # Filling the data
        RosTFMessage = typestore.types["tf2_msgs/msg/TFMessage"]
        RosTransform = typestore.types["geometry_msgs/msg/Transform"]
        RosTransformStamped = typestore.types["geometry_msgs/msg/TransformStamped"]

        # TODO: limitation -> each TransformStamped has the same Header since in Mosaico we do not save the header of each Transform?
        tf_transforms = []
        for transform_data in frame_transform_data.transforms:
            header = ms_header.to_ros(typestore)
            header.frame_id = transform_data.source_frame_id

            ros_transform = RosTransform(
                translation=Vector3Adapter.to_ros(
                    transform_data.translation, typestore
                ),
                rotation=QuaternionAdapter.to_ros(transform_data.rotation, typestore),
            )

            ros_transform_stamped = RosTransformStamped(
                header=header,
                child_frame_id=transform_data.target_frame_id,
                transform=ros_transform,
            )

            tf_transforms.append(ros_transform_stamped)

        if resolved_rosmsg_type == "tf2_msgs/msg/TFMessage":
            return RosTFMessage(transforms=tf_transforms)

        return None

    @classmethod
    def schema_metadata(cls, ros_data: dict, **kwargs: Any) -> Optional[dict]:
        """
        Extract the ROS message specific schema metadata, if any.
        """
        return None
