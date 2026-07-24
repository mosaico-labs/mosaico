from dataclasses import asdict

import pytest
from rosbags.typesys import get_types_from_msg
from rosbags.typesys.store import Typestore
from rosbags.typesys.stores import Stores, get_typestore

from mosaicolabs import (
    Message,
    Transform,
)
from mosaicolabs.ros_bridge.adapters import (
    FrameTransformAdapter,
)
from mosaicolabs.ros_bridge.data_ontology import FrameTransform
from mosaicolabs.ros_bridge.ros_message import ROSMessage
from testing.unit.ros_bridge.adapters.helper import assert_frame_transform

ROS_TYPESTORE_TO_TEST = [
    get_typestore(Stores.LATEST),
    get_typestore(Stores.ROS1_NOETIC),
    get_typestore(Stores.ROS2_DASHING),
    get_typestore(Stores.ROS2_ELOQUENT),
    get_typestore(Stores.ROS2_FOXY),
    get_typestore(Stores.ROS2_GALACTIC),
    get_typestore(Stores.ROS2_HUMBLE),
    get_typestore(Stores.ROS2_IRON),
    get_typestore(Stores.ROS2_JAZZY),
    get_typestore(Stores.ROS2_KILTED),
]


@pytest.fixture
def transform(vector3d, quaternion):
    return Transform(translation=vector3d, rotation=quaternion)


###############################################################################
########################## TestFrameTransformAdapter ##########################
###############################################################################


def register_tf2_messages(typestore: Typestore):

    # Check whether tf2_messages exists in typestore (False for ROS1, True for ROS2)
    if typestore.fielddefs.get("tf2_msgs/msg/TFMessage") is not None:
        return typestore

    # tf2_msgs/msg/TFMessage is not between default messages for ROS1
    # See this: https://gitlab.com/ternaris/rosbags/-/work_items/122
    add_types = get_types_from_msg(
        "geometry_msgs/TransformStamped[] transforms\n",
        "tf2_msgs/msg/TFMessage",
    )
    typestore.register(add_types)
    return typestore


@pytest.fixture
def frame_transform(transform: Transform):
    return FrameTransform(transforms=[transform, transform, transform])


@pytest.fixture
def frame_transform_rosmsg(ros_header, transform: Transform):
    return ROSMessage(
        bag_timestamp_ns=100,
        topic="/tf",
        msg_type="tf2_msgs/msg/TFMessage",
        data={
            "transforms": [
                {
                    "header": ros_header,
                    "child_frame_id": "base_link",
                    "transform": {
                        "rotation": {
                            "x": transform.rotation.x,
                            "y": transform.rotation.y,
                            "z": transform.rotation.z,
                            "w": transform.rotation.w,
                        },
                        "translation": {
                            "x": transform.translation.x,
                            "y": transform.translation.y,
                            "z": transform.translation.z,
                        },
                    },
                },
                {
                    "header": ros_header,
                    "child_frame_id": "camera",
                    "transform": {
                        "rotation": {
                            "x": transform.rotation.x,
                            "y": transform.rotation.y,
                            "z": transform.rotation.z,
                            "w": transform.rotation.w,
                        },
                        "translation": {
                            "x": transform.translation.x,
                            "y": transform.translation.y,
                            "z": transform.translation.z,
                        },
                    },
                },
                {
                    "header": ros_header,
                    "child_frame_id": "end_effector",
                    "transform": {
                        "rotation": {
                            "x": transform.rotation.x,
                            "y": transform.rotation.y,
                            "z": transform.rotation.z,
                            "w": transform.rotation.w,
                        },
                        "translation": {
                            "x": transform.translation.x,
                            "y": transform.translation.y,
                            "z": transform.translation.z,
                        },
                    },
                },
            ],
        },
    )


@pytest.fixture
def frame_transform_msg(frame_transform):
    return Message(
        data=frame_transform,
        timestamp_ns=100,
    )


class TestFrameTransformAdapter:
    def test_translate_nav_sat_fix(self, frame_transform_rosmsg: ROSMessage):
        ms_msg = FrameTransformAdapter.translate(frame_transform_rosmsg)

        assert_frame_transform(
            ms_msg.get_data(FrameTransform), frame_transform_rosmsg.data_field
        )

    def test_translate_raise_missing_required_key(
        self, frame_transform_rosmsg: ROSMessage
    ):
        data = frame_transform_rosmsg.data_field
        data.pop("transforms")
        with pytest.raises(ValueError, match="missing required keys"):
            FrameTransformAdapter.from_dict(data)

    @pytest.mark.parametrize("typestore", ROS_TYPESTORE_TO_TEST)
    def test_to_ros_frame_transform(
        self, frame_transform: FrameTransform, typestore: Typestore
    ):
        typestore = register_tf2_messages(typestore)
        ros_msg = FrameTransformAdapter.to_ros(
            frame_transform, typestore, "tf2_msgs/msg/TFMessage"
        )

        assert_frame_transform(frame_transform, asdict(ros_msg))

    @pytest.mark.parametrize("typestore", ROS_TYPESTORE_TO_TEST)
    def test_to_ros_frame_transform_message(
        self, frame_transform_msg: Message, typestore: Typestore
    ):
        typestore = register_tf2_messages(typestore)
        frame_transform = frame_transform_msg.get_data(FrameTransform)
        ros_msg = FrameTransformAdapter.to_ros(
            frame_transform_msg, typestore, "tf2_msgs/msg/TFMessage"
        )

        assert_frame_transform(frame_transform, asdict(ros_msg))

    @pytest.mark.parametrize("typestore", ROS_TYPESTORE_TO_TEST)
    def test_to_ros_default_type(
        self, frame_transform: FrameTransform, typestore: Typestore
    ):
        typestore = register_tf2_messages(typestore)
        ros_msg = FrameTransformAdapter.to_ros(frame_transform, typestore)

        assert_frame_transform(frame_transform, asdict(ros_msg))

    def test_to_ros_invalid_rosmsg_type(self, frame_transform: FrameTransform):

        with pytest.raises(
            TypeError,
            match=f"Adapter {FrameTransformAdapter.__name__} does not support tf2_msgs/msg/Bogus",
        ):
            FrameTransformAdapter.to_ros(
                frame_transform, get_typestore(Stores.LATEST), "tf2_msgs/msg/Bogus"
            )

    def test_to_ros_invalid_mosaico_type(self, invalid_ms_msg):
        with pytest.raises(TypeError):
            FrameTransformAdapter.to_ros(invalid_ms_msg, get_typestore(Stores.LATEST))
