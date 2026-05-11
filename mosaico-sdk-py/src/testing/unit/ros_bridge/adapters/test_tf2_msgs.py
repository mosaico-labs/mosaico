import pytest
from rosbags.typesys import get_types_from_msg
from rosbags.typesys.stores import Stores, Typestore, get_typestore

from mosaicolabs import (
    Message,
    Quaternion,
    Serializable,
    Transform,
    Vector3d,
)
from mosaicolabs.ros_bridge.adapters import (
    FrameTransformAdapter,
)
from mosaicolabs.ros_bridge.data_ontology import FrameTransform

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
def invalid_ms_msg() -> Message:
    return Message(
        data=Serializable(),
        timestamp_ns=0,
    )


@pytest.fixture
def vector3():
    return Vector3d(
        x=1.0,
        y=2.0,
        z=3.0,
    )


@pytest.fixture
def quaternion():
    return Quaternion(x=0.0, y=0.0, z=0.0, w=1.0)


@pytest.fixture
def transform(vector3, quaternion):
    return Transform(translation=vector3, rotation=quaternion)


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
def frame_transform(transform):
    return FrameTransform(transforms=[transform, transform, transform])


@pytest.fixture
def frame_transform_msg(frame_transform):
    return Message(
        data=frame_transform,
        timestamp_ns=100,
        frame_id="base_link",
    )


class TestFrameTransformAdapter:
    def assert_frame_transform(self, frame_trasform: FrameTransform, ros_msg):

        for mosaico_transform, ros_transform in zip(
            frame_trasform.transforms, ros_msg.transforms
        ):
            # assert mosaico_transform.source_frame_id == ros_transform.header.frame_id # TODO
            assert mosaico_transform.target_frame_id == ros_transform.child_frame_id
            assert (
                mosaico_transform.translation.x == ros_transform.transform.translation.x
            )
            assert (
                mosaico_transform.translation.y == ros_transform.transform.translation.y
            )
            assert (
                mosaico_transform.translation.z == ros_transform.transform.translation.z
            )
            assert mosaico_transform.rotation.x == ros_transform.transform.rotation.x
            assert mosaico_transform.rotation.y == ros_transform.transform.rotation.y
            assert mosaico_transform.rotation.z == ros_transform.transform.rotation.z
            assert mosaico_transform.rotation.w == ros_transform.transform.rotation.w

    @pytest.mark.parametrize("typestore", ROS_TYPESTORE_TO_TEST)
    def test_to_ros_frame_transform(
        self, frame_transform: FrameTransform, typestore: Typestore
    ):
        typestore = register_tf2_messages(typestore)
        ros_msg = FrameTransformAdapter.to_ros(
            frame_transform, typestore, "tf2_msgs/msg/TFMessage"
        )

        self.assert_frame_transform(frame_transform, ros_msg)

    @pytest.mark.parametrize("typestore", ROS_TYPESTORE_TO_TEST)
    def test_to_ros_frame_transform_message(
        self, frame_transform_msg: Message, typestore: Typestore
    ):
        typestore = register_tf2_messages(typestore)
        frame_transform = frame_transform_msg.get_data(FrameTransform)
        ros_msg = FrameTransformAdapter.to_ros(
            frame_transform_msg, typestore, "tf2_msgs/msg/TFMessage"
        )

        self.assert_frame_transform(frame_transform, ros_msg)

    @pytest.mark.parametrize("typestore", ROS_TYPESTORE_TO_TEST)
    def test_to_ros_default_type(
        self, frame_transform: FrameTransform, typestore: Typestore
    ):
        typestore = register_tf2_messages(typestore)
        ros_msg = FrameTransformAdapter.to_ros(frame_transform, typestore)

        self.assert_frame_transform(frame_transform, ros_msg)

    def test_to_ros_invalid_rosmsg_type(self, frame_transform: FrameTransform):
        ros_msg = FrameTransformAdapter.to_ros(
            frame_transform, get_typestore(Stores.LATEST), "tf2_msgs/msg/Bogus"
        )

        assert ros_msg is None

    def test_to_ros_invalid_mosaico_type(self, invalid_ms_msg):
        with pytest.raises(TypeError):
            FrameTransformAdapter.to_ros(invalid_ms_msg, get_typestore(Stores.LATEST))
