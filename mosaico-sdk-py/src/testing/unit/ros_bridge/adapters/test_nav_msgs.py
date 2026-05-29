from dataclasses import asdict

import pytest
from rosbags.typesys.stores import Stores, Typestore, get_typestore

from mosaicolabs import (
    Message,
    MotionState,
    Pose,
    Time,
    Velocity,
)
from mosaicolabs.ros_bridge.adapters import (
    OdometryAdapter,
)
from mosaicolabs.ros_bridge.ros_message import ROSMessage
from testing.unit.ros_bridge.adapters.helper import assert_motion_state

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
def pose(point3d, quaternion):
    return Pose(position=point3d, orientation=quaternion)


@pytest.fixture
def velocity(vector3d):
    return Velocity(linear=vector3d, angular=vector3d)


###############################################################################
############################# TestOdometryAdapter #############################
###############################################################################


@pytest.fixture
def motion_state(pose, velocity):
    return MotionState(
        pose=pose,
        velocity=velocity,
        target_frame_id="base_link",
    )


@pytest.fixture
def motion_state_rosmsg(ros_header, motion_state: MotionState):
    return ROSMessage(
        bag_timestamp_ns=100,
        topic="/odometry",
        msg_type="nav_msgs/msg/Odometry",
        data={
            "header": ros_header,
            "child_frame_id": motion_state.target_frame_id,
            "pose": {
                "pose": motion_state.pose.model_dump(
                    exclude={"covariance", "covariance_type"}
                ),
                "covariance": [0.0] * 36,
            },
            "twist": {
                "twist": motion_state.velocity.model_dump(
                    exclude={"covariance", "covariance_type"}
                ),
                "covariance": [0.0] * 36,
            },
        },
    )


@pytest.fixture
def motion_state_msg(motion_state):
    return Message(
        data=motion_state,
        timestamp_ns=100,
        frame_id="base_link",
    )


class TestOdometryAdapter:
    def test_translate_motion_state(self, motion_state_rosmsg: ROSMessage):
        ms_msg = OdometryAdapter.translate(motion_state_rosmsg)

        assert ms_msg.timestamp_ns == motion_state_rosmsg.header.stamp.to_nanoseconds()
        assert_motion_state(ms_msg.get_data(MotionState), motion_state_rosmsg.data)

    def test_translate_raise_missing_required_key(
        self, motion_state_rosmsg: ROSMessage
    ):
        data = motion_state_rosmsg.data
        data.pop("child_frame_id")
        with pytest.raises(ValueError):
            OdometryAdapter.from_dict(data)

    @pytest.mark.parametrize("typestore", ROS_TYPESTORE_TO_TEST)
    def test_to_ros_motion_state(self, motion_state: MotionState, typestore: Typestore):
        ros_msg = OdometryAdapter.to_ros(
            motion_state, typestore, "nav_msgs/msg/Odometry"
        )

        assert_motion_state(motion_state, asdict(ros_msg))

    @pytest.mark.parametrize("typestore", ROS_TYPESTORE_TO_TEST)
    def test_to_ros_motion_state_message(
        self, motion_state_msg: Message, typestore: Typestore
    ):
        motion_state = motion_state_msg.get_data(MotionState)
        ros_msg = OdometryAdapter.to_ros(
            motion_state_msg, typestore, "nav_msgs/msg/Odometry"
        )

        assert (
            motion_state_msg.timestamp_ns
            == Time(
                seconds=ros_msg.header.stamp.sec,
                nanoseconds=ros_msg.header.stamp.nanosec,
            ).to_nanoseconds()
        )
        assert motion_state_msg.frame_id == ros_msg.header.frame_id
        assert_motion_state(motion_state, asdict(ros_msg))

    @pytest.mark.parametrize("typestore", ROS_TYPESTORE_TO_TEST)
    def test_to_ros_default_type(self, motion_state: MotionState, typestore: Typestore):
        ros_msg = OdometryAdapter.to_ros(motion_state, typestore)

        assert_motion_state(motion_state, asdict(ros_msg))

    def test_to_ros_invalid_rosmsg_type(self, motion_state: MotionState):
        ros_msg = OdometryAdapter.to_ros(
            motion_state, get_typestore(Stores.LATEST), "nav_msgs/msg/Bogus"
        )

        assert ros_msg is None

    def test_to_ros_invalid_mosaico_type(self, invalid_ms_msg):
        with pytest.raises(TypeError):
            OdometryAdapter.to_ros(invalid_ms_msg, get_typestore(Stores.LATEST))
