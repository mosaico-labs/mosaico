import pytest
from rosbags.typesys.stores import Stores, Typestore, get_typestore

from mosaicolabs import (
    Message,
    MotionState,
    Point3d,
    Pose,
    Quaternion,
    Serializable,
    Time,
    Vector3d,
    Velocity,
)
from mosaicolabs.ros_bridge.adapters import (
    OdometryAdapter,
)

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
def point3d():
    return Point3d(
        x=1.0,
        y=2.0,
        z=3.0,
    )


@pytest.fixture
def quaternion():
    return Quaternion(x=0.0, y=0.0, z=0.0, w=1.0)


@pytest.fixture
def pose(point3d, quaternion):
    return Pose(position=point3d, orientation=quaternion)


@pytest.fixture
def velocity(vector3):
    return Velocity(linear=vector3, angular=vector3)


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
def motion_state_msg(motion_state):
    return Message(
        data=motion_state,
        timestamp_ns=100,
        frame_id="base_link",
    )


class TestOdometryAdapter:
    def assert_motion_state(self, motion_state: MotionState, ros_msg):

        assert motion_state.pose.position.x == ros_msg.pose.pose.position.x
        assert motion_state.pose.position.y == ros_msg.pose.pose.position.y
        assert motion_state.pose.position.z == ros_msg.pose.pose.position.z
        assert motion_state.pose.orientation.x == ros_msg.pose.pose.orientation.x
        assert motion_state.pose.orientation.y == ros_msg.pose.pose.orientation.y
        assert motion_state.pose.orientation.z == ros_msg.pose.pose.orientation.z
        assert motion_state.pose.orientation.w == ros_msg.pose.pose.orientation.w

        assert motion_state.velocity.linear.x == ros_msg.twist.twist.linear.x
        assert motion_state.velocity.linear.y == ros_msg.twist.twist.linear.y
        assert motion_state.velocity.linear.z == ros_msg.twist.twist.linear.z
        assert motion_state.velocity.angular.x == ros_msg.twist.twist.angular.x
        assert motion_state.velocity.angular.y == ros_msg.twist.twist.angular.y
        assert motion_state.velocity.angular.z == ros_msg.twist.twist.angular.z

        assert motion_state.target_frame_id == ros_msg.child_frame_id

    @pytest.mark.parametrize("typestore", ROS_TYPESTORE_TO_TEST)
    def test_to_ros_motion_state(self, motion_state: MotionState, typestore: Typestore):
        ros_msg = OdometryAdapter.to_ros(
            motion_state, typestore, "nav_msgs/msg/Odometry"
        )

        self.assert_motion_state(motion_state, ros_msg)

    @pytest.mark.parametrize("typestore", ROS_TYPESTORE_TO_TEST)
    def test_to_ros_odometry_message(
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
        self.assert_motion_state(motion_state, ros_msg)

    @pytest.mark.parametrize("typestore", ROS_TYPESTORE_TO_TEST)
    def test_to_ros_default_type(self, motion_state: MotionState, typestore: Typestore):
        ros_msg = OdometryAdapter.to_ros(motion_state, typestore)

        self.assert_motion_state(motion_state, ros_msg)

    def test_to_ros_invalid_rosmsg_type(self, motion_state: MotionState):
        ros_msg = OdometryAdapter.to_ros(
            motion_state, get_typestore(Stores.LATEST), "nav_msgs/msg/Bogus"
        )

        assert ros_msg is None

    def test_to_ros_invalid_mosaico_type(self, invalid_ms_msg):
        with pytest.raises(TypeError):
            OdometryAdapter.to_ros(invalid_ms_msg, get_typestore(Stores.LATEST))
