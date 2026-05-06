import numpy as np
import pytest
from rosbags.typesys.stores import Stores, Typestore, get_typestore

from mosaicolabs import Message, Point3d, Pose, Quaternion, Serializable, Time
from mosaicolabs.ros_bridge.adapters import PointAdapter, PoseAdapter, QuaternionAdapter

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


###############################################################################
############################### QuaternionAdapter ##############################
###############################################################################


@pytest.fixture
def quaternion():
    return Quaternion(x=0.0, y=0.0, z=0.0, w=1.0)


@pytest.fixture
def quat_msg(quaternion):
    return Message(
        data=quaternion,
        timestamp_ns=100,
        frame_id="base_link",
    )


class TestQuaternionAdapter:
    # def test_translate_quaternion(self): ...  # TODO
    # def test_translate_quaternion_stamped(self): ...  # TODO
    # def test_translate_raise_quaternion_not_dict(self): ...  # TODO
    # def test_translate_raise_missing_required_key(self): ...  # TODO

    @pytest.mark.parametrize("typestore", ROS_TYPESTORE_TO_TEST)
    def test_to_ros_quaternion(self, quaternion: Quaternion, typestore: Typestore):
        result = QuaternionAdapter.to_ros(
            quaternion, typestore, "geometry_msgs/msg/Quaternion"
        )

        assert quaternion.x == result.x
        assert quaternion.y == result.y
        assert quaternion.z == result.z
        assert quaternion.w == result.w

    @pytest.mark.parametrize("typestore", ROS_TYPESTORE_TO_TEST)
    def test_to_ros_quaternion_stamped(self, quat_msg: Message, typestore: Typestore):
        quaternion = quat_msg.get_data(Quaternion)
        result = QuaternionAdapter.to_ros(
            quat_msg, typestore, "geometry_msgs/msg/QuaternionStamped"
        )

        assert (
            quat_msg.timestamp_ns
            == Time(
                seconds=result.header.stamp.sec, nanoseconds=result.header.stamp.nanosec
            ).to_nanoseconds()
        )
        assert quat_msg.frame_id == result.header.frame_id
        assert quaternion.x == result.quaternion.x
        assert quaternion.y == result.quaternion.y
        assert quaternion.z == result.quaternion.z
        assert quaternion.w == result.quaternion.w

    @pytest.mark.parametrize("typestore", ROS_TYPESTORE_TO_TEST)
    def test_to_ros_default_type(self, quaternion: Quaternion, typestore: Typestore):
        result = QuaternionAdapter.to_ros(quaternion, typestore)

        assert quaternion.x == result.x
        assert quaternion.y == result.y
        assert quaternion.z == result.z
        assert quaternion.w == result.w

    def test_to_ros_invalid_rosmsg_type(self, quaternion: Quaternion):
        result = QuaternionAdapter.to_ros(
            quaternion, get_typestore(Stores.LATEST), "geometry_msgs/msg/Bogus"
        )

        assert result is None

    def test_to_ros_invalid_mosaico_type(self, invalid_ms_msg):
        with pytest.raises(TypeError):
            QuaternionAdapter.to_ros(invalid_ms_msg, get_typestore(Stores.LATEST))


###############################################################################
############################### TestPointAdapter ##############################
###############################################################################


@pytest.fixture
def point():
    return Point3d(x=1.0, y=2.0, z=3.0)


@pytest.fixture
def point_msg(point):
    return Message(
        data=point,
        timestamp_ns=100,
        frame_id="base_link",
    )


class TestPointadapter:
    # def test_translate_point(self): ...  # TODO
    # def test_translate_point_stamped(self): ...  # TODO
    # def test_translate_raise_point_not_dict(self): ...  # TODO
    # def test_translate_raise_missing_required_key(self): ...  # TODO

    @pytest.mark.parametrize("typestore", ROS_TYPESTORE_TO_TEST)
    def test_to_ros_point(self, point: Point3d, typestore: Typestore):
        result = PointAdapter.to_ros(point, typestore, "geometry_msgs/msg/Point")

        assert point.x == result.x
        assert point.y == result.y
        assert point.z == result.z

    @pytest.mark.parametrize("typestore", ROS_TYPESTORE_TO_TEST)
    def test_to_ros_point_stamped(self, point_msg: Message, typestore: Typestore):
        point = point_msg.get_data(Point3d)
        result = PointAdapter.to_ros(
            point_msg, typestore, "geometry_msgs/msg/PointStamped"
        )

        assert (
            point_msg.timestamp_ns
            == Time(
                seconds=result.header.stamp.sec, nanoseconds=result.header.stamp.nanosec
            ).to_nanoseconds()
        )
        assert point_msg.frame_id == result.header.frame_id
        assert point.x == result.point.x
        assert point.y == result.point.y
        assert point.z == result.point.z

    @pytest.mark.parametrize("typestore", ROS_TYPESTORE_TO_TEST)
    def test_to_ros_default_type(self, point: Point3d, typestore: Typestore):
        result = PointAdapter.to_ros(point, typestore)

        assert point.x == result.x
        assert point.y == result.y
        assert point.z == result.z

    def test_to_ros_invalid_rosmsg_type(self, point: Point3d):
        result = PointAdapter.to_ros(
            point, get_typestore(Stores.LATEST), "geometry_msgs/msg/Bogus"
        )

        assert result is None

    def test_to_ros_invalid_mosaico_type(self, invalid_ms_msg):
        with pytest.raises(TypeError):
            PointAdapter.to_ros(invalid_ms_msg, get_typestore(Stores.LATEST))


###############################################################################
############################### TestPoseAdapter ###############################
###############################################################################


@pytest.fixture
def pose(point, quaternion) -> Pose:
    return Pose(
        position=point,
        orientation=quaternion,
    )


@pytest.fixture
def pose_msg(pose) -> Message:
    return Message(
        data=pose,
        timestamp_ns=100,
        frame_id="base_link",
    )


@pytest.fixture
def pose_w_cov(point, quaternion) -> Pose:
    return Pose(position=point, orientation=quaternion, covariance=list(range(0, 36)))


@pytest.fixture
def pose_w_cov_msg(pose_w_cov) -> Message:
    return Message(
        data=pose_w_cov,
        timestamp_ns=100,
        frame_id="base_link",
    )


class TestPoseAdapter:
    # def test_translate_pose(self): ...  # TODO
    # def test_translate_pose_stamped(self): ...  # TODO
    # def test_translate_pose_with_covariance(self): ...  # TODO
    # def test_translate_pose_with_covariance_stamped(self): ...  # TODO
    # def test_translate_raise_pose_not_dict(self): ...  # TODO
    # def test_translate_raise_missing_required_key(self): ...  # TODO

    @pytest.mark.parametrize("typestore", ROS_TYPESTORE_TO_TEST)
    def test_to_ros_pose(self, pose: Pose, typestore: Typestore):
        result = PoseAdapter.to_ros(pose, typestore, "geometry_msgs/msg/Pose")

        assert pose.position.x == result.position.x
        assert pose.position.y == result.position.y
        assert pose.position.z == result.position.z
        assert pose.orientation.x == result.orientation.x
        assert pose.orientation.y == result.orientation.y
        assert pose.orientation.z == result.orientation.z
        assert pose.orientation.w == result.orientation.w

    @pytest.mark.parametrize("typestore", ROS_TYPESTORE_TO_TEST)
    def test_to_ros_null_cov(self, pose: Pose, typestore: Typestore):
        result = PoseAdapter.to_ros(
            pose, typestore, "geometry_msgs/msg/PoseWithCovariance"
        )

        assert (result.covariance == 0).all()
        assert pose.position.x == result.pose.position.x
        assert pose.position.y == result.pose.position.y
        assert pose.position.z == result.pose.position.z
        assert pose.orientation.x == result.pose.orientation.x
        assert pose.orientation.y == result.pose.orientation.y
        assert pose.orientation.z == result.pose.orientation.z
        assert pose.orientation.w == result.pose.orientation.w

    @pytest.mark.parametrize("typestore", ROS_TYPESTORE_TO_TEST)
    def test_to_ros_with_cov(self, pose_w_cov: Pose, typestore: Typestore):
        result = PoseAdapter.to_ros(
            pose_w_cov, typestore, "geometry_msgs/msg/PoseWithCovariance"
        )

        assert np.array_equal(pose_w_cov.covariance, result.covariance)
        assert pose_w_cov.position.x == result.pose.position.x
        assert pose_w_cov.position.y == result.pose.position.y
        assert pose_w_cov.position.z == result.pose.position.z
        assert pose_w_cov.orientation.x == result.pose.orientation.x
        assert pose_w_cov.orientation.y == result.pose.orientation.y
        assert pose_w_cov.orientation.z == result.pose.orientation.z
        assert pose_w_cov.orientation.w == result.pose.orientation.w

    @pytest.mark.parametrize("typestore", ROS_TYPESTORE_TO_TEST)
    def test_to_ros_pose_stamped(self, pose_msg: Message, typestore: Typestore):
        pose = pose_msg.get_data(Pose)
        result = PoseAdapter.to_ros(
            pose_msg, typestore, "geometry_msgs/msg/PoseStamped"
        )

        assert (
            pose_msg.timestamp_ns
            == Time(
                seconds=result.header.stamp.sec, nanoseconds=result.header.stamp.nanosec
            ).to_nanoseconds()
        )
        assert pose_msg.frame_id == result.header.frame_id
        assert pose.position.x == result.pose.position.x
        assert pose.position.y == result.pose.position.y
        assert pose.position.z == result.pose.position.z
        assert pose.orientation.x == result.pose.orientation.x
        assert pose.orientation.y == result.pose.orientation.y
        assert pose.orientation.z == result.pose.orientation.z
        assert pose.orientation.w == result.pose.orientation.w

    @pytest.mark.parametrize("typestore", ROS_TYPESTORE_TO_TEST)
    def test_to_ros_pose_stamped_w_cov(
        self, pose_w_cov_msg: Message, typestore: Typestore
    ):
        pose_w_cov = pose_w_cov_msg.get_data(Pose)
        result = PoseAdapter.to_ros(
            pose_w_cov_msg,
            typestore,
            "geometry_msgs/msg/PoseWithCovarianceStamped",
        )

        assert (
            pose_w_cov_msg.timestamp_ns
            == Time(
                seconds=result.header.stamp.sec, nanoseconds=result.header.stamp.nanosec
            ).to_nanoseconds()
        )
        assert pose_w_cov_msg.frame_id == result.header.frame_id
        assert np.array_equal(pose_w_cov.covariance, result.pose.covariance)
        assert pose_w_cov.position.x == result.pose.pose.position.x
        assert pose_w_cov.position.y == result.pose.pose.position.y
        assert pose_w_cov.position.z == result.pose.pose.position.z
        assert pose_w_cov.orientation.x == result.pose.pose.orientation.x
        assert pose_w_cov.orientation.y == result.pose.pose.orientation.y
        assert pose_w_cov.orientation.z == result.pose.pose.orientation.z
        assert pose_w_cov.orientation.w == result.pose.pose.orientation.w

    @pytest.mark.parametrize("typestore", ROS_TYPESTORE_TO_TEST)
    def test_to_ros_default_type(self, pose: Pose, typestore: Typestore):
        result = PoseAdapter.to_ros(pose, typestore)

        assert pose.position.x == result.position.x
        assert pose.position.y == result.position.y
        assert pose.position.z == result.position.z
        assert pose.orientation.x == result.orientation.x
        assert pose.orientation.y == result.orientation.y
        assert pose.orientation.z == result.orientation.z
        assert pose.orientation.w == result.orientation.w

    def test_to_ros_invalid_rosmsg_type(self, pose: Pose):
        result = PoseAdapter.to_ros(
            pose, get_typestore(Stores.LATEST), "geometry_msgs/msg/Bogus"
        )

        assert result is None

    def test_to_ros_invalid_mosaico_type(self, invalid_ms_msg):

        with pytest.raises(TypeError):
            PoseAdapter.to_ros(invalid_ms_msg, get_typestore(Stores.LATEST))
