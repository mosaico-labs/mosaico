import numpy as np
import pytest
from rosbags.typesys.stores import Stores, Typestore, get_typestore

from mosaicolabs import Message, Point3d, Pose, Quaternion, Serializable, Time
from mosaicolabs.ros_bridge.adapters import PoseAdapter

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
def ms_point():
    return Point3d(x=1.0, y=2.0, z=3.0)


@pytest.fixture
def ms_quaternion():
    return Quaternion(x=0.0, y=0.0, z=0.0, w=1.0)


@pytest.fixture
def invalid_ms_message() -> Message:
    return Message(
        data=Serializable(),
        timestamp_ns=0,
    )


###############################################################################
############################### TestPoseAdapter ###############################
###############################################################################


class TestPoseAdapter:
    # @pytest.fixture
    # def pose_dict(self):
    #     return {
    #         "position": {"x": 1.0, "y": 2.0, "z": 3.0},
    #         "orientation": {"x": 0.0, "y": 0.0, "z": 0.0, "w": 1.0},
    #     }

    # @pytest.fixture
    # def pose_w_cov_dict(self, pose_dict):
    #     return {
    #         "pose": pose_dict,
    #         "covariance": list(range(0, 36)),
    #     }

    # @pytest.fixture
    # def pose_stamped_dict(self, header_dict, pose_dict):
    #     return {"header": header_dict, "pose": pose_dict}

    # @pytest.fixture
    # def pose_with_cov_stamped_dict(self, header_dict, pose_w_cov_dict):
    #     return {"header": header_dict, "pose": pose_w_cov_dict}

    # def test_translate_pose(self): ...  # TODO
    # def test_translate_pose_stamped(self): ...  # TODO
    # def test_translate_pose_with_covariance(self): ...  # TODO
    # def test_translate_pose_with_covariance_stamped(self): ...  # TODO
    # def test_translate_raise_pose_not_dict(self): ...  # TODO
    # def test_translate_raise_missing_required_key(self): ...  # TODO

    @pytest.fixture
    def pose(self, ms_point, ms_quaternion) -> Pose:
        return Pose(
            position=ms_point,
            orientation=ms_quaternion,
        )

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

    def test_to_ros_invalid_rosmsg_type(self, pose: Pose):
        result = PoseAdapter.to_ros(
            pose, get_typestore(Stores.LATEST), "geometry_msgs/msg/Bogus"
        )

        assert result is None

    @pytest.fixture
    def pose_w_cov(self, ms_point, ms_quaternion) -> Pose:
        return Pose(
            position=ms_point, orientation=ms_quaternion, covariance=list(range(0, 36))
        )

    @pytest.mark.parametrize("typestore", ROS_TYPESTORE_TO_TEST)
    def test_to_ros_with_cov(self, pose_w_cov: Pose, typestore: Typestore):
        result = PoseAdapter.to_ros(
            pose_w_cov, typestore, "geometry_msgs/msg/PoseWithCovariance"
        )

        assert np.array_equal(pose_w_cov.covariance, result.covariance)

    @pytest.fixture
    def pose_message(self, pose) -> Message:
        return Message(
            data=pose,
            timestamp_ns=100,
            frame_id="base_link",
        )

    @pytest.mark.parametrize("typestore", ROS_TYPESTORE_TO_TEST)
    def test_to_ros_pose_stamped(self, pose_message: Message, typestore: Typestore):
        result = PoseAdapter.to_ros(
            pose_message, typestore, "geometry_msgs/msg/PoseStamped"
        )

        assert (
            pose_message.timestamp_ns
            == Time(
                seconds=result.header.stamp.sec, nanoseconds=result.header.stamp.nanosec
            ).to_nanoseconds()
        )
        assert pose_message.frame_id == result.header.frame_id

    @pytest.fixture
    def message_w_cov(self, pose_w_cov) -> Message:
        return Message(
            data=pose_w_cov,
            timestamp_ns=100,
            frame_id="base_link",
        )

    @pytest.mark.parametrize("typestore", ROS_TYPESTORE_TO_TEST)
    def test_to_ros_pose_stamped_w_cov(
        self, message_w_cov: Message, typestore: Typestore
    ):
        result = PoseAdapter.to_ros(
            message_w_cov,
            typestore,
            "geometry_msgs/msg/PoseWithCovarianceStamped",
        )

        assert (
            message_w_cov.timestamp_ns
            == Time(
                seconds=result.header.stamp.sec, nanoseconds=result.header.stamp.nanosec
            ).to_nanoseconds()
        )
        assert message_w_cov.frame_id == result.header.frame_id
        assert np.array_equal(
            message_w_cov.get_data(Pose).covariance, result.pose.covariance
        )

    def test_to_ros_invalid_mosaico_type(self, invalid_ms_message):

        with pytest.raises(TypeError):
            PoseAdapter.to_ros(
                invalid_ms_message,
                get_typestore(Stores.LATEST),
                "geometry_msgs/msg/Pose",
            )
