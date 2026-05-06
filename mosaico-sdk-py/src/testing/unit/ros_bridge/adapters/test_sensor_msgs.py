import numpy as np
import pytest
from rosbags.typesys.stores import Stores, Typestore, get_typestore

from mosaicolabs import (
    GPS,
    IMU,
    ROI,
    CameraInfo,
    GPSStatus,
    Message,
    Point3d,
    Quaternion,
    Serializable,
    Time,
    Vector2d,
    Vector3d,
)
from mosaicolabs.ros_bridge.adapters import (
    CameraInfoAdapter,
    GPSAdapter,
    IMUAdapter,
    NavSatStatusAdapter,
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


###############################################################################
########################### TestCameraInfoAdapter #############################
###############################################################################


@pytest.fixture
def camera_info():
    return CameraInfo(
        height=1080,
        width=1920,
        distortion_model="plumb_bob",
        distortion_parameters=list(range(0, 6)),
        intrinsic_parameters=list(range(0, 9)),
        rectification_parameters=list(range(0, 9)),
        projection_parameters=list(range(0, 12)),
        binning=Vector2d(x=1.0, y=2.0),
        roi=ROI(offset=Vector2d(x=1.0, y=2.0), height=500, width=600, do_rectify=True),
    )


@pytest.fixture
def camera_info_msg(camera_info):
    return Message(
        data=camera_info,
        timestamp_ns=100,
        frame_id="base_link",
    )


class TestCameraInfoAdapter:
    # def test_translate_camera_info(self): ...  # TODO
    # def test_translate_camera_info_stamped(self): ...  # TODO
    # def test_translate_raise_camera_info_not_dict(self): ...  # TODO
    # def test_translate_raise_missing_required_key(self): ...  # TODO

    def assert_camera_info(self, camera_info, ros_msg):
        assert camera_info.height == ros_msg.height
        assert camera_info.width == ros_msg.width
        assert camera_info.distortion_model == ros_msg.distortion_model

        if "d" in ros_msg.__dataclass_fields__:  # ROS2
            assert (camera_info.distortion_parameters == ros_msg.d).all()
        else:
            assert (camera_info.distortion_parameters == ros_msg.D).all()

        if "k" in ros_msg.__dataclass_fields__:  # ROS2
            assert (camera_info.intrinsic_parameters == ros_msg.k).all()
        else:
            assert (camera_info.intrinsic_parameters == ros_msg.K).all()

        if "r" in ros_msg.__dataclass_fields__:  # ROS2
            assert (camera_info.rectification_parameters == ros_msg.r).all()
        else:
            assert (camera_info.rectification_parameters == ros_msg.R).all()

        if "p" in ros_msg.__dataclass_fields__:  # ROS2
            assert (camera_info.projection_parameters == ros_msg.p).all()
        else:
            assert (camera_info.projection_parameters == ros_msg.P).all()

        assert camera_info.binning.x == ros_msg.binning_x
        assert camera_info.binning.y == ros_msg.binning_y
        assert camera_info.roi.offset.x == ros_msg.roi.x_offset
        assert camera_info.roi.offset.y == ros_msg.roi.y_offset
        assert camera_info.roi.height == ros_msg.roi.height
        assert camera_info.roi.width == ros_msg.roi.width
        assert camera_info.roi.do_rectify == ros_msg.roi.do_rectify

    @pytest.mark.parametrize("typestore", ROS_TYPESTORE_TO_TEST)
    def test_to_ros_camera_info(self, camera_info: CameraInfo, typestore: Typestore):
        ros_msg = CameraInfoAdapter.to_ros(
            camera_info, typestore, "sensor_msgs/msg/CameraInfo"
        )

        self.assert_camera_info(camera_info, ros_msg)

    @pytest.mark.parametrize("typestore", ROS_TYPESTORE_TO_TEST)
    def test_to_ros_camera_info_message(
        self, camera_info_msg: Message, typestore: Typestore
    ):
        camera_info = camera_info_msg.get_data(CameraInfo)
        ros_msg = CameraInfoAdapter.to_ros(
            camera_info_msg, typestore, "sensor_msgs/msg/CameraInfo"
        )

        self.assert_camera_info(camera_info, ros_msg)

    @pytest.mark.parametrize("typestore", ROS_TYPESTORE_TO_TEST)
    def test_to_ros_default_type(self, camera_info: CameraInfo, typestore: Typestore):
        ros_msg = CameraInfoAdapter.to_ros(camera_info, typestore)

        self.assert_camera_info(camera_info, ros_msg)

    def test_to_ros_invalid_rosmsg_type(self, camera_info: CameraInfo):
        ros_msg = CameraInfoAdapter.to_ros(
            camera_info, get_typestore(Stores.LATEST), "sensor_msgs/msg/Bogus"
        )

        assert ros_msg is None

    def test_to_ros_invalid_mosaico_type(self, invalid_ms_msg):
        with pytest.raises(TypeError):
            CameraInfoAdapter.to_ros(invalid_ms_msg, get_typestore(Stores.LATEST))


###############################################################################
############################ TestNavSatStatusAdapter ##########################
###############################################################################


@pytest.fixture
def gps_status():
    return GPSStatus(
        status=0,
        service=1,
    )


@pytest.fixture
def gps_status_msg(gps_status):
    return Message(
        data=gps_status,
        timestamp_ns=100,
        frame_id="base_satellite",
    )


class TestNavSatStatusAdapter:
    # def test_translate_nav_sat_status(self): ...  # TODO
    # def test_translate_nav_sat_status_stamped(self): ...  # TODO
    # def test_translate_raise_nav_sat_status_not_dict(self): ...  # TODO
    # def test_translate_raise_missing_required_key(self): ...  # TODO

    @pytest.mark.parametrize("typestore", ROS_TYPESTORE_TO_TEST)
    def test_to_ros_nav_sat_status(self, gps_status: GPSStatus, typestore: Typestore):
        ros_msg = NavSatStatusAdapter.to_ros(
            gps_status, typestore, "sensor_msgs/msg/NavSatStatus"
        )

        assert gps_status.status == ros_msg.status
        assert gps_status.service == ros_msg.service

    @pytest.mark.parametrize("typestore", ROS_TYPESTORE_TO_TEST)
    def test_to_ros_nav_sat_status_message(
        self, gps_status_msg: Message, typestore: Typestore
    ):
        gps_status = gps_status_msg.get_data(GPSStatus)
        ros_msg = NavSatStatusAdapter.to_ros(
            gps_status_msg, typestore, "sensor_msgs/msg/NavSatStatus"
        )

        assert gps_status.status == ros_msg.status
        assert gps_status.service == ros_msg.service

    @pytest.mark.parametrize("typestore", ROS_TYPESTORE_TO_TEST)
    def test_to_ros_default_type(self, gps_status: GPSStatus, typestore: Typestore):
        ros_msg = NavSatStatusAdapter.to_ros(gps_status, typestore)

        assert gps_status.status == ros_msg.status
        assert gps_status.service == ros_msg.service

    def test_to_ros_invalid_rosmsg_type(self, gps_status: GPSStatus):
        ros_msg = NavSatStatusAdapter.to_ros(
            gps_status, get_typestore(Stores.LATEST), "sensor_msgs/msg/Bogus"
        )

        assert ros_msg is None

    def test_to_ros_invalid_mosaico_type(self, invalid_ms_msg):
        with pytest.raises(TypeError):
            NavSatStatusAdapter.to_ros(invalid_ms_msg, get_typestore(Stores.LATEST))


###############################################################################
################################ TestGPSAdapter ###############################
###############################################################################


@pytest.fixture
def gps(gps_status):
    return GPS(position=Point3d(x=1.0, y=2.0, z=3.0), status=gps_status)


@pytest.fixture
def point_w_cov():
    return Point3d(x=1.0, y=2.0, z=3.0, covariance=list(range(0, 9)), covariance_type=3)


@pytest.fixture
def gps_w_cov(point_w_cov, gps_status):
    return GPS(
        position=point_w_cov,
        status=gps_status,
    )


@pytest.fixture
def gps_msg(gps):
    return Message(
        data=gps,
        timestamp_ns=100,
        frame_id="base_satellite",
    )


class TestGPSAdapter:
    # def test_translate_nav_sat_fix(self): ...  # TODO
    # def test_translate_nav_sat_fix_stamped(self): ...  # TODO
    # def test_translate_raise_nav_sat_fix_not_dict(self): ...  # TODO
    # def test_translate_raise_missing_required_key(self): ...  # TODO

    @pytest.mark.parametrize("typestore", ROS_TYPESTORE_TO_TEST)
    def test_to_ros_nav_sat_fix(self, gps: GPS, typestore: Typestore):
        ros_msg = GPSAdapter.to_ros(gps, typestore, "sensor_msgs/msg/NavSatFix")

        assert gps.status.status == ros_msg.status.status
        assert gps.status.service == ros_msg.status.service
        assert gps.position.x == ros_msg.latitude
        assert gps.position.y == ros_msg.longitude
        assert gps.position.z == ros_msg.altitude
        assert (ros_msg.position_covariance == 0.0).all()

    @pytest.mark.parametrize("typestore", ROS_TYPESTORE_TO_TEST)
    def test_to_ros_nav_sat_fix_w_cov(self, gps_w_cov: GPS, typestore: Typestore):
        ros_msg = GPSAdapter.to_ros(gps_w_cov, typestore, "sensor_msgs/msg/NavSatFix")

        assert gps_w_cov.status.status == ros_msg.status.status
        assert gps_w_cov.status.service == ros_msg.status.service
        assert gps_w_cov.position.x == ros_msg.latitude
        assert gps_w_cov.position.y == ros_msg.longitude
        assert gps_w_cov.position.z == ros_msg.altitude
        assert np.array_equal(
            gps_w_cov.position.covariance, ros_msg.position_covariance
        )
        assert gps_w_cov.position.covariance_type == ros_msg.position_covariance_type

    @pytest.mark.parametrize("typestore", ROS_TYPESTORE_TO_TEST)
    def test_to_ros_nav_sat_fix_message(self, gps_msg: Message, typestore: Typestore):
        gps = gps_msg.get_data(GPS)
        ros_msg = GPSAdapter.to_ros(gps_msg, typestore, "sensor_msgs/msg/NavSatFix")

        assert gps_msg.frame_id == ros_msg.header.frame_id
        assert (
            gps_msg.timestamp_ns
            == Time(
                seconds=ros_msg.header.stamp.sec,
                nanoseconds=ros_msg.header.stamp.nanosec,
            ).to_nanoseconds()
        )
        assert gps.status.status == ros_msg.status.status
        assert gps.status.service == ros_msg.status.service
        assert gps.position.x == ros_msg.latitude
        assert gps.position.y == ros_msg.longitude
        assert gps.position.z == ros_msg.altitude
        assert (ros_msg.position_covariance == 0.0).all()

    @pytest.mark.parametrize("typestore", ROS_TYPESTORE_TO_TEST)
    def test_to_ros_default_type(self, gps: GPS, typestore: Typestore):
        ros_msg = GPSAdapter.to_ros(gps, typestore)

        assert gps.status.status == ros_msg.status.status
        assert gps.status.service == ros_msg.status.service
        assert gps.position.x == ros_msg.latitude
        assert gps.position.y == ros_msg.longitude
        assert gps.position.z == ros_msg.altitude
        assert (ros_msg.position_covariance == 0.0).all()

    def test_to_ros_invalid_rosmsg_type(self, gps: GPS):
        ros_msg = GPSAdapter.to_ros(
            gps, get_typestore(Stores.LATEST), "sensor_msgs/msg/Bogus"
        )

        assert ros_msg is None

    def test_to_ros_invalid_mosaico_type(self, invalid_ms_msg):
        with pytest.raises(TypeError):
            GPSAdapter.to_ros(invalid_ms_msg, get_typestore(Stores.LATEST))


###############################################################################
################################ TestIMUAdapter ###############################
###############################################################################


@pytest.fixture
def imu():
    return IMU(
        acceleration=Vector3d(x=1.0, y=2.0, z=3.0),
        angular_velocity=Vector3d(x=4.0, y=5.0, z=6.0),
        orientation=Quaternion(x=0.0, y=0.0, z=0.0, w=1.0),
    )


@pytest.fixture
def vector_w_cov():
    return Vector3d(
        x=1.0, y=2.0, z=3.0, covariance=list(range(0, 9)), covariance_type=3
    )


@pytest.fixture
def vector_w_cov_2():
    return Vector3d(
        x=4.0, y=5.0, z=6.0, covariance=list(range(9, 18)), covariance_type=3
    )


@pytest.fixture
def quaternion_w_cov():
    return Quaternion(
        x=0.0, y=0.0, z=0.0, w=1.0, covariance=list(range(18, 27)), covariance_type=3
    )


@pytest.fixture
def imu_w_cov(vector_w_cov, vector_w_cov_2, quaternion_w_cov):
    return IMU(
        acceleration=vector_w_cov,
        angular_velocity=vector_w_cov_2,
        orientation=quaternion_w_cov,
    )


@pytest.fixture
def imu_msg(imu):
    return Message(
        data=imu,
        timestamp_ns=100,
        frame_id="base_link",
    )


class TestIMUAdapter:
    # def test_translate_imu(self): ...  # TODO
    # def test_translate_imu_stamped(self): ...  # TODO
    # def test_translate_raise_imu_not_dict(self): ...  # TODO
    # def test_translate_raise_missing_required_key(self): ...  # TODO

    def assert_imu(self, imu: IMU, ros_msg):
        assert imu.orientation.x == ros_msg.orientation.x
        assert imu.orientation.y == ros_msg.orientation.y
        assert imu.orientation.z == ros_msg.orientation.z
        assert imu.angular_velocity.x == ros_msg.angular_velocity.x
        assert imu.angular_velocity.y == ros_msg.angular_velocity.y
        assert imu.angular_velocity.z == ros_msg.angular_velocity.z
        assert imu.acceleration.x == ros_msg.linear_acceleration.x
        assert imu.acceleration.y == ros_msg.linear_acceleration.y
        assert imu.acceleration.z == ros_msg.linear_acceleration.z

    @pytest.mark.parametrize("typestore", ROS_TYPESTORE_TO_TEST)
    def test_to_ros_imu(self, imu: IMU, typestore: Typestore):
        ros_msg = IMUAdapter.to_ros(imu, typestore, "sensor_msgs/msg/Imu")

        self.assert_imu(imu, ros_msg)
        assert (ros_msg.orientation_covariance == 0.0).all()
        assert (ros_msg.angular_velocity_covariance == 0.0).all()
        assert (ros_msg.linear_acceleration_covariance == 0.0).all()

    @pytest.mark.parametrize("typestore", ROS_TYPESTORE_TO_TEST)
    def test_to_ros_imu_w_cov(self, imu_w_cov: IMU, typestore: Typestore):
        ros_msg = IMUAdapter.to_ros(imu_w_cov, typestore, "sensor_msgs/msg/Imu")

        self.assert_imu(imu_w_cov, ros_msg)
        assert np.array_equal(
            imu_w_cov.orientation.covariance, ros_msg.orientation_covariance
        )
        assert np.array_equal(
            imu_w_cov.angular_velocity.covariance, ros_msg.angular_velocity_covariance
        )
        assert np.array_equal(
            imu_w_cov.acceleration.covariance, ros_msg.linear_acceleration_covariance
        )

    @pytest.mark.parametrize("typestore", ROS_TYPESTORE_TO_TEST)
    def test_to_ros_imu_message(self, imu_msg: Message, typestore: Typestore):
        imu = imu_msg.get_data(IMU)
        ros_msg = IMUAdapter.to_ros(imu_msg, typestore, "sensor_msgs/msg/Imu")

        assert imu_msg.frame_id == ros_msg.header.frame_id
        assert (
            imu_msg.timestamp_ns
            == Time(
                seconds=ros_msg.header.stamp.sec,
                nanoseconds=ros_msg.header.stamp.nanosec,
            ).to_nanoseconds()
        )
        self.assert_imu(imu, ros_msg)

    @pytest.mark.parametrize("typestore", ROS_TYPESTORE_TO_TEST)
    def test_to_ros_default_type(self, imu: IMU, typestore: Typestore):
        ros_msg = IMUAdapter.to_ros(imu, typestore)

        self.assert_imu(imu, ros_msg)

    def test_to_ros_invalid_rosmsg_type(self, imu: IMU):
        ros_msg = IMUAdapter.to_ros(
            imu, get_typestore(Stores.LATEST), "sensor_msgs/msg/Bogus"
        )

        assert ros_msg is None

    def test_to_ros_invalid_mosaico_type(self, invalid_ms_msg):
        with pytest.raises(TypeError):
            IMUAdapter.to_ros(invalid_ms_msg, get_typestore(Stores.LATEST))
