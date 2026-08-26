import math
import sys
from dataclasses import asdict

import numpy as np
import pytest
from rosbags.typesys import get_types_from_msg
from rosbags.typesys.store import Typestore
from rosbags.typesys.stores import Stores, get_typestore

from mosaicolabs import (
    GPS,
    IMU,
    ROI,
    CameraInfo,
    CompressedImage,
    GPSStatus,
    Image,
    ImageFormat,
    Joy,
    Magnetometer,
    Message,
    NMEASentence,
    Point3d,
    Pressure,
    Quaternion,
    RobotJoint,
    Serializable,
    Temperature,
    Vector2d,
    Vector3d,
    futures,
)
from mosaicolabs.bridges.ros import ROSMessage
from mosaicolabs.bridges.ros.adapters import (
    BatteryStateAdapter,
    CameraInfoAdapter,
    CompressedImageAdapter,
    GPSAdapter,
    ImageAdapter,
    IMUAdapter,
    JoyAdapter,
    LaserScanAdapter,
    LidarAdapter,
    MagneticFieldAdapter,
    MultiEchoLaserScanAdapter,
    NavSatStatusAdapter,
    NMEASentenceAdapter,
    PointCloudAdapter,
    PointCloudAdapterBase,
    PressureAdapter,
    RadarAdapter,
    RGBDCameraAdapter,
    RobotJointAdapter,
    ROIAdapter,
    StereoCameraAdapter,
    TemperatureAdapter,
    ToFCameraAdapter,
)
from mosaicolabs.bridges.ros.data_ontology import (
    BatteryState,
    PointCloud2,
    PointField,
    PointFieldDataType,
)
from testing.unit.bridges.ros.adapters.helper import (
    assert_battery_state,
    assert_camera_info,
    assert_compressed_image,
    assert_gps,
    assert_gps_status,
    assert_image,
    assert_imu,
    assert_joy,
    assert_laserscan,
    assert_magnetometer,
    assert_multiecho_laserscan,
    assert_nmea_sentence,
    assert_pcl2,
    assert_pressure,
    assert_robot_joint,
    assert_roi,
    assert_temperature,
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
def camera_info_w_header(camera_info, ms_header):
    camera_info.header = ms_header
    return camera_info


@pytest.fixture
def roi_rosmsg():
    return ROSMessage(
        bag_timestamp_ns=100,
        topic="/roi",
        msg_type="sensor_msgs/msg/RegionOfInterest",
        data={
            "x_offset": 0,
            "y_offset": 0,
            "height": 100,
            "width": 200,
            "do_rectify": True,
        },
    )


@pytest.fixture
def camera_info_ros1msg(ros_header, roi_rosmsg):
    return ROSMessage(
        bag_timestamp_ns=100,
        topic="/camera_info",
        msg_type="sensor_msgs/msg/CameraInfo",
        data={
            "header": ros_header,
            "height": 1080,
            "width": 1920,
            "distortion_model": "plumb_bob",
            "D": list(range(0, 6)),
            "K": list(range(0, 9)),
            "R": list(range(0, 9)),
            "P": list(range(0, 12)),
            "binning_x": 3,
            "binning_y": 3,
            "roi": roi_rosmsg.data_field,
        },
    )


@pytest.fixture
def camera_info_ros2msg(ros_header, roi_rosmsg):
    return ROSMessage(
        bag_timestamp_ns=100,
        topic="/camera_info",
        msg_type="sensor_msgs/msg/CameraInfo",
        data={
            "header": ros_header,
            "height": 1080,
            "width": 1920,
            "distortion_model": "plumb_bob",
            "d": list(range(0, 6)),
            "k": list(range(0, 9)),
            "r": list(range(0, 9)),
            "p": list(range(0, 12)),
            "binning_x": 3,
            "binning_y": 3,
            "roi": roi_rosmsg.data_field,
        },
    )


class TestCameraInfoAdapter:
    def test_translate_camera_info1(self, camera_info_ros1msg: ROSMessage):
        ms_msg = CameraInfoAdapter.translate(camera_info_ros1msg)

        assert_camera_info(ms_msg.get_data(CameraInfo), camera_info_ros1msg.data_field)

    def test_translate_camera_info2(self, camera_info_ros2msg: ROSMessage):
        ms_msg = CameraInfoAdapter.translate(camera_info_ros2msg)

        assert_camera_info(ms_msg.get_data(CameraInfo), camera_info_ros2msg.data_field)

    def test_translate_raise_missing_required_key(
        self, camera_info_ros1msg: ROSMessage
    ):
        data = camera_info_ros1msg.data_field
        data.pop("height")
        with pytest.raises(ValueError, match="missing required keys"):
            CameraInfoAdapter.from_dict(data)

    @pytest.mark.parametrize("typestore", ROS_TYPESTORE_TO_TEST)
    def test_to_ros_camera_info(self, camera_info: CameraInfo, typestore: Typestore):
        ros_msg = CameraInfoAdapter.to_ros(
            camera_info, typestore, "sensor_msgs/msg/CameraInfo"
        )

        assert_camera_info(camera_info, asdict(ros_msg))

    @pytest.mark.parametrize("typestore", ROS_TYPESTORE_TO_TEST)
    def test_to_ros_camera_info_w_header(
        self, camera_info_w_header: CameraInfo, typestore: Typestore
    ):
        ros_msg = CameraInfoAdapter.to_ros(
            camera_info_w_header, typestore, "sensor_msgs/msg/CameraInfo"
        )

        assert_camera_info(camera_info_w_header, asdict(ros_msg))

    @pytest.mark.parametrize("typestore", ROS_TYPESTORE_TO_TEST)
    def test_to_ros_default_type(self, camera_info: CameraInfo, typestore: Typestore):
        ros_msg = CameraInfoAdapter.to_ros(camera_info, typestore)

        assert_camera_info(camera_info, asdict(ros_msg))

    def test_to_ros_invalid_rosmsg_type(self, camera_info: CameraInfo):

        with pytest.raises(
            TypeError,
            match=f"Adapter {CameraInfoAdapter.__name__} does not support sensor_msgs/msg/Bogus",
        ):
            CameraInfoAdapter.to_ros(
                camera_info, get_typestore(Stores.LATEST), "sensor_msgs/msg/Bogus"
            )

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
def nav_sat_status_rosmsg(gps_status: GPSStatus):
    return ROSMessage(
        bag_timestamp_ns=100,
        topic="/nav_sat_status",
        msg_type="sensor_msgs/msg/NavSatStatus",
        data={"status": gps_status.status, "service": gps_status.service},
    )


class TestNavSatStatusAdapter:
    def test_translate_nav_sat_status(self, nav_sat_status_rosmsg: ROSMessage):
        ms_msg = NavSatStatusAdapter.translate(nav_sat_status_rosmsg)

        assert_gps_status(ms_msg.get_data(GPSStatus), nav_sat_status_rosmsg.data_field)

    def test_translate_raise_missing_required_key(
        self, nav_sat_status_rosmsg: ROSMessage
    ):
        data = nav_sat_status_rosmsg.data_field
        data.pop("status")
        with pytest.raises(ValueError, match="missing required keys"):
            NavSatStatusAdapter.from_dict(data)

    @pytest.mark.parametrize("typestore", ROS_TYPESTORE_TO_TEST)
    def test_to_ros_nav_sat_status(self, gps_status: GPSStatus, typestore: Typestore):
        ros_msg = NavSatStatusAdapter.to_ros(
            gps_status, typestore, "sensor_msgs/msg/NavSatStatus"
        )

        assert_gps_status(gps_status, asdict(ros_msg))

    @pytest.mark.parametrize("typestore", ROS_TYPESTORE_TO_TEST)
    def test_to_ros_default_type(self, gps_status: GPSStatus, typestore: Typestore):
        ros_msg = NavSatStatusAdapter.to_ros(gps_status, typestore)

        assert_gps_status(gps_status, asdict(ros_msg))

    def test_to_ros_invalid_rosmsg_type(self, gps_status: GPSStatus):

        with pytest.raises(
            TypeError,
            match=f"Adapter {NavSatStatusAdapter.__name__} does not support sensor_msgs/msg/Bogus",
        ):
            NavSatStatusAdapter.to_ros(
                gps_status, get_typestore(Stores.LATEST), "sensor_msgs/msg/Bogus"
            )

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
def gps_w_header(gps, ms_header):
    gps.header = ms_header
    return gps


@pytest.fixture
def nav_sat_fix_rosmsg(ros_header, gps_status: GPSStatus):
    return ROSMessage(
        bag_timestamp_ns=100,
        topic="/nav_sat_fix",
        msg_type="sensor_msgs/msg/NavSatFix",
        data={
            "header": ros_header,
            "status": {"status": gps_status.status, "service": gps_status.service},
            "latitude": 10.0,
            "longitude": 20.0,
            "altitude": 30.0,
            "position_covariance": list(range(9)),
            "position_covariance_type": 2,
        },
    )


class TestGPSAdapter:
    def test_translate_nav_sat_fix(self, nav_sat_fix_rosmsg: ROSMessage):
        ms_msg = GPSAdapter.translate(nav_sat_fix_rosmsg)

        assert_gps(ms_msg.get_data(GPS), nav_sat_fix_rosmsg.data_field)

    def test_translate_raise_missing_required_key(self, nav_sat_fix_rosmsg: ROSMessage):
        data = nav_sat_fix_rosmsg.data_field
        data.pop("status")
        with pytest.raises(ValueError, match="missing required keys"):
            GPSAdapter.from_dict(data)

    @pytest.mark.parametrize("typestore", ROS_TYPESTORE_TO_TEST)
    def test_to_ros_nav_sat_fix(self, gps: GPS, typestore: Typestore):
        ros_msg = GPSAdapter.to_ros(gps, typestore, "sensor_msgs/msg/NavSatFix")

        assert_gps(gps, asdict(ros_msg))

    @pytest.mark.parametrize("typestore", ROS_TYPESTORE_TO_TEST)
    def test_to_ros_nav_sat_fix_w_cov(self, gps_w_cov: GPS, typestore: Typestore):
        ros_msg = GPSAdapter.to_ros(gps_w_cov, typestore, "sensor_msgs/msg/NavSatFix")

        assert_gps(gps_w_cov, asdict(ros_msg))

    @pytest.mark.parametrize("typestore", ROS_TYPESTORE_TO_TEST)
    def test_to_ros_nav_sat_fix_w_header(self, gps_w_header: GPS, typestore: Typestore):
        ros_msg = GPSAdapter.to_ros(
            gps_w_header, typestore, "sensor_msgs/msg/NavSatFix"
        )

        assert_gps(gps_w_header, asdict(ros_msg))

    @pytest.mark.parametrize("typestore", ROS_TYPESTORE_TO_TEST)
    def test_to_ros_default_type(self, gps: GPS, typestore: Typestore):
        ros_msg = GPSAdapter.to_ros(gps, typestore)

        assert_gps(gps, asdict(ros_msg))

    def test_to_ros_invalid_rosmsg_type(self, gps: GPS):

        with pytest.raises(
            TypeError,
            match=f"Adapter {GPSAdapter.__name__} does not support sensor_msgs/msg/Bogus",
        ):
            GPSAdapter.to_ros(
                gps, get_typestore(Stores.LATEST), "sensor_msgs/msg/Bogus"
            )

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
def imu_w_header(imu, ms_header):
    imu.header = ms_header
    return imu


@pytest.fixture
def imu_rosmsg(ros_header):
    return ROSMessage(
        bag_timestamp_ns=100,
        topic="/imu",
        msg_type="sensor_msgs/msg/imu",
        data={
            "header": ros_header,
            "orientation": {"x": 0.0, "y": 0.0, "z": 0.0, "w": 1.0},
            "orientation_covariance": [0.0] * 9,
            "angular_velocity": {"x": 1.0, "y": 2.0, "z": 3.0},
            "angular_velocity_covariance": [0.0] * 9,
            "linear_acceleration": {"x": 1.0, "y": 2.0, "z": 3.0},
            "linear_acceleration_covariance": [0.0] * 9,
        },
    )


@pytest.fixture
def imu_w_cov_rosmsg(ros_header):
    tmp = ROSMessage(
        bag_timestamp_ns=100,
        topic="/imu",
        msg_type="sensor_msgs/msg/imu",
        data={
            "header": ros_header,
            "orientation": {"x": 0.0, "y": 0.0, "z": 0.0, "w": 1.0},
            "orientation_covariance": list(range(9)),
            "angular_velocity": {"x": 1.0, "y": 2.0, "z": 3.0},
            "angular_velocity_covariance": list(range(9)),
            "linear_acceleration": {"x": 1.0, "y": 2.0, "z": 3.0},
            "linear_acceleration_covariance": list(range(9)),
        },
    )

    return tmp


class TestIMUAdapter:
    def test_translate_imu(self, imu_rosmsg: ROSMessage):
        ms_msg = IMUAdapter.translate(imu_rosmsg)

        assert_imu(ms_msg.get_data(IMU), imu_rosmsg.data_field)

    def test_translate_imu_w_cov(self, imu_w_cov_rosmsg: ROSMessage):
        ms_msg = IMUAdapter.translate(imu_w_cov_rosmsg)

        assert_imu(ms_msg.get_data(IMU), imu_w_cov_rosmsg.data_field)

    def test_translate_raise_missing_required_key(self, imu_w_cov_rosmsg: ROSMessage):
        data = imu_w_cov_rosmsg.data_field
        data.pop("angular_velocity")
        with pytest.raises(ValueError, match="missing required keys"):
            IMUAdapter.from_dict(data)

    @pytest.mark.parametrize("typestore", ROS_TYPESTORE_TO_TEST)
    def test_to_ros_imu(self, imu: IMU, typestore: Typestore):
        ros_msg = IMUAdapter.to_ros(imu, typestore, "sensor_msgs/msg/Imu")

        assert_imu(imu, asdict(ros_msg))

    @pytest.mark.parametrize("typestore", ROS_TYPESTORE_TO_TEST)
    def test_to_ros_imu_w_cov(self, imu_w_cov: IMU, typestore: Typestore):
        ros_msg = IMUAdapter.to_ros(imu_w_cov, typestore, "sensor_msgs/msg/Imu")

        assert_imu(imu_w_cov, asdict(ros_msg))

    @pytest.mark.parametrize("typestore", ROS_TYPESTORE_TO_TEST)
    def test_to_ros_imu_message(self, imu_w_header: IMU, typestore: Typestore):
        ros_msg = IMUAdapter.to_ros(imu_w_header, typestore, "sensor_msgs/msg/Imu")

        assert_imu(imu_w_header, asdict(ros_msg))

    @pytest.mark.parametrize("typestore", ROS_TYPESTORE_TO_TEST)
    def test_to_ros_default_type(self, imu: IMU, typestore: Typestore):
        ros_msg = IMUAdapter.to_ros(imu, typestore)

        assert_imu(imu, asdict(ros_msg))

    def test_to_ros_invalid_rosmsg_type(self, imu: IMU):

        with pytest.raises(
            TypeError,
            match=f"Adapter {IMUAdapter.__name__} does not support sensor_msgs/msg/Bogus",
        ):
            IMUAdapter.to_ros(
                imu, get_typestore(Stores.LATEST), "sensor_msgs/msg/Bogus"
            )

    def test_to_ros_invalid_mosaico_type(self, invalid_ms_msg):
        with pytest.raises(TypeError):
            IMUAdapter.to_ros(invalid_ms_msg, get_typestore(Stores.LATEST))


###############################################################################
########################### TestNMEASentenceAdapter ###########################
###############################################################################


def register_nmea_sentence(typestore: Typestore):
    add_types = get_types_from_msg(
        "std_msgs/Header header\nstring sentence",
        "nmea_msgs/msg/Sentence",
    )
    typestore.register(add_types)
    return typestore


@pytest.fixture
def nmea_sentence():
    return NMEASentence(sentence="A sentence")


@pytest.fixture
def nmea_sentence_w_header(nmea_sentence, ms_header):
    nmea_sentence.header = ms_header
    return nmea_sentence


@pytest.fixture
def nmea_sentence_rosmsg(ros_header):
    return ROSMessage(
        bag_timestamp_ns=100,
        topic="/nmea_sentence",
        msg_type="nmea_msgs/msg/Sentence",
        data={
            "header": ros_header,
            "sentence": "Another sentence",
        },
    )


class TestNMEASentenceAdapter:
    def test_translate_nmea_sentence(self, nmea_sentence_rosmsg: ROSMessage):
        ms_msg = NMEASentenceAdapter.translate(nmea_sentence_rosmsg)

        assert_nmea_sentence(
            ms_msg.get_data(NMEASentence), nmea_sentence_rosmsg.data_field
        )

    def test_translate_raise_missing_required_key(
        self, nmea_sentence_rosmsg: ROSMessage
    ):
        data = nmea_sentence_rosmsg.data_field
        data.pop("sentence")
        with pytest.raises(ValueError, match="missing required keys"):
            NMEASentenceAdapter.from_dict(data)

    @pytest.mark.parametrize("typestore", ROS_TYPESTORE_TO_TEST)
    def test_to_ros_nmea_sentence(
        self, nmea_sentence: NMEASentence, typestore: Typestore
    ):

        typestore = register_nmea_sentence(typestore)
        ros_msg = NMEASentenceAdapter.to_ros(
            nmea_sentence, typestore, "nmea_msgs/msg/Sentence"
        )
        assert_nmea_sentence(nmea_sentence, asdict(ros_msg))

    @pytest.mark.parametrize("typestore", ROS_TYPESTORE_TO_TEST)
    def test_to_ros_nmea_sentence_w_header(
        self, nmea_sentence_w_header: NMEASentence, typestore: Typestore
    ):
        typestore = register_nmea_sentence(typestore)
        ros_msg = NMEASentenceAdapter.to_ros(
            nmea_sentence_w_header, typestore, "nmea_msgs/msg/Sentence"
        )
        assert_nmea_sentence(nmea_sentence_w_header, asdict(ros_msg))

    @pytest.mark.parametrize("typestore", ROS_TYPESTORE_TO_TEST)
    def test_to_ros_default_type(
        self, nmea_sentence: NMEASentence, typestore: Typestore
    ):
        typestore = register_nmea_sentence(typestore)
        ros_msg = NMEASentenceAdapter.to_ros(nmea_sentence, typestore)
        assert_nmea_sentence(nmea_sentence, asdict(ros_msg))

    def test_to_ros_invalid_rosmsg_type(self, nmea_sentence: NMEASentence):

        with pytest.raises(
            TypeError,
            match=f"Adapter {NMEASentenceAdapter.__name__} does not support sensor_msgs/msg/Bogus",
        ):
            NMEASentenceAdapter.to_ros(
                nmea_sentence, get_typestore(Stores.LATEST), "sensor_msgs/msg/Bogus"
            )

    def test_to_ros_invalid_mosaico_type(self, invalid_ms_msg):
        typestore = register_nmea_sentence(get_typestore(Stores.LATEST))
        with pytest.raises(TypeError):
            NMEASentenceAdapter.to_ros(invalid_ms_msg, typestore)


###############################################################################
############################# TestImageAdapter ################################
###############################################################################


@pytest.fixture
def image_raw():
    # 2x2 bgr8: stride = 2 pixels × 3 bytes = 6 bytes/row, total = 12 bytes
    return Image.from_linear_pixels(
        data=list(range(12)),
        stride=6,
        width=2,
        height=2,
        encoding="bgr8",
        is_bigendian=False,
        format=ImageFormat.RAW,
    )


@pytest.fixture
def image_png():
    # 2x2 bgr8: stride = 2 pixels × 3 bytes = 6 bytes/row, total = 12 bytes
    return Image.from_linear_pixels(
        data=list(range(12)),
        stride=6,
        width=2,
        height=2,
        encoding="rgb8",
        is_bigendian=False,
        format=ImageFormat.PNG,
    )


@pytest.fixture
def image_w_header(image_raw, ms_header):
    image_raw.header = ms_header
    return image_raw


@pytest.fixture
def image_rosmsg(ros_header):
    return ROSMessage(
        bag_timestamp_ns=100,
        topic="/image",
        msg_type="sensor_msgs/msg/Image",
        data={
            "header": ros_header,
            "height": 2,
            "width": 2,
            "encoding": "rgb8",  # defined in src/image_encodings.cpp
            "is_bigendian": sys.byteorder == "big",
            "step": 6,
            "data": np.array(
                [1, 200, 15, 200, 231, 123, 1, 200, 15, 200, 231, 123], dtype=np.uint8
            ),
        },
    )


class TestImageAdapter:
    def test_translate_image_sentence(self, image_rosmsg: ROSMessage):
        ms_msg = ImageAdapter.translate(image_rosmsg)

        assert_image(ms_msg.get_data(Image), image_rosmsg.data_field)

    def test_translate_raise_missing_required_key(self, image_rosmsg: ROSMessage):
        data = image_rosmsg.data_field
        data.pop("width")
        with pytest.raises(ValueError, match="missing required keys"):
            ImageAdapter.from_dict(data)

    @pytest.mark.parametrize("typestore", ROS_TYPESTORE_TO_TEST)
    def test_to_ros_image_raw(self, image_raw: Image, typestore: Typestore):
        ros_msg = ImageAdapter.to_ros(image_raw, typestore, "sensor_msgs/msg/Image")
        assert_image(image_raw, asdict(ros_msg))

    @pytest.mark.parametrize("typestore", ROS_TYPESTORE_TO_TEST)
    def test_to_ros_image_png(self, image_png: Image, typestore: Typestore):
        ros_msg = ImageAdapter.to_ros(image_png, typestore, "sensor_msgs/msg/Image")
        assert_image(image_png, asdict(ros_msg))

    @pytest.mark.parametrize("typestore", ROS_TYPESTORE_TO_TEST)
    def test_to_ros_image_message(self, image_w_header: Image, typestore: Typestore):
        ros_msg = ImageAdapter.to_ros(
            image_w_header, typestore, "sensor_msgs/msg/Image"
        )
        assert_image(image_w_header, asdict(ros_msg))

    @pytest.mark.parametrize("typestore", ROS_TYPESTORE_TO_TEST)
    def test_to_ros_default_type(self, image_raw: Image, typestore: Typestore):
        ros_msg = ImageAdapter.to_ros(image_raw, typestore)
        assert_image(image_raw, asdict(ros_msg))

    def test_to_ros_invalid_rosmsg_type(self, image_raw: Image):
        with pytest.raises(
            TypeError,
            match=f"Adapter {ImageAdapter.__name__} does not support sensor_msgs/msg/Bogus",
        ):
            ImageAdapter.to_ros(
                image_raw, get_typestore(Stores.LATEST), "sensor_msgs/msg/Bogus"
            )

    def test_to_ros_invalid_mosaico_type(self, invalid_ms_msg):
        with pytest.raises(TypeError):
            ImageAdapter.to_ros(invalid_ms_msg, get_typestore(Stores.LATEST))


###############################################################################
###################### TestCompressedImageAdapter ############################
###############################################################################


@pytest.fixture
def compressed_image():
    return CompressedImage(data=bytes(range(16)), format=ImageFormat.JPEG)


@pytest.fixture
def compressed_image_w_header(compressed_image, ms_header):
    compressed_image.header = ms_header
    return compressed_image


@pytest.fixture
def compressed_image_rosmsg(ros_header, compressed_image: CompressedImage):
    return ROSMessage(
        bag_timestamp_ns=100,
        topic="/compressed_image",
        msg_type="sensor_msgs/msg/CompressedImage",
        data={
            "header": ros_header,
            "format": compressed_image.format,
            "data": np.frombuffer(compressed_image.data, dtype=np.uint8),
        },
    )


class TestCompressedImageAdapter:
    def test_translate_image_compressed(self, compressed_image_rosmsg: ROSMessage):
        ms_msg = CompressedImageAdapter.translate(compressed_image_rosmsg)

        assert_compressed_image(
            ms_msg.get_data(CompressedImage), compressed_image_rosmsg.data_field
        )

    def test_translate_raise_missing_required_key(
        self, compressed_image_rosmsg: ROSMessage
    ):
        data = compressed_image_rosmsg.data_field
        data.pop("format")
        with pytest.raises(ValueError, match="missing required keys"):
            CompressedImageAdapter.from_dict(data)

    @pytest.mark.parametrize("typestore", ROS_TYPESTORE_TO_TEST)
    def test_to_ros_compressed_image(
        self, compressed_image: CompressedImage, typestore: Typestore
    ):
        ros_msg = CompressedImageAdapter.to_ros(
            compressed_image, typestore, "sensor_msgs/msg/CompressedImage"
        )

        assert_compressed_image(compressed_image, asdict(ros_msg))

    @pytest.mark.parametrize("typestore", ROS_TYPESTORE_TO_TEST)
    def test_to_ros_compressed_image_message(
        self, compressed_image_w_header: CompressedImage, typestore: Typestore
    ):
        ros_msg = CompressedImageAdapter.to_ros(
            compressed_image_w_header, typestore, "sensor_msgs/msg/CompressedImage"
        )

        assert_compressed_image(compressed_image_w_header, asdict(ros_msg))

    @pytest.mark.parametrize("typestore", ROS_TYPESTORE_TO_TEST)
    def test_to_ros_default_type(
        self, compressed_image: CompressedImage, typestore: Typestore
    ):
        ros_msg = CompressedImageAdapter.to_ros(compressed_image, typestore)

        assert_compressed_image(compressed_image, asdict(ros_msg))

    def test_to_ros_invalid_rosmsg_type(self, compressed_image: CompressedImage):

        with pytest.raises(
            TypeError,
            match=f"Adapter {CompressedImageAdapter.__name__} does not support sensor_msgs/msg/Bogus",
        ):
            CompressedImageAdapter.to_ros(
                compressed_image, get_typestore(Stores.LATEST), "sensor_msgs/msg/Bogus"
            )

    def test_to_ros_invalid_mosaico_type(self, invalid_ms_msg):
        with pytest.raises(TypeError):
            CompressedImageAdapter.to_ros(invalid_ms_msg, get_typestore(Stores.LATEST))


###############################################################################
################################ TestROIAdapter ###############################
###############################################################################


@pytest.fixture
def roi():
    return ROI(
        offset=Vector2d(x=10.0, y=20.0),
        height=100,
        width=200,
        do_rectify=True,
    )


class TestROIAdapter:
    def test_translate_roi_sentence(self, roi_rosmsg: ROSMessage):
        ms_msg = ROIAdapter.translate(roi_rosmsg)

        assert_roi(ms_msg.get_data(ROI), roi_rosmsg.data_field)
        assert ms_msg.timestamp_ns == roi_rosmsg.bag_timestamp_ns

    def test_translate_raise_missing_required_key(self, roi_rosmsg: ROSMessage):
        data = roi_rosmsg.data_field
        data.pop("height")
        with pytest.raises(ValueError, match="missing required keys"):
            ROIAdapter.from_dict(data)

    @pytest.mark.parametrize("typestore", ROS_TYPESTORE_TO_TEST)
    def test_to_ros_roi(self, roi: ROI, typestore: Typestore):
        ros_msg = ROIAdapter.to_ros(roi, typestore, "sensor_msgs/msg/RegionOfInterest")
        assert_roi(roi, asdict(ros_msg))

    @pytest.mark.parametrize("typestore", ROS_TYPESTORE_TO_TEST)
    def test_to_ros_default_type(self, roi: ROI, typestore: Typestore):
        ros_msg = ROIAdapter.to_ros(roi, typestore)
        assert_roi(roi, asdict(ros_msg))

    def test_to_ros_invalid_rosmsg_type(self, roi: ROI):

        with pytest.raises(
            TypeError,
            match=f"Adapter {ROIAdapter.__name__} does not support sensor_msgs/msg/Bogus",
        ):
            ROIAdapter.to_ros(
                roi, get_typestore(Stores.LATEST), "sensor_msgs/msg/Bogus"
            )

    def test_to_ros_invalid_mosaico_type(self, invalid_ms_msg):
        with pytest.raises(TypeError):
            ROIAdapter.to_ros(invalid_ms_msg, get_typestore(Stores.LATEST))


###############################################################################
############################ TestBatteryStateAdapter ##########################
###############################################################################


@pytest.fixture
def battery_state():
    return BatteryState(
        voltage=10.0,
        temperature=50.0,
        current=1.0,
        charge=None,
        capacity=100.0,
        design_capacity=210.0,
        percentage=20.0,
        power_supply_status=6,
        power_supply_health=7,
        power_supply_technology=8,
        present=True,
        location="car",
        serial_number="123456789",
        cell_voltage=None,
        cell_temperature=[4.0, 5.0, 6.0],
    )


@pytest.fixture
def battery_state_w_header(battery_state, ms_header):
    battery_state.header = ms_header
    return battery_state


@pytest.fixture
def battery_state_rosmsg(ros_header, battery_state: BatteryState):
    return ROSMessage(
        bag_timestamp_ns=100,
        topic="/battery_state",
        msg_type="sensor_msgs/msg/BatteryState",
        data={
            "header": ros_header,
            "voltage": battery_state.voltage,
            "temperature": battery_state.temperature or math.nan,
            "current": battery_state.current or math.nan,
            "charge": battery_state.charge or math.nan,
            "capacity": battery_state.capacity or math.nan,
            "design_capacity": battery_state.design_capacity or math.nan,
            "percentage": battery_state.percentage,
            "power_supply_status": battery_state.power_supply_status,
            "power_supply_health": battery_state.power_supply_health,
            "power_supply_technology": battery_state.power_supply_technology,
            "present": battery_state.present,
            "cell_voltage": battery_state.cell_voltage or [],
            "cell_temperature": battery_state.cell_temperature or [],
            "location": battery_state.location,
            "serial_number": battery_state.serial_number,
        },
    )


class TestBatteryStateAdapter:
    def test_translate_battery_state(self, battery_state_rosmsg: ROSMessage):
        ms_msg = BatteryStateAdapter.translate(battery_state_rosmsg)

        assert_battery_state(
            ms_msg.get_data(BatteryState), battery_state_rosmsg.data_field
        )

    def test_translate_raise_missing_required_key(
        self, battery_state_rosmsg: ROSMessage
    ):
        data = battery_state_rosmsg.data_field
        data.pop("voltage")
        with pytest.raises(ValueError, match="missing required keys"):
            BatteryStateAdapter.from_dict(data)

    @pytest.mark.parametrize("typestore", ROS_TYPESTORE_TO_TEST)
    def test_to_ros_battery_state(
        self, battery_state: BatteryState, typestore: Typestore
    ):
        ros_msg = BatteryStateAdapter.to_ros(
            battery_state, typestore, "sensor_msgs/msg/BatteryState"
        )

        assert_battery_state(battery_state, asdict(ros_msg))

    @pytest.mark.parametrize("typestore", ROS_TYPESTORE_TO_TEST)
    def test_to_ros_battery_state_w_header(
        self, battery_state_w_header: BatteryState, typestore: Typestore
    ):
        ros_msg = BatteryStateAdapter.to_ros(
            battery_state_w_header, typestore, "sensor_msgs/msg/BatteryState"
        )

        assert_battery_state(battery_state_w_header, asdict(ros_msg))

    @pytest.mark.parametrize("typestore", ROS_TYPESTORE_TO_TEST)
    def test_to_ros_default_type(
        self, battery_state: BatteryState, typestore: Typestore
    ):
        ros_msg = BatteryStateAdapter.to_ros(battery_state, typestore)

        assert_battery_state(battery_state, asdict(ros_msg))

    def test_to_ros_invalid_rosmsg_type(self, battery_state: BatteryState):

        with pytest.raises(
            TypeError,
            match=f"Adapter {BatteryStateAdapter.__name__} does not support sensor_msgs/msg/Bogus",
        ):
            BatteryStateAdapter.to_ros(
                battery_state, get_typestore(Stores.LATEST), "sensor_msgs/msg/Bogus"
            )

    def test_to_ros_invalid_mosaico_type(self, invalid_ms_msg):
        with pytest.raises(TypeError):
            BatteryStateAdapter.to_ros(invalid_ms_msg, get_typestore(Stores.LATEST))


###############################################################################
########################## TestRobotJointAdapter ##############################
###############################################################################


@pytest.fixture
def robot_joint():
    return RobotJoint(
        names=["joint1", "joint2"],
        positions=[0.0, 1.57],
        velocities=[0.1, 0.2],
        efforts=[10.0, 20.0],
    )


@pytest.fixture
def robot_joint_w_header(robot_joint, ms_header):
    robot_joint.header = ms_header
    return robot_joint


@pytest.fixture
def robot_joint_rosmsg(ros_header, robot_joint: RobotJoint):
    return ROSMessage(
        bag_timestamp_ns=100,
        topic="/joint_states",
        msg_type="sensor_msgs/msg/JointState",
        data={
            "header": ros_header,
            "name": robot_joint.names,
            "position": robot_joint.positions,
            "velocity": robot_joint.velocities,
            "effort": robot_joint.efforts,
        },
    )


class TestRobotJointAdapter:
    def test_translate_robot_joint(self, robot_joint_rosmsg: ROSMessage):
        ms_msg = RobotJointAdapter.translate(robot_joint_rosmsg)

        assert_robot_joint(ms_msg.get_data(RobotJoint), robot_joint_rosmsg.data_field)

    def test_translate_raise_missing_required_key(self, robot_joint_rosmsg: ROSMessage):
        data = robot_joint_rosmsg.data_field
        data.pop("name")
        with pytest.raises(ValueError, match="missing required keys"):
            RobotJointAdapter.from_dict(data)

    @pytest.mark.parametrize("typestore", ROS_TYPESTORE_TO_TEST)
    def test_to_ros_joint_state(self, robot_joint: RobotJoint, typestore: Typestore):
        ros_msg = RobotJointAdapter.to_ros(
            robot_joint, typestore, "sensor_msgs/msg/JointState"
        )
        assert_robot_joint(robot_joint, asdict(ros_msg))

    @pytest.mark.parametrize("typestore", ROS_TYPESTORE_TO_TEST)
    def test_to_ros_joint_state_w_header(
        self, robot_joint_w_header: RobotJoint, typestore: Typestore
    ):
        ros_msg = RobotJointAdapter.to_ros(
            robot_joint_w_header, typestore, "sensor_msgs/msg/JointState"
        )

        assert_robot_joint(robot_joint_w_header, asdict(ros_msg))

    @pytest.mark.parametrize("typestore", ROS_TYPESTORE_TO_TEST)
    def test_to_ros_default_type(self, robot_joint: RobotJoint, typestore: Typestore):
        ros_msg = RobotJointAdapter.to_ros(robot_joint, typestore)
        assert_robot_joint(robot_joint, asdict(ros_msg))

    def test_to_ros_invalid_rosmsg_type(self, robot_joint: RobotJoint):

        with pytest.raises(
            TypeError,
            match=f"Adapter {RobotJointAdapter.__name__} does not support sensor_msgs/msg/Bogus",
        ):
            RobotJointAdapter.to_ros(
                robot_joint, get_typestore(Stores.LATEST), "sensor_msgs/msg/Bogus"
            )

    def test_to_ros_invalid_mosaico_type(self, invalid_ms_msg):
        with pytest.raises(TypeError):
            RobotJointAdapter.to_ros(invalid_ms_msg, get_typestore(Stores.LATEST))


###############################################################################
############################# TestOverrideAdapter #############################
###############################################################################


def lidar():
    return futures.Lidar(
        x=list(range(0, 5)),
        y=list(range(5, 10)),
        z=list(range(10, 15)),
        intensity=list(range(15, 20)),
        reflectivity=list(range(20, 25)),
        beam_id=list(range(25, 30)),
        range=list(range(30, 35)),
        near_ir=list(range(35, 40)),
        azimuth=list(range(40, 45)),
        elevation=list(range(45, 50)),
        confidence=list(range(50, 55)),
        return_type=list(range(55, 60)),
        point_timestamp=list(range(60, 65)),
    )


# As of now, we do not have data to test starting from ROS data. Therefore, we use the encode() data to create the ROSMessage
def lidar_pcl():
    encoded = LidarAdapter.encode(lidar().model_dump(exclude_none=True))
    return ROSMessage(
        bag_timestamp_ns=100,
        topic="/lidar",
        msg_type="sensor_msgs/msg/PointCloud2",
        data=encoded,
    )


def radar():
    return futures.Radar(
        x=list(range(0, 5)),
        y=list(range(5, 10)),
        z=list(range(10, 15)),
        range=list(range(15, 20)),
        azimuth=list(range(20, 25)),
        elevation=list(range(25, 30)),
        rcs=list(range(30, 35)),
        snr=list(range(35, 40)),
        doppler_velocity=list(range(40, 45)),
        vx=list(range(45, 50)),
        vy=list(range(50, 55)),
        vx_comp=list(range(55, 60)),
        vy_comp=list(range(60, 65)),
        ax=list(range(65, 70)),
        ay=list(range(70, 75)),
        radial_speed=list(range(75, 80)),
    )


# As of now, we do not have data to test starting from ROS data. Therefore, we use the encode() data to create the ROSMessage
def radar_pcl():
    encoded = RadarAdapter.encode(radar().model_dump(exclude_none=True))
    return ROSMessage(
        bag_timestamp_ns=100,
        topic="/radar",
        msg_type="sensor_msgs/msg/PointCloud2",
        data=encoded,
    )


def rgbd_camera():
    return futures.RGBDCamera(
        x=list(range(0, 5)),
        y=list(range(5, 10)),
        z=list(range(10, 15)),
        rgb=list(range(15, 20)),
        intensity=list(range(20, 25)),
    )


# As of now, we do not have data to test starting from ROS data. Therefore, we use the encode() data to create the ROSMessage
def rgbd_camera_pcl():
    encoded = RGBDCameraAdapter.encode(rgbd_camera().model_dump(exclude_none=True))
    return ROSMessage(
        bag_timestamp_ns=100,
        topic="/rgbd",
        msg_type="sensor_msgs/msg/PointCloud2",
        data=encoded,
    )


def tof_camera():
    return futures.ToFCamera(
        x=list(range(0, 5)),
        y=list(range(5, 10)),
        z=list(range(10, 15)),
        rgb=list(range(15, 20)),
        intensity=list(range(20, 25)),
        noise=list(range(25, 30)),
        grayscale=list(range(30, 35)),
    )


# As of now, we do not have data to test starting from ROS data. Therefore, we use the encode() data to create the ROSMessage
def tof_camera_pcl():
    encoded = ToFCameraAdapter.encode(tof_camera().model_dump(exclude_none=True))
    return ROSMessage(
        bag_timestamp_ns=100,
        topic="/tof",
        msg_type="sensor_msgs/msg/PointCloud2",
        data=encoded,
    )


def stereo_camera():
    return futures.StereoCamera(
        x=list(range(0, 5)),
        y=list(range(5, 10)),
        z=list(range(10, 15)),
        rgb=list(range(15, 20)),
        intensity=list(range(20, 25)),
        luma=list(range(25, 30)),
        cost=list(range(30, 35)),
    )


# As of now, we do not have data to test starting from ROS data. Therefore, we use the encode() data to create the ROSMessage
def stereo_camera_pcl():
    encoded = StereoCameraAdapter.encode(stereo_camera().model_dump(exclude_none=True))
    return ROSMessage(
        bag_timestamp_ns=100,
        topic="/stereo",
        msg_type="sensor_msgs/msg/PointCloud2",
        data=encoded,
    )


PCL_ADAPTER_PAIR = [
    (lidar(), LidarAdapter),
    (radar(), RadarAdapter),
    (rgbd_camera(), RGBDCameraAdapter),
    (tof_camera(), ToFCameraAdapter),
    (stereo_camera(), StereoCameraAdapter),
]


PCL_ROSPCL_ADAPTER = [
    (lidar(), lidar_pcl(), LidarAdapter),
    (radar(), radar_pcl(), RadarAdapter),
    (rgbd_camera(), rgbd_camera_pcl(), RGBDCameraAdapter),
    (tof_camera(), tof_camera_pcl(), ToFCameraAdapter),
    (stereo_camera(), stereo_camera_pcl(), StereoCameraAdapter),
]


def lidar_message(lidar):
    return Message(data=lidar, timestamp_ns=10, frame_id="base_link")


def radar_message(radar):
    return Message(data=radar, timestamp_ns=10, frame_id="base_link")


def rgbd_camera_message(rgbd_camera):
    return Message(data=rgbd_camera, timestamp_ns=10, frame_id="camera_link")


def tof_camera_message(tof_camera):
    return Message(data=tof_camera, timestamp_ns=10, frame_id="camera_link")


def stereo_camera_message(stereo_camera):
    return Message(data=stereo_camera, timestamp_ns=10, frame_id="camera_link")


MESSAGE_ADAPTER_PAIR = [
    (lidar_message(lidar()), LidarAdapter),
    (radar_message(radar()), RadarAdapter),
    (rgbd_camera_message(rgbd_camera()), RGBDCameraAdapter),
    (tof_camera_message(tof_camera()), ToFCameraAdapter),
    (stereo_camera_message(stereo_camera()), StereoCameraAdapter),
]


class TestOverrideAdapter:
    def assert_pcl(self, adapter: PointCloudAdapterBase, pcl: Serializable, ros_msg):
        pcl_model = pcl.model_dump(exclude_none=True)

        assert [f.name for f in ros_msg.fields] == list(pcl_model.keys())

        # Round-trip: recreate Mosaico PointCloud2 message from ros message
        assert pcl == adapter.from_dict(
            {
                "height": ros_msg.height,
                "width": ros_msg.width,
                "fields": [
                    {
                        "name": f.name,
                        "offset": f.offset,
                        "datatype": f.datatype,
                        "count": f.count,
                    }
                    for f in ros_msg.fields
                ],
                "is_bigendian": ros_msg.is_bigendian,
                "point_step": ros_msg.point_step,
                "row_step": ros_msg.row_step,
                "data": bytes(ros_msg.data),
                "is_dense": ros_msg.is_dense,
            }
        )

    @pytest.mark.parametrize("ms_pcl, ros_pcl, adapter", PCL_ROSPCL_ADAPTER)
    def test_translate_override_adapter(
        self, ms_pcl, ros_pcl: ROSMessage, adapter: PointCloudAdapterBase
    ):
        ms_msg = adapter.translate(ros_pcl)

        assert ms_msg.data == ms_pcl

    @pytest.mark.parametrize("typestore", ROS_TYPESTORE_TO_TEST)
    @pytest.mark.parametrize("pcl, adapter", PCL_ADAPTER_PAIR)
    def test_to_ros_pointcloud2(self, pcl, adapter: PointCloudAdapterBase, typestore):
        ros_msg = adapter.to_ros(pcl, typestore, "sensor_msgs/msg/PointCloud2")
        self.assert_pcl(adapter, pcl, ros_msg)

    @pytest.mark.parametrize("typestore", ROS_TYPESTORE_TO_TEST)
    @pytest.mark.parametrize("message, adapter", MESSAGE_ADAPTER_PAIR)
    def test_to_ros_pointcloud2_message(
        self, message: Message, adapter: PointCloudAdapterBase, typestore: Typestore
    ):
        pcl = message.get_data(adapter.__mosaico_ontology_type__)
        ros_msg = adapter.to_ros(message, typestore, "sensor_msgs/msg/PointCloud2")

        self.assert_pcl(adapter, pcl, ros_msg)

    @pytest.mark.parametrize("typestore", ROS_TYPESTORE_TO_TEST)
    @pytest.mark.parametrize("pcl, adapter", PCL_ADAPTER_PAIR)
    def test_to_ros_default_type(
        self, pcl, adapter: PointCloudAdapterBase, typestore: Typestore
    ):
        ros_msg = adapter.to_ros(pcl, typestore)
        self.assert_pcl(adapter, pcl, ros_msg)

    @pytest.mark.parametrize("pcl, adapter", PCL_ADAPTER_PAIR)
    def test_to_ros_invalid_rosmsg_type(self, pcl, adapter):

        with pytest.raises(
            TypeError,
            match=f"Adapter {adapter.__name__} does not support sensor_msgs/msg/Bogus",
        ):
            adapter.to_ros(pcl, get_typestore(Stores.LATEST), "sensor_msgs/msg/Bogus")

    @pytest.mark.parametrize("pcl, adapter", PCL_ADAPTER_PAIR)
    def test_to_ros_invalid_mosaico_type(self, invalid_ms_msg, pcl, adapter):
        with pytest.raises(TypeError):
            adapter.to_ros(invalid_ms_msg, get_typestore(Stores.LATEST))


###############################################################################
########################### TestPointCloud2Adapter ############################
###############################################################################


@pytest.fixture
def pcl2():
    return PointCloud2(
        height=10,
        width=20,
        fields=[
            PointField(
                name="x", offset=0, datatype=PointFieldDataType.FLOAT32, count=1
            ),
            PointField(
                name="y", offset=4, datatype=PointFieldDataType.FLOAT32, count=1
            ),
            PointField(
                name="z", offset=8, datatype=PointFieldDataType.FLOAT32, count=1
            ),
        ],
        is_bigendian=sys.byteorder == "big",
        point_step=12,  # 3 points * 4 bytes
        row_step=240,  # 12 * width
        data=bytes([10] * 2400),
        is_dense=True,
    )


@pytest.fixture
def pcl2_rosmsg(ros_header, pcl2: PointCloud2):
    return ROSMessage(
        bag_timestamp_ns=100,
        topic="/pointcloud2",
        msg_type="sensor_msgs/msg/PointCloud2",
        data={
            "header": ros_header,
            "height": pcl2.height,
            "width": pcl2.width,
            "fields": [
                {
                    "name": field.name,
                    "offset": field.offset,
                    "datatype": field.datatype,
                    "count": field.count,
                }
                for field in pcl2.fields
            ],
            "is_bigendian": pcl2.is_bigendian,
            "point_step": pcl2.point_step,
            "row_step": pcl2.row_step,
            "data": list(pcl2.data),
            "is_dense": pcl2.is_dense,
        },
    )


@pytest.fixture
def pcl2_w_header(pcl2, ms_header):
    pcl2.header = ms_header
    return pcl2


class TestPointCloud2Adapter:
    def test_translate_pcl2(self, pcl2_rosmsg: ROSMessage):
        ms_msg = PointCloudAdapter.translate(pcl2_rosmsg)

        assert_pcl2(ms_msg.get_data(PointCloud2), pcl2_rosmsg.data_field)

    def test_translate_raise_missing_required_key(self, pcl2_rosmsg: ROSMessage):
        data = pcl2_rosmsg.data_field
        data.pop("width")
        with pytest.raises(ValueError, match="missing required keys"):
            PointCloudAdapter.from_dict(data)

    @pytest.mark.parametrize("typestore", ROS_TYPESTORE_TO_TEST)
    def test_to_ros_pointcloud2(self, pcl2: PointCloud2, typestore: Typestore):
        ros_msg = PointCloudAdapter.to_ros(
            pcl2, typestore, "sensor_msgs/msg/PointCloud2"
        )
        assert_pcl2(pcl2, asdict(ros_msg))

    @pytest.mark.parametrize("typestore", ROS_TYPESTORE_TO_TEST)
    def test_to_ros_pointcloud2_message(
        self, pcl2_w_header: PointCloud2, typestore: Typestore
    ):
        ros_msg = PointCloudAdapter.to_ros(
            pcl2_w_header, typestore, "sensor_msgs/msg/PointCloud2"
        )
        assert_pcl2(pcl2_w_header, asdict(ros_msg))

    @pytest.mark.parametrize("typestore", ROS_TYPESTORE_TO_TEST)
    def test_to_ros_default_type(self, pcl2: PointCloud2, typestore: Typestore):
        ros_msg = PointCloudAdapter.to_ros(pcl2, typestore)
        assert_pcl2(pcl2, asdict(ros_msg))

    def test_to_ros_invalid_rosmsg_type(self, pcl2: PointCloud2):

        with pytest.raises(
            TypeError,
            match=f"Adapter {PointCloudAdapter.__name__} does not support sensor_msgs/msg/Bogus",
        ):
            PointCloudAdapter.to_ros(
                pcl2, get_typestore(Stores.LATEST), "sensor_msgs/msg/Bogus"
            )

    def test_to_ros_invalid_mosaico_type(self, invalid_ms_msg):
        with pytest.raises(TypeError):
            PointCloudAdapter.to_ros(invalid_ms_msg, get_typestore(Stores.LATEST))


###############################################################################
############################# TestLaserScanAdapter ############################
###############################################################################


@pytest.fixture
def laserscan():
    return futures.LaserScan(
        angle_min=-1.57,
        angle_max=1.57,
        angle_increment=0.01,
        time_increment=0.0,
        scan_time=0.1,
        range_min=0.2,
        range_max=10.0,
        ranges=list(np.array([1.1, 2.1, 3.1], dtype=np.float32)),
        intensities=list(np.array([100.0, 200.0, 300.0], dtype=np.float32)),
    )


@pytest.fixture
def laserscan_rosmsg(ros_header, laserscan: futures.LaserScan):
    return ROSMessage(
        bag_timestamp_ns=100,
        topic="/laserscan",
        msg_type="sensor_msgs/msg/LaserScan",
        data={
            "header": ros_header,
            "angle_min": laserscan.angle_min,
            "angle_max": laserscan.angle_max,
            "angle_increment": laserscan.angle_increment,
            "time_increment": laserscan.time_increment,
            "scan_time": laserscan.scan_time,
            "range_min": laserscan.range_min,
            "range_max": laserscan.range_max,
            "ranges": laserscan.ranges,
            "intensities": laserscan.intensities,
        },
    )


@pytest.fixture
def laserscan_w_header(laserscan, ms_header):
    laserscan.header = ms_header
    return laserscan


class TestLaserScannerAdapter:
    def test_translate_laser_scanner(self, laserscan_rosmsg: ROSMessage):
        ms_msg = LaserScanAdapter.translate(laserscan_rosmsg)

        assert_laserscan(
            ms_msg.get_data(futures.LaserScan), laserscan_rosmsg.data_field
        )

    def test_translate_raise_missing_required_key(self, laserscan_rosmsg: ROSMessage):
        data = laserscan_rosmsg.data_field
        data.pop("angle_min")
        with pytest.raises(ValueError, match="missing required keys"):
            LaserScanAdapter.from_dict(data)

    @pytest.mark.parametrize("typestore", ROS_TYPESTORE_TO_TEST)
    def test_to_ros_laserscan(self, laserscan: futures.LaserScan, typestore: Typestore):
        ros_msg = LaserScanAdapter.to_ros(
            laserscan, typestore, "sensor_msgs/msg/LaserScan"
        )
        assert_laserscan(laserscan, asdict(ros_msg))

    @pytest.mark.parametrize("typestore", ROS_TYPESTORE_TO_TEST)
    def test_to_ros_laserscan_message(
        self, laserscan_w_header: futures.LaserScan, typestore: Typestore
    ):
        ros_msg = LaserScanAdapter.to_ros(
            laserscan_w_header, typestore, "sensor_msgs/msg/LaserScan"
        )

        assert_laserscan(laserscan_w_header, asdict(ros_msg))

    @pytest.mark.parametrize("typestore", ROS_TYPESTORE_TO_TEST)
    def test_to_ros_default_type(
        self, laserscan: futures.LaserScan, typestore: Typestore
    ):
        ros_msg = LaserScanAdapter.to_ros(laserscan, typestore)
        assert_laserscan(laserscan, asdict(ros_msg))

    def test_to_ros_invalid_rosmsg_type(self, laserscan: futures.LaserScan):
        with pytest.raises(
            TypeError,
            match=f"Adapter {LaserScanAdapter.__name__} does not support sensor_msgs/msg/Bogus",
        ):
            LaserScanAdapter.to_ros(
                laserscan, get_typestore(Stores.LATEST), "sensor_msgs/msg/Bogus"
            )

    def test_to_ros_invalid_mosaico_type(self, invalid_ms_msg):
        with pytest.raises(TypeError):
            LaserScanAdapter.to_ros(invalid_ms_msg, get_typestore(Stores.LATEST))


###############################################################################
######################## TestMultiEchoLaserScanAdapter ########################
###############################################################################


@pytest.fixture
def multiecho_laserscan():
    return futures.MultiEchoLaserScan(
        angle_min=-1.57,
        angle_max=1.57,
        angle_increment=0.01,
        time_increment=0.0,
        scan_time=0.1,
        range_min=0.2,
        range_max=10.0,
        ranges=list(np.array([[1.0, 1.1], [2.0, 2.1], [3.0, 3.1]], dtype=np.float32)),
        intensities=list(
            np.array([[100.0, 110.0], [200.0, 210.0], [300.0, 310.0]], dtype=np.float32)
        ),
    )


@pytest.fixture
def multiecho_laserscan_rosmsg(
    ros_header, multiecho_laserscan: futures.MultiEchoLaserScan
):
    return ROSMessage(
        bag_timestamp_ns=100,
        topic="/multiecho_laserscan",
        msg_type="sensor_msgs/msg/MultiEchoLaserScan",
        data={
            "header": ros_header,
            "angle_min": multiecho_laserscan.angle_min,
            "angle_max": multiecho_laserscan.angle_max,
            "angle_increment": multiecho_laserscan.angle_increment,
            "time_increment": multiecho_laserscan.time_increment,
            "scan_time": multiecho_laserscan.scan_time,
            "range_min": multiecho_laserscan.range_min,
            "range_max": multiecho_laserscan.range_max,
            "ranges": [{"echoes": x} for x in multiecho_laserscan.ranges],
            "intensities": [{"echoes": x} for x in multiecho_laserscan.intensities],
        },
    )


@pytest.fixture
def multiecho_laserscan_w_header(multiecho_laserscan, ms_header):
    multiecho_laserscan.header = ms_header
    return multiecho_laserscan


class TestMultiEchoLaserScanAdapter:
    def test_translate_multi_echo_laser_scanner(
        self, multiecho_laserscan_rosmsg: ROSMessage
    ):
        ms_msg = MultiEchoLaserScanAdapter.translate(multiecho_laserscan_rosmsg)

        assert_multiecho_laserscan(
            ms_msg.get_data(futures.MultiEchoLaserScan),
            multiecho_laserscan_rosmsg.data_field,
        )

    def test_translate_raise_missing_required_key(
        self, multiecho_laserscan_rosmsg: ROSMessage
    ):
        data = multiecho_laserscan_rosmsg.data_field
        data.pop("angle_min")
        with pytest.raises(ValueError, match="missing required keys"):
            MultiEchoLaserScanAdapter.from_dict(data)

    @pytest.mark.parametrize("typestore", ROS_TYPESTORE_TO_TEST)
    def test_to_ros_multiecho_laserscan(
        self, multiecho_laserscan: futures.MultiEchoLaserScan, typestore: Typestore
    ):
        ros_msg = MultiEchoLaserScanAdapter.to_ros(
            multiecho_laserscan, typestore, "sensor_msgs/msg/MultiEchoLaserScan"
        )
        assert_multiecho_laserscan(multiecho_laserscan, asdict(ros_msg))

    @pytest.mark.parametrize("typestore", ROS_TYPESTORE_TO_TEST)
    def test_to_ros_multiecho_laserscan_message(
        self,
        multiecho_laserscan_w_header: futures.MultiEchoLaserScan,
        typestore: Typestore,
    ):
        ros_msg = MultiEchoLaserScanAdapter.to_ros(
            multiecho_laserscan_w_header,
            typestore,
            "sensor_msgs/msg/MultiEchoLaserScan",
        )

        assert_multiecho_laserscan(multiecho_laserscan_w_header, asdict(ros_msg))

    @pytest.mark.parametrize("typestore", ROS_TYPESTORE_TO_TEST)
    def test_to_ros_default_type(
        self, multiecho_laserscan: futures.MultiEchoLaserScan, typestore: Typestore
    ):
        ros_msg = MultiEchoLaserScanAdapter.to_ros(multiecho_laserscan, typestore)
        assert_multiecho_laserscan(multiecho_laserscan, asdict(ros_msg))

    def test_to_ros_invalid_rosmsg_type(
        self, multiecho_laserscan: futures.MultiEchoLaserScan
    ):
        with pytest.raises(
            TypeError,
            match=f"Adapter {MultiEchoLaserScanAdapter.__name__} does not support sensor_msgs/msg/Bogus",
        ):
            MultiEchoLaserScanAdapter.to_ros(
                multiecho_laserscan,
                get_typestore(Stores.LATEST),
                "sensor_msgs/msg/Bogus",
            )

    def test_to_ros_invalid_mosaico_type(self, invalid_ms_msg):
        with pytest.raises(TypeError):
            MultiEchoLaserScanAdapter.to_ros(
                invalid_ms_msg, get_typestore(Stores.LATEST)
            )


###############################################################################
############################## TestJoyAdapter #################################
###############################################################################


@pytest.fixture
def joy():
    return Joy(
        axes=[0.0, -1.0, 0.5],
        buttons=[0, 1, 0, 1],
    )


@pytest.fixture
def joy_w_header(joy, ms_header):
    joy.header = ms_header
    return joy


@pytest.fixture
def joy_rosmsg(ros_header, joy: Joy):
    return ROSMessage(
        bag_timestamp_ns=100,
        topic="/joy",
        msg_type="sensor_msgs/msg/Joy",
        data={
            "header": ros_header,
            "axes": np.asarray(joy.axes, dtype=np.float32),
            "buttons": np.asarray(joy.buttons, dtype=np.int32),
        },
    )


class TestJoyAdapter:
    def test_translate_joy(self, joy_rosmsg: ROSMessage):
        ms_msg = JoyAdapter.translate(joy_rosmsg)

        assert_joy(ms_msg.get_data(Joy), joy_rosmsg.data_field)

    def test_translate_raise_missing_required_key(self, joy_rosmsg: ROSMessage):
        data = joy_rosmsg.data_field
        data.pop("axes")
        with pytest.raises(ValueError, match="missing required keys"):
            JoyAdapter.from_dict(data)

    @pytest.mark.parametrize("typestore", ROS_TYPESTORE_TO_TEST)
    def test_to_ros_joy(self, joy: Joy, typestore: Typestore):
        ros_msg = JoyAdapter.to_ros(joy, typestore, "sensor_msgs/msg/Joy")

        assert_joy(joy, asdict(ros_msg))

    @pytest.mark.parametrize("typestore", ROS_TYPESTORE_TO_TEST)
    def test_to_ros_joy_message(self, joy_w_header: Joy, typestore: Typestore):
        ros_msg = JoyAdapter.to_ros(joy_w_header, typestore, "sensor_msgs/msg/Joy")

        assert_joy(joy_w_header, asdict(ros_msg))

    @pytest.mark.parametrize("typestore", ROS_TYPESTORE_TO_TEST)
    def test_to_ros_default_type(self, joy: Joy, typestore: Typestore):
        ros_msg = JoyAdapter.to_ros(joy, typestore)
        assert_joy(joy, asdict(ros_msg))

    def test_to_ros_invalid_rosmsg_type(self, joy: Joy):

        with pytest.raises(
            TypeError,
            match=f"Adapter {JoyAdapter.__name__} does not support sensor_msgs/msg/Bogus",
        ):
            JoyAdapter.to_ros(
                joy, get_typestore(Stores.LATEST), "sensor_msgs/msg/Bogus"
            )

    def test_to_ros_invalid_mosaico_type(self, invalid_ms_msg):
        with pytest.raises(TypeError):
            JoyAdapter.to_ros(invalid_ms_msg, get_typestore(Stores.LATEST))


###############################################################################
########################## TestMagneticFieldAdapter ###########################
###############################################################################


@pytest.fixture
def magnetometer():
    return Magnetometer(magnetic_field=Vector3d(x=0.12, y=-0.05, z=0.98))


@pytest.fixture
def magnetometer_w_cov():
    return Magnetometer(
        magnetic_field=Vector3d(x=0.12, y=-0.05, z=0.98, covariance=range(0, 9))
    )


@pytest.fixture
def magnetometer_w_header(magnetometer, ms_header):
    magnetometer.header = ms_header
    return magnetometer


@pytest.fixture
def magnetometer_rosmsg(ros_header, magnetometer: Magnetometer):
    return ROSMessage(
        bag_timestamp_ns=100,
        topic="/magnetic_field",
        msg_type="sensor_msgs/msg/MagneticField",
        data={
            "header": ros_header,
            "magnetic_field": {
                "x": magnetometer.magnetic_field.x,
                "y": magnetometer.magnetic_field.y,
                "z": magnetometer.magnetic_field.z,
            },
            "magnetic_field_covariance": [0.0] * 9,
        },
    )


@pytest.fixture
def magnetometer_w_cov_rosmsg(ros_header, magnetometer_w_cov: Magnetometer):
    return ROSMessage(
        bag_timestamp_ns=100,
        topic="/magnetic_field",
        msg_type="sensor_msgs/msg/MagneticField",
        data={
            "header": ros_header,
            "magnetic_field": {
                "x": magnetometer_w_cov.magnetic_field.x,
                "y": magnetometer_w_cov.magnetic_field.y,
                "z": magnetometer_w_cov.magnetic_field.z,
            },
            "magnetic_field_covariance": list(range(9)),
        },
    )


class TestMagneticFieldAdapter:
    def test_translate_magnetometer(self, magnetometer_rosmsg: ROSMessage):
        ms_msg = MagneticFieldAdapter.translate(magnetometer_rosmsg)

        assert_magnetometer(
            ms_msg.get_data(Magnetometer), magnetometer_rosmsg.data_field
        )

    def test_translate_magnetometer_w_cov(self, magnetometer_w_cov_rosmsg: ROSMessage):
        ms_msg = MagneticFieldAdapter.translate(magnetometer_w_cov_rosmsg)

        assert_magnetometer(
            ms_msg.get_data(Magnetometer), magnetometer_w_cov_rosmsg.data_field
        )

    def test_translate_raise_missing_required_key(
        self, magnetometer_rosmsg: ROSMessage
    ):
        data = magnetometer_rosmsg.data_field
        data.pop("magnetic_field")
        with pytest.raises(ValueError, match="missing required keys"):
            MagneticFieldAdapter.from_dict(data)

    @pytest.mark.parametrize("typestore", ROS_TYPESTORE_TO_TEST)
    def test_to_ros_magnetic_field(
        self, magnetometer: Magnetometer, typestore: Typestore
    ):
        ros_msg = MagneticFieldAdapter.to_ros(
            magnetometer, typestore, "sensor_msgs/msg/MagneticField"
        )
        assert_magnetometer(magnetometer, asdict(ros_msg))

    @pytest.mark.parametrize("typestore", ROS_TYPESTORE_TO_TEST)
    def test_to_ros_magnetic_field_w_cov(
        self, magnetometer_w_cov: Magnetometer, typestore: Typestore
    ):
        ros_msg = MagneticFieldAdapter.to_ros(
            magnetometer_w_cov, typestore, "sensor_msgs/msg/MagneticField"
        )
        assert_magnetometer(magnetometer_w_cov, asdict(ros_msg))

    @pytest.mark.parametrize("typestore", ROS_TYPESTORE_TO_TEST)
    def test_to_ros_magnetic_field_message(
        self, magnetometer_w_header: Magnetometer, typestore: Typestore
    ):
        ros_msg = MagneticFieldAdapter.to_ros(
            magnetometer_w_header, typestore, "sensor_msgs/msg/MagneticField"
        )

        assert_magnetometer(magnetometer_w_header, asdict(ros_msg))

    @pytest.mark.parametrize("typestore", ROS_TYPESTORE_TO_TEST)
    def test_to_ros_default_type(
        self, magnetometer: Magnetometer, typestore: Typestore
    ):
        ros_msg = MagneticFieldAdapter.to_ros(magnetometer, typestore)
        assert_magnetometer(magnetometer, asdict(ros_msg))

    def test_to_ros_invalid_rosmsg_type(self, magnetometer: Magnetometer):

        with pytest.raises(
            TypeError,
            match=f"Adapter {MagneticFieldAdapter.__name__} does not support sensor_msgs/msg/Bogus",
        ):
            MagneticFieldAdapter.to_ros(
                magnetometer, get_typestore(Stores.LATEST), "sensor_msgs/msg/Bogus"
            )

    def test_to_ros_invalid_mosaico_type(self, invalid_ms_msg):
        with pytest.raises(TypeError):
            MagneticFieldAdapter.to_ros(invalid_ms_msg, get_typestore(Stores.LATEST))


###############################################################################
########################## TestTemperatureAdapter #############################
###############################################################################


@pytest.fixture
def temperature():
    return Temperature(
        value=300.0,  # Kelvin
    )


@pytest.fixture
def temperature_w_var():
    return Temperature(value=300.0, variance=5.0)


@pytest.fixture
def temperature_w_header(temperature, ms_header):
    temperature.header = ms_header
    return temperature


@pytest.fixture
def temperature_rosmsg(ros_header, temperature: Temperature):
    return ROSMessage(
        bag_timestamp_ns=100,
        topic="/temperature",
        msg_type="sensor_msgs/msg/Temperature",
        data={
            "header": ros_header,
            "temperature": temperature.to_celsius(),
            "variance": 0.0,
        },
    )


@pytest.fixture
def temperature_w_var_rosmsg(ros_header, temperature_w_var: Temperature):
    return ROSMessage(
        bag_timestamp_ns=100,
        topic="/temperature",
        msg_type="sensor_msgs/msg/Temperature",
        data={
            "header": ros_header,
            "temperature": temperature_w_var.to_celsius(),
            "variance": temperature_w_var.variance,
        },
    )


class TestTemperatureAdapter:
    def test_translate_temperature(self, temperature_rosmsg: ROSMessage):
        ms_msg = TemperatureAdapter.translate(temperature_rosmsg)

        assert_temperature(ms_msg.get_data(Temperature), temperature_rosmsg.data_field)

    def test_translate_temperature_w_var(self, temperature_w_var_rosmsg: ROSMessage):
        ms_msg = TemperatureAdapter.translate(temperature_w_var_rosmsg)

        assert_temperature(
            ms_msg.get_data(Temperature), temperature_w_var_rosmsg.data_field
        )

    def test_translate_raise_missing_required_key(self, temperature_rosmsg: ROSMessage):
        data = temperature_rosmsg.data_field
        data.pop("temperature")
        with pytest.raises(ValueError, match="missing required keys"):
            TemperatureAdapter.from_dict(data)

    @pytest.mark.parametrize("typestore", ROS_TYPESTORE_TO_TEST)
    def test_to_ros_temperature(self, temperature: Temperature, typestore: Typestore):
        ros_msg = TemperatureAdapter.to_ros(
            temperature, typestore, "sensor_msgs/msg/Temperature"
        )

        assert_temperature(temperature, asdict(ros_msg))

    @pytest.mark.parametrize("typestore", ROS_TYPESTORE_TO_TEST)
    def test_to_ros_temperature_w_var(
        self, temperature_w_var: Temperature, typestore: Typestore
    ):
        ros_msg = TemperatureAdapter.to_ros(
            temperature_w_var, typestore, "sensor_msgs/msg/Temperature"
        )

        assert_temperature(temperature_w_var, asdict(ros_msg))

    @pytest.mark.parametrize("typestore", ROS_TYPESTORE_TO_TEST)
    def test_to_ros_temperature_message(
        self, temperature_w_header: Temperature, typestore: Typestore
    ):
        ros_msg = TemperatureAdapter.to_ros(
            temperature_w_header, typestore, "sensor_msgs/msg/Temperature"
        )

        assert_temperature(temperature_w_header, asdict(ros_msg))

    @pytest.mark.parametrize("typestore", ROS_TYPESTORE_TO_TEST)
    def test_to_ros_default_type(self, temperature: Temperature, typestore: Typestore):
        ros_msg = TemperatureAdapter.to_ros(temperature, typestore)
        assert_temperature(temperature, asdict(ros_msg))

    def test_to_ros_invalid_rosmsg_type(self, temperature: Temperature):
        with pytest.raises(
            TypeError,
            match=f"Adapter {TemperatureAdapter.__name__} does not support sensor_msgs/msg/Bogus",
        ):
            TemperatureAdapter.to_ros(
                temperature, get_typestore(Stores.LATEST), "sensor_msgs/msg/Bogus"
            )

    def test_to_ros_invalid_mosaico_type(self, invalid_ms_msg):
        with pytest.raises(TypeError):
            TemperatureAdapter.to_ros(invalid_ms_msg, get_typestore(Stores.LATEST))


###############################################################################
########################### TestPressureAdapter ###############################
###############################################################################


@pytest.fixture
def pressure():
    return Pressure(
        value=300.0,  # Pascal
    )


@pytest.fixture
def pressure_w_var():
    return Pressure(value=300.0, variance=5.0)


@pytest.fixture
def pressure_w_header(pressure, ms_header):
    pressure.header = ms_header
    return pressure


@pytest.fixture
def pressure_rosmsg(ros_header, pressure: Pressure):
    return ROSMessage(
        bag_timestamp_ns=100,
        topic="/pressure",
        msg_type="sensor_msgs/msg/FluidPressure",
        data={
            "header": ros_header,
            "fluid_pressure": pressure.value,
            "variance": 0.0,
        },
    )


@pytest.fixture
def pressure_w_var_rosmsg(ros_header, pressure_w_var: Pressure):
    return ROSMessage(
        bag_timestamp_ns=100,
        topic="/pressure",
        msg_type="sensor_msgs/msg/FluidPressure",
        data={
            "header": ros_header,
            "fluid_pressure": pressure_w_var.value,
            "variance": pressure_w_var.variance,
        },
    )


class TestpressureAdapter:
    def test_translate_pressure(self, pressure_rosmsg: ROSMessage):
        ms_msg = PressureAdapter.translate(pressure_rosmsg)

        assert_pressure(ms_msg.get_data(Pressure), pressure_rosmsg.data_field)

    def test_translate_pressure_w_var(self, pressure_w_var_rosmsg: ROSMessage):
        ms_msg = PressureAdapter.translate(pressure_w_var_rosmsg)

        assert_pressure(ms_msg.get_data(Pressure), pressure_w_var_rosmsg.data_field)

    def test_translate_raise_missing_required_key(self, pressure_rosmsg: ROSMessage):
        data = pressure_rosmsg.data_field
        data.pop("fluid_pressure")
        with pytest.raises(ValueError, match="missing required keys"):
            PressureAdapter.from_dict(data)

    @pytest.mark.parametrize("typestore", ROS_TYPESTORE_TO_TEST)
    def test_to_ros_pressure(self, pressure: pressure, typestore: Typestore):
        ros_msg = PressureAdapter.to_ros(
            pressure, typestore, "sensor_msgs/msg/FluidPressure"
        )
        assert_pressure(pressure, asdict(ros_msg))

    @pytest.mark.parametrize("typestore", ROS_TYPESTORE_TO_TEST)
    def test_to_ros_pressure_w_var(
        self, pressure_w_var: pressure, typestore: Typestore
    ):
        ros_msg = PressureAdapter.to_ros(
            pressure_w_var, typestore, "sensor_msgs/msg/FluidPressure"
        )
        assert_pressure(pressure_w_var, asdict(ros_msg))

    @pytest.mark.parametrize("typestore", ROS_TYPESTORE_TO_TEST)
    def test_to_ros_pressure_message(
        self, pressure_w_header: Pressure, typestore: Typestore
    ):
        ros_msg = PressureAdapter.to_ros(
            pressure_w_header, typestore, "sensor_msgs/msg/FluidPressure"
        )

        assert_pressure(pressure_w_header, asdict(ros_msg))

    @pytest.mark.parametrize("typestore", ROS_TYPESTORE_TO_TEST)
    def test_to_ros_default_type(self, pressure: pressure, typestore: Typestore):
        ros_msg = PressureAdapter.to_ros(pressure, typestore)
        assert_pressure(pressure, asdict(ros_msg))

    def test_to_ros_invalid_rosmsg_type(self, pressure: pressure):
        with pytest.raises(
            TypeError,
            match=f"Adapter {PressureAdapter.__name__} does not support sensor_msgs/msg/Bogus",
        ):
            PressureAdapter.to_ros(
                pressure, get_typestore(Stores.LATEST), "sensor_msgs/msg/Bogus"
            )

    def test_to_ros_invalid_mosaico_type(self, invalid_ms_msg):
        with pytest.raises(TypeError):
            TemperatureAdapter.to_ros(invalid_ms_msg, get_typestore(Stores.LATEST))
