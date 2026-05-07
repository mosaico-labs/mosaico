import numpy as np
import pytest
from rosbags.typesys import get_types_from_msg
from rosbags.typesys.stores import Stores, Typestore, get_typestore

from mosaicolabs import (
    GPS,
    IMU,
    ROI,
    CameraInfo,
    CompressedImage,
    GPSStatus,
    Image,
    ImageFormat,
    Message,
    NMEASentence,
    Point3d,
    Quaternion,
    RobotJoint,
    Serializable,
    Time,
    Vector2d,
    Vector3d,
)
from mosaicolabs.ros_bridge.adapters import (
    BatteryStateAdapter,
    CameraInfoAdapter,
    CompressedImageAdapter,
    GPSAdapter,
    ImageAdapter,
    IMUAdapter,
    NavSatStatusAdapter,
    NMEASentenceAdapter,
    RobotJointAdapter,
    ROIAdapter,
)
from mosaicolabs.ros_bridge.data_ontology import BatteryState

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
def nmea_sentence_msg(nmea_sentence):
    return Message(data=nmea_sentence, timestamp_ns=100, frame_id="satellite_link")


class TestNMEASentenceAdapter:
    def assert_nmea_sentence(self, nmea_sentence: NMEASentence, ros_msg):
        assert nmea_sentence.sentence == ros_msg.sentence

    @pytest.mark.parametrize("typestore", ROS_TYPESTORE_TO_TEST)
    def test_to_ros_nmea_sentence(
        self, nmea_sentence: NMEASentence, typestore: Typestore
    ):

        typestore = register_nmea_sentence(typestore)
        ros_msg = NMEASentenceAdapter.to_ros(
            nmea_sentence, typestore, "nmea_msgs/msg/Sentence"
        )
        self.assert_nmea_sentence(nmea_sentence, ros_msg)

    @pytest.mark.parametrize("typestore", ROS_TYPESTORE_TO_TEST)
    def test_to_ros_nmea_sentence_message(
        self, nmea_sentence_msg: Message, typestore: Typestore
    ):
        typestore = register_nmea_sentence(typestore)
        nmea_sentence = nmea_sentence_msg.get_data(NMEASentence)
        ros_msg = NMEASentenceAdapter.to_ros(
            nmea_sentence_msg, typestore, "nmea_msgs/msg/Sentence"
        )
        self.assert_nmea_sentence(nmea_sentence, ros_msg)

    @pytest.mark.parametrize("typestore", ROS_TYPESTORE_TO_TEST)
    def test_to_ros_default_type(
        self, nmea_sentence: NMEASentence, typestore: Typestore
    ):
        typestore = register_nmea_sentence(typestore)
        ros_msg = NMEASentenceAdapter.to_ros(nmea_sentence, typestore)
        self.assert_nmea_sentence(nmea_sentence, ros_msg)

    def test_to_ros_invalid_rosmsg_type(self, nmea_sentence: NMEASentence):
        ros_msg = NMEASentenceAdapter.to_ros(
            nmea_sentence, get_typestore(Stores.LATEST), "sensor_msgs/msg/Bogus"
        )
        assert ros_msg is None

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
def image_msg(image_raw):
    return Message(data=image_raw, timestamp_ns=100, frame_id="camera_link")


class TestImageAdapter:
    def assert_image(self, image: Image, ros_msg):
        assert image.height == ros_msg.height
        assert image.width == ros_msg.width
        assert image.encoding == ros_msg.encoding
        assert int(image.is_bigendian) == ros_msg.is_bigendian
        assert image.stride == ros_msg.step
        assert np.array_equal(np.frombuffer(image.data, dtype=np.uint8), ros_msg.data)

    @pytest.mark.parametrize("typestore", ROS_TYPESTORE_TO_TEST)
    def test_to_ros_image_raw(self, image_raw: Image, typestore: Typestore):
        ros_msg = ImageAdapter.to_ros(image_raw, typestore, "sensor_msgs/msg/Image")
        self.assert_image(image_raw, ros_msg)

    @pytest.mark.parametrize("typestore", ROS_TYPESTORE_TO_TEST)
    def test_to_ros_image_png(self, image_png: Image, typestore: Typestore):
        ros_msg = ImageAdapter.to_ros(image_png, typestore, "sensor_msgs/msg/Image")
        self.assert_image(image_png, ros_msg)

    @pytest.mark.parametrize("typestore", ROS_TYPESTORE_TO_TEST)
    def test_to_ros_image_message(self, image_msg: Message, typestore: Typestore):
        image = image_msg.get_data(Image)
        ros_msg = ImageAdapter.to_ros(image_msg, typestore, "sensor_msgs/msg/Image")
        assert image_msg.frame_id == ros_msg.header.frame_id
        assert (
            image_msg.timestamp_ns
            == Time(
                seconds=ros_msg.header.stamp.sec,
                nanoseconds=ros_msg.header.stamp.nanosec,
            ).to_nanoseconds()
        )
        self.assert_image(image, ros_msg)

    @pytest.mark.parametrize("typestore", ROS_TYPESTORE_TO_TEST)
    def test_to_ros_default_type(self, image_raw: Image, typestore: Typestore):
        ros_msg = ImageAdapter.to_ros(image_raw, typestore)
        self.assert_image(image_raw, ros_msg)

    def test_to_ros_invalid_rosmsg_type(self, image_raw: Image):
        ros_msg = ImageAdapter.to_ros(
            image_raw, get_typestore(Stores.LATEST), "sensor_msgs/msg/Bogus"
        )
        assert ros_msg is None

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
def compressed_image_msg(compressed_image):
    return Message(data=compressed_image, timestamp_ns=100, frame_id="camera_link")


class TestCompressedImageAdapter:
    def assert_compressed_image(self, compressed_image: CompressedImage, ros_msg):
        assert compressed_image.format == ros_msg.format
        assert np.array_equal(
            np.frombuffer(compressed_image.data, dtype=np.uint8), ros_msg.data
        )

    @pytest.mark.parametrize("typestore", ROS_TYPESTORE_TO_TEST)
    def test_to_ros_compressed_image(
        self, compressed_image: CompressedImage, typestore: Typestore
    ):
        ros_msg = CompressedImageAdapter.to_ros(
            compressed_image, typestore, "sensor_msgs/msg/CompressedImage"
        )
        self.assert_compressed_image(compressed_image, ros_msg)

    @pytest.mark.parametrize("typestore", ROS_TYPESTORE_TO_TEST)
    def test_to_ros_compressed_image_message(
        self, compressed_image_msg: Message, typestore: Typestore
    ):
        compressed_image = compressed_image_msg.get_data(CompressedImage)
        ros_msg = CompressedImageAdapter.to_ros(
            compressed_image_msg, typestore, "sensor_msgs/msg/CompressedImage"
        )
        assert compressed_image_msg.frame_id == ros_msg.header.frame_id
        assert (
            compressed_image_msg.timestamp_ns
            == Time(
                seconds=ros_msg.header.stamp.sec,
                nanoseconds=ros_msg.header.stamp.nanosec,
            ).to_nanoseconds()
        )
        self.assert_compressed_image(compressed_image, ros_msg)

    @pytest.mark.parametrize("typestore", ROS_TYPESTORE_TO_TEST)
    def test_to_ros_default_type(
        self, compressed_image: CompressedImage, typestore: Typestore
    ):
        ros_msg = CompressedImageAdapter.to_ros(compressed_image, typestore)
        self.assert_compressed_image(compressed_image, ros_msg)

    def test_to_ros_invalid_rosmsg_type(self, compressed_image: CompressedImage):
        ros_msg = CompressedImageAdapter.to_ros(
            compressed_image, get_typestore(Stores.LATEST), "sensor_msgs/msg/Bogus"
        )
        assert ros_msg is None

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


@pytest.fixture
def roi_msg(roi):
    return Message(data=roi, timestamp_ns=100, frame_id="camera_link")


class TestROIAdapter:
    def assert_roi(self, roi: ROI, ros_msg):
        assert roi.offset.x == ros_msg.x_offset
        assert roi.offset.y == ros_msg.y_offset
        assert roi.height == ros_msg.height
        assert roi.width == ros_msg.width
        assert roi.do_rectify == ros_msg.do_rectify

    @pytest.mark.parametrize("typestore", ROS_TYPESTORE_TO_TEST)
    def test_to_ros_roi(self, roi: ROI, typestore: Typestore):
        ros_msg = ROIAdapter.to_ros(roi, typestore, "sensor_msgs/msg/RegionOfInterest")
        self.assert_roi(roi, ros_msg)

    @pytest.mark.parametrize("typestore", ROS_TYPESTORE_TO_TEST)
    def test_to_ros_roi_message(self, roi_msg: Message, typestore: Typestore):
        roi = roi_msg.get_data(ROI)
        ros_msg = ROIAdapter.to_ros(
            roi_msg, typestore, "sensor_msgs/msg/RegionOfInterest"
        )
        self.assert_roi(roi, ros_msg)

    @pytest.mark.parametrize("typestore", ROS_TYPESTORE_TO_TEST)
    def test_to_ros_default_type(self, roi: ROI, typestore: Typestore):
        ros_msg = ROIAdapter.to_ros(roi, typestore)
        self.assert_roi(roi, ros_msg)

    def test_to_ros_invalid_rosmsg_type(self, roi: ROI):
        ros_msg = ROIAdapter.to_ros(
            roi, get_typestore(Stores.LATEST), "sensor_msgs/msg/Bogus"
        )
        assert ros_msg is None

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
        charge=70.0,
        capacity=100.0,
        design_capacity=210.0,
        percentage=20.0,
        power_supply_status=6,
        power_supply_health=7,
        power_supply_technology=8,
        present=True,
        location="car",
        serial_number="123456789",
        cell_voltage=[1.0, 2.0, 3.0],
        cell_temperature=[4.0, 5.0, 6.0],
    )


@pytest.fixture
def battery_state_msg(battery_state):
    return Message(data=battery_state, timestamp_ns=100, frame_id="car_link")


class TestBatteryStateAdapter:
    def assert_battery_state(self, battery_state: BatteryState, ros_msg):
        assert battery_state.voltage == ros_msg.voltage
        assert battery_state.temperature == ros_msg.temperature
        assert battery_state.current == ros_msg.current
        assert battery_state.charge == ros_msg.charge
        assert battery_state.capacity == ros_msg.capacity
        assert battery_state.design_capacity == ros_msg.design_capacity
        assert battery_state.percentage == ros_msg.percentage
        assert battery_state.power_supply_status == ros_msg.power_supply_status
        assert battery_state.power_supply_health == ros_msg.power_supply_health
        assert battery_state.power_supply_technology == ros_msg.power_supply_technology
        assert battery_state.present == ros_msg.present
        assert battery_state.location == ros_msg.location
        assert battery_state.serial_number == ros_msg.serial_number
        assert np.array_equal(battery_state.cell_voltage, ros_msg.cell_voltage)
        assert np.array_equal(battery_state.cell_temperature, ros_msg.cell_temperature)

    @pytest.mark.parametrize("typestore", ROS_TYPESTORE_TO_TEST)
    def test_to_ros_battery_state(
        self, battery_state: BatteryState, typestore: Typestore
    ):
        ros_msg = BatteryStateAdapter.to_ros(
            battery_state, typestore, "sensor_msgs/msg/BatteryState"
        )
        self.assert_battery_state(battery_state, ros_msg)

    @pytest.mark.parametrize("typestore", ROS_TYPESTORE_TO_TEST)
    def test_to_ros_battery_state_message(
        self, battery_state_msg: Message, typestore: Typestore
    ):
        battery_state = battery_state_msg.get_data(BatteryState)
        ros_msg = BatteryStateAdapter.to_ros(
            battery_state_msg, typestore, "sensor_msgs/msg/BatteryState"
        )
        self.assert_battery_state(battery_state, ros_msg)

    @pytest.mark.parametrize("typestore", ROS_TYPESTORE_TO_TEST)
    def test_to_ros_default_type(
        self, battery_state: BatteryState, typestore: Typestore
    ):
        ros_msg = BatteryStateAdapter.to_ros(battery_state, typestore)
        self.assert_battery_state(battery_state, ros_msg)

    def test_to_ros_invalid_rosmsg_type(self, battery_state: BatteryState):
        ros_msg = BatteryStateAdapter.to_ros(
            battery_state, get_typestore(Stores.LATEST), "sensor_msgs/msg/Bogus"
        )
        assert ros_msg is None

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
def robot_joint_msg(robot_joint):
    return Message(data=robot_joint, timestamp_ns=100, frame_id="base_link")


class TestRobotJointAdapter:
    def assert_robot_joint(self, robot_joint: RobotJoint, ros_msg):
        assert robot_joint.names == ros_msg.name
        assert np.array_equal(robot_joint.positions, ros_msg.position)
        assert np.array_equal(robot_joint.velocities, ros_msg.velocity)
        assert np.array_equal(robot_joint.efforts, ros_msg.effort)

    @pytest.mark.parametrize("typestore", ROS_TYPESTORE_TO_TEST)
    def test_to_ros_joint_state(self, robot_joint: RobotJoint, typestore: Typestore):
        ros_msg = RobotJointAdapter.to_ros(
            robot_joint, typestore, "sensor_msgs/msg/JointState"
        )
        self.assert_robot_joint(robot_joint, ros_msg)

    @pytest.mark.parametrize("typestore", ROS_TYPESTORE_TO_TEST)
    def test_to_ros_joint_state_message(
        self, robot_joint_msg: Message, typestore: Typestore
    ):
        robot_joint = robot_joint_msg.get_data(RobotJoint)
        ros_msg = RobotJointAdapter.to_ros(
            robot_joint_msg, typestore, "sensor_msgs/msg/JointState"
        )
        assert robot_joint_msg.frame_id == ros_msg.header.frame_id
        assert (
            robot_joint_msg.timestamp_ns
            == Time(
                seconds=ros_msg.header.stamp.sec,
                nanoseconds=ros_msg.header.stamp.nanosec,
            ).to_nanoseconds()
        )
        self.assert_robot_joint(robot_joint, ros_msg)

    @pytest.mark.parametrize("typestore", ROS_TYPESTORE_TO_TEST)
    def test_to_ros_default_type(self, robot_joint: RobotJoint, typestore: Typestore):
        ros_msg = RobotJointAdapter.to_ros(robot_joint, typestore)
        self.assert_robot_joint(robot_joint, ros_msg)

    def test_to_ros_invalid_rosmsg_type(self, robot_joint: RobotJoint):
        ros_msg = RobotJointAdapter.to_ros(
            robot_joint, get_typestore(Stores.LATEST), "sensor_msgs/msg/Bogus"
        )
        assert ros_msg is None

    def test_to_ros_invalid_mosaico_type(self, invalid_ms_msg):
        with pytest.raises(TypeError):
            RobotJointAdapter.to_ros(invalid_ms_msg, get_typestore(Stores.LATEST))
