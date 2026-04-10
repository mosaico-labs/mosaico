import pytest

from mosaicolabs import (
    GPS,
    IMU,
    ROI,
    Acceleration,
    Boolean,
    CameraInfo,
    Floating16,
    Floating32,
    Floating64,
    ForceTorque,
    GPSStatus,
    Image,
    ImageFormat,
    Integer8,
    Integer16,
    Integer32,
    Integer64,
    LargeString,
    Magnetometer,
    Message,
    MotionState,
    NMEASentence,
    Point2d,
    Point3d,
    Pose,
    Pressure,
    Quaternion,
    Range,
    RobotJoint,
    Serializable,
    String,
    Temperature,
    Transform,
    Unsigned8,
    Unsigned16,
    Unsigned32,
    Unsigned64,
    Vector2d,
    Vector3d,
    Vector4d,
    Velocity,
)
from mosaicolabs.models.futures.depth_camera import (
    RGBDCamera,
    StereoCamera,
    ToFCamera,
    pack_rgb,
)
from mosaicolabs.models.futures.laser import LaserScan, MultiEchoLaserScan
from mosaicolabs.models.futures.lidar import Lidar
from mosaicolabs.models.futures.radar import Radar


def assert_helper(expected, actual, rel=1e-4, abs_tol=None):

    assert type(expected) is type(actual), (
        f"Type mismatch (expected != actual): {type(expected)} != {type(actual)}"
    )

    if isinstance(expected, dict):
        assert set(expected.keys()) == set(actual.keys()), (
            f"Dict key mismatch (expected != actual):\n {set(expected.keys())} != {set(actual.keys())}"
        )

        for key in expected:
            assert_helper(expected[key], actual[key], rel=rel, abs_tol=abs_tol)

    elif isinstance(expected, (tuple, list)):
        assert len(expected) == len(actual), (
            f"tuple/list length mismatch (expected != actual): {len(expected)} != {len(actual)}"
        )

        for e, a in zip(expected, actual):
            assert_helper(e, a, rel=rel, abs_tol=abs_tol)

    elif isinstance(expected, (int, float)) and not isinstance(expected, bool):
        if abs_tol:
            assert expected == pytest.approx(actual, rel=rel, abs=abs_tol), (
                f"Value mismatch (expected != actual): {expected} != {actual}"
            )
        else:
            assert expected == pytest.approx(actual, rel=rel), (
                f"Value mismatch (expected != actual): {expected} != {actual}"
            )

    else:
        assert expected == actual, (
            f"Value mismatch(expected != actual): {expected} != {actual}"
        )


def assert_roundtrip(model: Serializable, rel=1e-4, abs_tol=None):
    assert model.is_registered() is True

    msg_to_encode = Message(timestamp_ns=1_000_000, data=model)
    rb = msg_to_encode._to_pa_record_batch()

    msg_decoded = Message._from_pa_record_batch(rb, model.ontology_tag())
    assert msg_decoded is not None

    a = model.model_dump()
    b = msg_decoded.data.model_dump()

    assert_helper(a, b, rel, abs_tol)


def test_integer8_roundtrip():
    model = Integer8(data=-127)
    assert_roundtrip(model)


def test_integer16_roundtrip():
    model = Integer16(data=32760)
    assert_roundtrip(model)


def test_integer32_roundtrip():
    model = Integer32(data=-2147483640)
    assert_roundtrip(model)


def test_integer64_roundtrip():
    model = Integer64(data=9223372036854775800)
    assert_roundtrip(model)


def test_unsigned8_roundtrip():
    model = Unsigned8(data=255)
    assert_roundtrip(model)


def test_unsigned16_roundtrip():
    model = Unsigned16(data=65530)
    assert_roundtrip(model)


def test_unsigned32_roundtrip():
    model = Unsigned32(data=4294967290)
    assert_roundtrip(model)


def test_unsigned64_roundtrip():
    model = Unsigned64(data=18446744073709551610)
    assert_roundtrip(model)


def test_floating16_roundtrip():
    model = Floating16(data=123.4)
    assert_roundtrip(model, rel=1e-3)


def test_floating32_roundtrip():
    model = Floating32(data=-45.678)
    assert_roundtrip(model)


def test_floating64_roundtrip():
    model = Floating64(data=123456.78901234567)
    assert_roundtrip(model)


def test_boolean_roundtrip():
    model = Boolean(data=True)
    assert_roundtrip(model)


def test_string_roundtrip():
    model = String(data="Mosaico_Test_Data_@2026_€")
    assert_roundtrip(model)


def test_large_string_roundtrip():
    model = LargeString(data="LOG_START" + ("A" * 1000) + "LOG_END")
    assert_roundtrip(model)


def test_forcetorque_roundtrip():
    model = ForceTorque(
        force=Vector3d(x=0.0, y=0.0, z=-9.81),
        torque=Vector3d(x=0.0, y=0.0, z=0.0),
        covariance=[0.01] * 36,
        covariance_type=1,
    )
    assert_roundtrip(model)


def test_forcetorque_required_only_roundtrip():
    model = ForceTorque(
        force=Vector3d(x=0.0, y=0.0, z=-9.81),
        torque=Vector3d(x=0.0, y=0.0, z=0.0),
    )
    assert_roundtrip(model)


def test_vector2d_roundtrip():
    model = Vector2d(x=1.5, y=-2.8, covariance=[0.1, 0.0, 0.0, 0.1], covariance_type=1)
    assert_roundtrip(model)


def test_vector2d_required_only_roudtrip():
    model = Vector2d(x=1.5, y=-2.8)
    assert_roundtrip(model)


def test_point2d_roundtrip():
    model = Point2d(x=10.0, y=20.0, covariance=[0.01] * 9, covariance_type=2)
    assert_roundtrip(model)


def test_point2d_required_only_roundtrip():
    model = Point2d(x=10.0, y=20.0)
    assert_roundtrip(model)


def test_vector3d_roundtrip():
    model = Vector3d(x=0.5, y=0.2, z=9.81, covariance=[0.01] * 9, covariance_type=2)
    assert_roundtrip(model)


def test_vector3d_required_only_roundtrip():
    model = Vector3d(x=0.5, y=0.2, z=9.81)
    assert_roundtrip(model)


def test_point3d_roundtrip():
    model = Point3d(
        x=41.8902, y=12.4922, z=54.0, covariance=[0.01] * 9, covariance_type=2
    )
    assert_roundtrip(model)


def test_point3d_required_only_roundtrip():
    model = Point3d(x=41.8902, y=12.4922, z=54.0)
    assert_roundtrip(model)


def test_vector4d_roundtrip():
    model = Vector4d(
        x=1.0, y=2.0, z=3.0, w=4.0, covariance=[0.0] * 16, covariance_type=1
    )
    assert_roundtrip(model)


def test_vector4d_required_only_roundtrip():
    model = Vector4d(x=1.0, y=2.0, z=3.0, w=4.0)
    assert_roundtrip(model)


def test_quaternion_roundtrip():
    model = Quaternion(
        x=0.0, y=0.0, z=0.7071, w=0.7071, covariance=[0.001] * 16, covariance_type=2
    )
    assert_roundtrip(model)


def test_quaternion_required_only_roundtrip():
    model = Quaternion(x=0.0, y=0.0, z=0.7071, w=0.7071)
    assert_roundtrip(model)


def test_transform_roundtrip():
    model = Transform(
        translation=Vector3d(x=1.0, y=0.0, z=0.5),
        rotation=Quaternion(x=0.0, y=0.0, z=0.0, w=1.0),
        target_frame_id="base_link",
        covariance=[0.01] * 69,
        covariance_type=1,
    )
    assert_roundtrip(model)


def test_transform_required_only_roundtrip():
    model = Transform(
        translation=Vector3d(x=1.0, y=0.0, z=0.5),
        rotation=Quaternion(x=0.0, y=0.0, z=0.0, w=1.0),
    )
    assert_roundtrip(model)


def test_pose_roundtrip():
    model = Pose(
        position=Point3d(x=-5.0, y=12.3, z=0.0),
        orientation=Quaternion(x=0.5, y=0.5, z=0.5, w=0.5),
        covariance=[0.0] * 49,
        covariance_type=2,
    )
    assert_roundtrip(model)


def test_pose_required_only_roundtrip():
    model = Pose(
        position=Point3d(x=-5.0, y=12.3, z=0.0),
        orientation=Quaternion(x=0.5, y=0.5, z=0.5, w=0.5),
    )
    assert_roundtrip(model)


def test_velocity_roundtrip():
    model = Velocity(
        linear=Vector3d(x=2.5, y=-1.2, z=0.0),
        angular=Vector3d(x=0.0, y=0.0, z=0.4),
        covariance=[0.01] * 36,
        covariance_type=1,
    )
    assert_roundtrip(model)


def test_velocity_required_only_roundtrip():
    model = Velocity(
        linear=Vector3d(x=2.5, y=-1.2, z=0.0),
        angular=Vector3d(x=0.0, y=0.0, z=0.4),
    )
    assert_roundtrip(model)


def test_acceleration_roundtrip():
    model = Acceleration(
        linear=Vector3d(x=0.0, y=0.0, z=-9.81),
        angular=Vector3d(x=0.1, y=0.0, z=0.0),
        covariance=[0.05] * 36,
        covariance_type=2,
    )
    assert_roundtrip(model)


def test_acceleration_required_only_roundtrip():
    model = Acceleration(linear=Vector3d(x=0.0, y=0.0, z=-9.81))
    assert_roundtrip(model)


def test_motion_state_roundtrip():
    model = MotionState(
        pose=Pose(
            position=Point3d(x=10.0, y=5.0, z=0.0),
            orientation=Quaternion(x=0.0, y=0.0, z=0.707, w=0.707),
        ),
        velocity=Velocity(
            linear=Vector3d(x=5.0, y=0.0, z=0.0), angular=Vector3d(x=0.0, y=0.0, z=0.1)
        ),
        acceleration=Acceleration(linear=Vector3d(x=0.5, y=0.0, z=0.0)),
        target_frame_id="map",
        covariance=[0.0] * 36,
        covariance_type=0,
    )
    assert_roundtrip(model)


def test_motion_state_required_only_roundtrip():
    model = MotionState(
        pose=Pose(
            position=Point3d(x=10.0, y=5.0, z=0.0),
            orientation=Quaternion(x=0.0, y=0.0, z=0.707, w=0.707),
        ),
        velocity=Velocity(
            linear=Vector3d(x=5.0, y=0.0, z=0.0), angular=Vector3d(x=0.0, y=0.0, z=0.1)
        ),
        target_frame_id="map",
    )
    assert_roundtrip(model)


def test_roi_roundtrip():
    model = ROI(
        offset=Vector2d(x=100.0, y=150.0), height=480, width=640, do_rectify=True
    )
    assert_roundtrip(model)


def test_roi_required_only_roundtrip():
    model = ROI(offset=Vector2d(x=100.0, y=150.0), height=480, width=640)
    assert_roundtrip(model)


def test_laser_roundtrip():
    model = LaserScan(
        angle_min=1.0,
        angle_max=1.0,
        angle_increment=1.0,
        time_increment=1.0,
        scan_time=12.12,
        range_min=1.0,
        range_max=3.0,
        ranges=[1.0, 2.0, 3.0],
        intensities=[1.0, 2.0, 3.0],
    )

    assert_roundtrip(model)


def test_laser_required_only_roundtrip():
    model = LaserScan(
        angle_min=1.0,
        angle_max=1.0,
        angle_increment=1.0,
        time_increment=1.0,
        scan_time=12.12,
        range_min=1.0,
        range_max=3.0,
        ranges=[1.0, 2.0, 3.0],
    )

    assert_roundtrip(model)


def test_multiecho_laser_roundtrip():
    model = MultiEchoLaserScan(
        angle_min=1.0,
        angle_max=1.0,
        angle_increment=1.0,
        time_increment=1.0,
        scan_time=12.12,
        range_min=1.0,
        range_max=3.0,
        ranges=[[1.0, 2.0, 3.0], [-1.0, -2.0, -3.0]],
        intensities=[[1.0, 2.0, 3.0], [-1.0, -2.0, -3.0]],
    )

    assert_roundtrip(model)


def test_multiecho_laser_required_only_roundtrip():
    model = MultiEchoLaserScan(
        angle_min=1.0,
        angle_max=1.0,
        angle_increment=1.0,
        time_increment=1.0,
        scan_time=12.12,
        range_min=1.0,
        range_max=3.0,
        ranges=[[1.0, 2.0, 3.0], [-1.0, -2.0, -3.0]],
    )

    assert_roundtrip(model)


def test_lidar_roundtrip():
    model = Lidar(
        x=[1.0, -1.0, 0.0001],
        y=[2.0, -2.0, 0.12],
        z=[3.0, -3.0, 9999.1],
        intensity=[1.0, 2.0, 3.0],
        reflectivity=[1, 2, 3],
        beam_id=[1, 2, 3],
        range=[1.0, 2.0, 3.0],
        near_ir=[223.1, 231.3, 44.8],
        azimuth=[43.0, 22.0, 99.1],
        elevation=[30.1, 23.1, 43.1],
        confidence=[1, 0, 1],
        return_type=[0, 1, 1],
        point_timestamp=[1_000_123.0, 1_000_124.0, 1_000_166.0],
    )

    assert_roundtrip(model)


def test_lidar_required_only_roundtrip():
    model = Lidar(
        x=[1.0, -1.0, 0.0001],
        y=[2.0, -2.0, 0.12],
        z=[3.0, -3.0, 9999.1],
    )

    assert_roundtrip(model)


def test_radar_roundtrip():
    model = Radar(
        x=[15.3, 8.7, 25.0],
        y=[0.2, -3.1, 4.8],
        z=[-0.1, 0.0, 0.3],
        range=[15.30, 9.24, 25.46],
        azimuth=[0.013, -0.342, 0.189],
        elevation=[-0.007, 0.0, 0.012],
        rcs=[12.5, -3.2, 18.0],
        snr=[24.1, 11.8, 30.5],
        doppler_velocity=[-8.3, -1.2, 0.1],
        vx=[-8.2, -1.1, 0.1],
        vy=[-0.1, 0.3, 0.0],
        vx_comp=[0.13, 7.23, 8.43],
        vy_comp=[-0.1, 0.3, 0.0],
        ax=[0.2, 0.0, 0.0],
        ay=[0.0, 0.05, 0.0],
        radial_speed=[8.3, 1.2, 0.4],
    )

    assert_roundtrip(model)


def test_radar_required_only_roundtrip():
    model = Radar(
        x=[15.3, 8.7, 25.0],
        y=[0.2, -3.1, 4.8],
        z=[-0.1, 0.0, 0.3],
    )

    assert_roundtrip(model)


def test_rgbdcamera_roundtrip():
    model = RGBDCamera(
        x=[-0.5, 0.0, 1.2, -2.1],
        y=[0.3, -0.8, 0.4, -1.5],
        z=[8.0, 15.0, 6.5, 40.0],
        rgb=[
            pack_rgb(80, 75, 70),
            pack_rgb(220, 50, 50),
            pack_rgb(160, 140, 120),
            pack_rgb(135, 180, 220),
        ],
        intensity=[0.45, 0.91, 0.52, 0.30],
    )

    assert_roundtrip(model, rel=1e-5, abs_tol=1e-40)


def test_rgbdcamera_required_only_roundtrip():
    model = RGBDCamera(
        x=[-0.5, 0.0, 1.2, -2.1],
        y=[0.3, -0.8, 0.4, -1.5],
        z=[8.0, 15.0, 6.5, 40.0],
    )

    assert_roundtrip(model, rel=1e-5)


def test_tofcamera_roundtrip():
    model = ToFCamera(
        x=[-0.5, 0.0, 1.2, -2.1],
        y=[0.3, -0.8, 0.4, -1.5],
        z=[8.0, 15.0, 6.5, 40.0],
        rgb=[
            pack_rgb(80, 75, 70),
            pack_rgb(220, 50, 50),
            pack_rgb(160, 140, 120),
            pack_rgb(135, 180, 220),
        ],
        intensity=[0.45, 0.91, 0.52, 0.30],
        noise=[0.02, 0.05, 0.03, 0.18],
        grayscale=[0.30, 0.45, 0.55, 0.72],
    )

    assert_roundtrip(model, rel=1e-5)


def test_tofcamera_required_only_roundtrip():
    model = ToFCamera(
        x=[-0.5, 0.0, 1.2, -2.1],
        y=[0.3, -0.8, 0.4, -1.5],
        z=[8.0, 15.0, 6.5, 40.0],
    )

    assert_roundtrip(model, rel=1e-5)


def test_stereocamera_roundtrip():
    model = StereoCamera(
        x=[-0.5, 0.0, 1.2, -2.1],
        y=[0.3, -0.8, 0.4, -1.5],
        z=[8.0, 15.0, 6.5, 40.0],
        rgb=[
            pack_rgb(80, 75, 70),
            pack_rgb(220, 50, 50),
            pack_rgb(160, 140, 120),
            pack_rgb(135, 180, 220),
        ],
        intensity=[0.45, 0.91, 0.52, 0.30],
        luma=[76, 115, 140, 184],
        cost=[4, 12, 7, 48],
    )

    assert_roundtrip(model, rel=1e-5)


def test_stereocamera_required_only_roundtrip():
    model = StereoCamera(
        x=[-0.5, 0.0, 1.2, -2.1],
        y=[0.3, -0.8, 0.4, -1.5],
        z=[8.0, 15.0, 6.5, 40.0],
    )

    assert_roundtrip(model, rel=1e-5)


def test_imu_roundtrip():
    model = IMU(
        acceleration=Vector3d(x=0.0156, y=-0.0212, z=9.8066),
        angular_velocity=Vector3d(x=0.0001, y=-0.0001, z=0.0),
        orientation=Quaternion(
            x=0.2588,
            y=0.0,
            z=0.0,
            w=0.9659,
            covariance=[0.01, 0.0, 0.0, 0.0, 0.01, 0.0, 0.0, 0.0, 0.01],
            covariance_type=2,
        ),
    )
    assert_roundtrip(model)


def test_imu_required_only_roundtrip():
    model = IMU(
        acceleration=Vector3d(x=0.0156, y=-0.0212, z=9.8066),
        angular_velocity=Vector3d(x=0.0001, y=-0.0001, z=0.0),
    )
    assert_roundtrip(model)


def test_temperature_roundtrip():
    model = Temperature(value=23.85, variance=0.0025, variance_type=1)
    assert_roundtrip(model)


def test_temperature_required_only_roundtrip():
    model = Temperature(value=23.85)
    assert_roundtrip(model)


def test_robot_joint_roundtrip():
    model = RobotJoint(
        names=["shoulder_pan_joint", "shoulder_lift_joint", "elbow_joint"],
        positions=[0.0, 1.5708, -0.7854],
        velocities=[0.1, -0.05, 0.2],
        efforts=[1.2, 45.8, 12.5],
    )
    assert_roundtrip(model)


def test_range_roundtrip():
    model = Range(
        radiation_type=0,
        field_of_view=0.2618,
        min_range=0.02,
        max_range=4.0,
        range=1.5432,
        variance=0.0001,
        variance_type=1,
    )
    assert_roundtrip(model)


def test_range_required_only_roundtrip():
    model = Range(
        radiation_type=0,
        field_of_view=0.2618,
        min_range=0.02,
        max_range=4.0,
        range=1.5432,
    )
    assert_roundtrip(model)


def test_pressure_roundtrip():
    model = Pressure(value=101325.4578, variance=1.0, variance_type=2)
    assert_roundtrip(model)


def test_pressure_required_only_roundtrip():
    model = Pressure(value=101325.4578)
    assert_roundtrip(model)


def test_magnetometer_roundtrip():
    model = Magnetometer(
        magnetic_field=Vector3d(
            x=22.5,
            y=-1.5,
            z=-42.8,
            covariance=[0.04, 0.0, 0.0, 0.0, 0.04, 0.0, 0.0, 0.0, 0.04],
            covariance_type=2,
        )
    )
    assert_roundtrip(model)


def test_magnetometer_required_only_roundtrip():
    model = Magnetometer(magnetic_field=Vector3d(x=22.5, y=-1.5, z=-42.8))
    assert_roundtrip(model)


def test_image_roundtrip():
    model = Image(
        data=bytes(
            [
                255,
                0,
                0,
                0,
                255,
                0,
                0,
                0,
                255,
                255,
                255,
                0,
                0,
                255,
                255,
                255,
                0,
                255,
            ]
        ),
        format=ImageFormat.RAW,
        width=3,
        height=2,
        stride=9,
        encoding="bgr8",
        is_bigendian=False,
    )

    assert_roundtrip(model)


def test_image_required_only_roundtrip():
    model = Image(
        data=bytes(
            [
                255,
                0,
                0,
                0,
                255,
                0,
                0,
                0,
                255,
                255,
                255,
                0,
                0,
                255,
                255,
                255,
                0,
                255,
            ]
        ),
        format=ImageFormat.RAW,
        width=3,
        height=2,
        stride=9,
        encoding="bgr8",
    )

    assert_roundtrip(model)


def test_gps_status_roundtrip():
    model = GPSStatus(
        status=3,
        service=1,
        satellites=18,
        hdop=0.6,
        vdop=0.8,
    )

    assert_roundtrip(model)


def test_gps_status_required_only_roundtrip():
    model = GPSStatus(status=3, service=1)

    assert_roundtrip(model)


def test_gps_roundtrip():
    model = GPS(
        position=Point3d(
            x=35.360625,
            y=138.727361,
            z=3776.0,
        ),
        velocity=Vector3d(x=0.01, y=-0.02, z=0.0),
        status=GPSStatus(
            status=3,
            service=1,
            satellites=18,
            hdop=0.6,
            vdop=0.8,
        ),
    )

    assert_roundtrip(model)


def test_gps_required_only_roundtrip():
    model = GPS(
        position=Point3d(
            x=35.360625,
            y=138.727361,
            z=3776.0,
        )
    )

    assert_roundtrip(model)


def test_nmea_sentence_roundtrip():
    model = NMEASentence(
        sentence="$GPGGA,060000,3521.6375,N,13843.6416,E,1,18,0.6,3776.0,M,45.0,M,,*52"
    )
    assert_roundtrip(model)


def test_camera_info_roundtrip():
    model = CameraInfo(
        height=1080,
        width=1920,
        distortion_model="plumb_bob",
        distortion_parameters=[-0.2, 0.1, 0.001, 0.001, 0.0],
        intrinsic_parameters=[1200.0, 0.0, 960.0, 0.0, 1200.0, 540.0, 0.0, 0.0, 1.0],
        rectification_parameters=[1.0, 0.0, 0.0, 0.0, 1.0, 0.0, 0.0, 0.0, 1.0],
        projection_parameters=[
            1200.0,
            0.0,
            960.0,
            0.0,
            0.0,
            1200.0,
            540.0,
            0.0,
            0.0,
            0.0,
            1.0,
            0.0,
        ],
        binning=Vector2d(x=1, y=1),
        roi=ROI(offset=Vector2d(x=0, y=0), width=0, height=0, do_rectify=False),
    )
    assert_roundtrip(model)


def test_camera_info_required_only_roundtrip():
    model = CameraInfo(
        height=1080,
        width=1920,
        distortion_model="plumb_bob",
        distortion_parameters=[-0.2, 0.1, 0.001, 0.001, 0.0],
        intrinsic_parameters=[1200.0, 0.0, 960.0, 0.0, 1200.0, 540.0, 0.0, 0.0, 1.0],
        rectification_parameters=[1.0, 0.0, 0.0, 0.0, 1.0, 0.0, 0.0, 0.0, 1.0],
        projection_parameters=[
            1200.0,
            0.0,
            960.0,
            0.0,
            0.0,
            1200.0,
            540.0,
            0.0,
            0.0,
            0.0,
            1.0,
            0.0,
        ],
    )
    assert_roundtrip(model)
