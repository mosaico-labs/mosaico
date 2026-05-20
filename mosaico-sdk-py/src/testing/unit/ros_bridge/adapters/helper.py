import numpy as np

from mosaicolabs import (
    GPS,
    IMU,
    ROI,
    Acceleration,
    CameraInfo,
    ForceTorque,
    GPSStatus,
    Inertia,
    Point3d,
    Polygon,
    Pose,
    Quaternion,
    Transform,
    Vector2d,
    Vector3d,
    Velocity,
)


def assert_vector2(vector2d: Vector2d, ros_msg: dict):
    assert vector2d.x == ros_msg["x"]
    assert vector2d.y == ros_msg["y"]


def assert_vector3(vector3d: Vector3d, ros_msg: dict):
    assert vector3d.x == ros_msg["x"]
    assert vector3d.y == ros_msg["y"]
    assert vector3d.z == ros_msg["z"]


def assert_point3d(point3d: Point3d, ros_msg: dict):
    assert point3d.x == ros_msg["x"]
    assert point3d.y == ros_msg["y"]
    assert point3d.z == ros_msg["z"]


def assert_quaternion(quaternion: Quaternion, ros_msg: dict):
    assert quaternion.x == ros_msg["x"]
    assert quaternion.y == ros_msg["y"]
    assert quaternion.z == ros_msg["z"]
    assert quaternion.w == ros_msg["w"]


def assert_transform(transform: Transform, ros_msg: dict):
    assert_vector3(transform.translation, ros_msg["translation"])
    assert_quaternion(transform.rotation, ros_msg["rotation"])

    assert transform.covariance == ros_msg.get("covariance")


def assert_force_torque(force_torque: ForceTorque, ros_msg: dict):
    assert_vector3(force_torque.force, ros_msg["force"])
    assert_vector3(force_torque.torque, ros_msg["torque"])


def assert_polygon(polygon: Polygon, ros_msg: dict):
    for point3d, ros_point in zip(polygon.points, ros_msg["points"]):
        assert_point3d(point3d, ros_point)


def assert_inertia(inertia: Inertia, ros_msg: dict):
    assert inertia.mass == ros_msg["m"]
    assert_vector3(inertia.center_of_mass, ros_msg["com"])
    ixx, ixy, ixz = ros_msg["ixx"], ros_msg["ixy"], ros_msg["ixz"]
    iyy, iyz, izz = ros_msg["iyy"], ros_msg["iyz"], ros_msg["izz"]
    assert len(inertia.inertia) == 6
    assert inertia.inertia == [ixx, ixy, ixz, iyy, iyz, izz]


def assert_pose(pose: Pose, ros_msg):
    assert_point3d(pose.position, ros_msg["position"])
    assert_quaternion(pose.orientation, ros_msg["orientation"])


def assert_pose_w_cov(pose_w_cov: Pose, ros_msg):

    assert_pose(pose_w_cov, ros_msg["pose"])

    if pose_w_cov.covariance is None:
        assert (ros_msg["covariance"] == 0).all()
    else:
        assert np.array_equal(pose_w_cov.covariance, ros_msg["covariance"])


def assert_twist(twist: Velocity, ros_msg):
    assert_vector3(twist.linear, ros_msg["linear"])
    assert_vector3(twist.angular, ros_msg["angular"])


def assert_twist_w_cov(twist_w_cov: Velocity, ros_msg):

    assert_twist(twist_w_cov, ros_msg["twist"])

    if twist_w_cov.covariance is None:
        assert (ros_msg["covariance"] == 0).all()
    else:
        assert np.array_equal(twist_w_cov.covariance, ros_msg["covariance"])


def assert_accel(accel: Acceleration, ros_msg):
    assert_vector3(accel.linear, ros_msg["linear"])
    assert_vector3(accel.angular, ros_msg["angular"])


def assert_accel_w_cov(accel_w_cov: Acceleration, ros_msg):

    assert_accel(accel_w_cov, ros_msg["accel"])

    if accel_w_cov.covariance is None:
        assert (ros_msg["covariance"] == 0).all()
    else:
        assert np.array_equal(accel_w_cov.covariance, ros_msg["covariance"])


def assert_roi(roi: ROI, ros_msg):
    assert roi.offset.x == ros_msg["x_offset"]
    assert roi.offset.y == ros_msg["y_offset"]
    assert roi.height == ros_msg["height"]
    assert roi.width == ros_msg["width"]
    assert roi.do_rectify == ros_msg["do_rectify"]


def assert_camera_info(camera_info: CameraInfo, ros_msg):
    assert camera_info.height == ros_msg["height"]
    assert camera_info.width == ros_msg["width"]
    assert camera_info.distortion_model == ros_msg["distortion_model"]

    if "d" in ros_msg:  # ROS2
        assert camera_info.distortion_parameters == list(ros_msg["d"])
    else:
        assert camera_info.distortion_parameters == list(ros_msg["D"])

    if "k" in ros_msg:  # ROS2
        assert camera_info.intrinsic_parameters == list(ros_msg["k"])
    else:
        assert camera_info.intrinsic_parameters == list(ros_msg["K"])

    if "r" in ros_msg:  # ROS2
        assert camera_info.rectification_parameters == list(ros_msg["r"])
    else:
        assert camera_info.rectification_parameters == list(ros_msg["R"])

    if "p" in ros_msg:  # ROS2
        assert camera_info.projection_parameters == list(ros_msg["p"])
    else:
        assert camera_info.projection_parameters == list(ros_msg["P"])

    assert camera_info.binning.x == ros_msg["binning_x"]
    assert camera_info.binning.y == ros_msg["binning_y"]
    assert_roi(camera_info.roi, ros_msg["roi"])


def assert_gps_status(gps_status: GPSStatus, ros_msg):
    assert gps_status.status == ros_msg["status"]
    assert gps_status.service == ros_msg["service"]


def assert_gps(gps: GPS, ros_msg):
    assert_gps_status(gps.status, ros_msg["status"])
    assert gps.position.x == ros_msg["latitude"]
    assert gps.position.y == ros_msg["longitude"]
    assert gps.position.z == ros_msg["altitude"]

    if gps.position.covariance is None:
        assert (ros_msg["position_covariance"] == 0.0).all()
    else:
        assert np.array_equal(gps.position.covariance, ros_msg["position_covariance"])
        assert gps.position.covariance_type == ros_msg["position_covariance_type"]


def assert_imu(imu: IMU, ros_msg):
    assert_quaternion(imu.orientation, ros_msg["orientation"])
    assert_vector3(imu.angular_velocity, ros_msg["angular_velocity"])
    assert_vector3(imu.acceleration, ros_msg["linear_acceleration"])

    if imu.orientation.covariance is None:
        assert all(v == 0 for v in ros_msg["orientation_covariance"])
    else:
        assert np.array_equal(
            imu.orientation.covariance, ros_msg["orientation_covariance"]
        )

    if imu.angular_velocity.covariance is None:
        assert all(v == 0 for v in ros_msg["angular_velocity_covariance"])
    else:
        assert np.array_equal(
            imu.angular_velocity.covariance, ros_msg["angular_velocity_covariance"]
        )

    if imu.acceleration.covariance is None:
        assert all(v == 0 for v in ros_msg["angular_velocity_covariance"])
    else:
        assert np.array_equal(
            imu.acceleration.covariance, ros_msg["linear_acceleration_covariance"]
        )
