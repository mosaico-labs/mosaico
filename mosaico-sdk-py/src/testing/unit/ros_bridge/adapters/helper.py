import numpy as np

from mosaicolabs import (
    ForceTorque,
    Inertia,
    Point3d,
    Polygon,
    Pose,
    Quaternion,
    Transform,
    Vector3d,
)


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
    assert_vector3(pose.position, ros_msg["position"])
    assert_quaternion(pose.orientation, ros_msg["orientation"])


def asset_pose_w_cov(pose_w_cov: Pose, ros_msg):

    assert_pose(pose_w_cov, ros_msg["pose"])

    if pose_w_cov.covariance is None:
        assert (ros_msg["covariance"] == 0).all()
    else:
        assert np.array_equal(pose_w_cov.covariance, ros_msg["covariance"])
