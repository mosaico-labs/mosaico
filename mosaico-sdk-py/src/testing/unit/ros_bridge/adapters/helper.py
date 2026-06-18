import math
from typing import Optional

import numpy as np

from mosaicolabs import (
    GPS,
    IMU,
    ROI,
    Acceleration,
    CameraInfo,
    CompressedImage,
    ForceTorque,
    GPSStatus,
    Header,
    Image,
    Inertia,
    Joy,
    Magnetometer,
    MotionState,
    NMEASentence,
    Point3d,
    Polygon,
    Pose,
    Pressure,
    Quaternion,
    RobotJoint,
    RobotPath,
    Temperature,
    Time,
    Transform,
    Vector2d,
    Vector3d,
    Velocity,
    futures,
)
from mosaicolabs.models.futures import GridCells, MapMetadata, OccupancyGrid
from mosaicolabs.ros_bridge.data_ontology import (
    BatteryState,
    FrameTransform,
    PointCloud2,
)


def assert_time(time: Time, ros_msg: dict):
    assert time.seconds == ros_msg["sec"]
    assert time.nanoseconds == ros_msg["nanosec"]


def assert_header(header: Optional[Header], ros_msg: dict):

    if not header:  # In case there is not header in mosaico, it is encoded as an empty header in ROS
        assert ros_msg["frame_id"] == ""
        assert ros_msg["stamp"]["sec"] == 0
        assert ros_msg["stamp"]["nanosec"] == 0

        return

    assert header.frame_id == ros_msg["frame_id"]
    assert_time(header.timestamp, ros_msg["stamp"])


def assert_vector2(vector2d: Vector2d, ros_msg: dict):
    assert vector2d.x == ros_msg["x"]
    assert vector2d.y == ros_msg["y"]


def assert_vector3(vector3d: Vector3d, ros_msg: dict):

    if "header" in ros_msg:
        assert_vector3(vector3d, ros_msg["vector"])
        assert_header(vector3d.header, ros_msg["header"])

        return

    assert vector3d.x == ros_msg["x"]
    assert vector3d.y == ros_msg["y"]
    assert vector3d.z == ros_msg["z"]


def assert_point3d(point3d: Point3d, ros_msg: dict):

    if "header" in ros_msg:
        assert_point3d(point3d, ros_msg["point"])
        assert_header(point3d.header, ros_msg["header"])

        return

    assert point3d.x == ros_msg["x"]
    assert point3d.y == ros_msg["y"]
    assert point3d.z == ros_msg["z"]


def assert_quaternion(quaternion: Quaternion, ros_msg: dict):

    if "header" in ros_msg:
        assert_quaternion(quaternion, ros_msg["quaternion"])
        assert_header(quaternion.header, ros_msg["header"])

        return

    assert quaternion.x == ros_msg["x"]
    assert quaternion.y == ros_msg["y"]
    assert quaternion.z == ros_msg["z"]
    assert quaternion.w == ros_msg["w"]


def assert_transform(transform: Transform, ros_msg: dict):

    if "header" in ros_msg:
        assert_transform(transform, ros_msg["transform"])

        if transform.target_frame_id:
            assert transform.target_frame_id == ros_msg["child_frame_id"]
        else:
            assert ros_msg["child_frame_id"] == ""

        assert_header(transform.header, ros_msg["header"])

        return

    assert_vector3(transform.translation, ros_msg["translation"])
    assert_quaternion(transform.rotation, ros_msg["rotation"])


def assert_force_torque(force_torque: ForceTorque, ros_msg: dict):

    if "header" in ros_msg:
        assert_force_torque(force_torque, ros_msg["wrench"])
        assert_header(force_torque.header, ros_msg["header"])

        return

    assert_vector3(force_torque.force, ros_msg["force"])
    assert_vector3(force_torque.torque, ros_msg["torque"])


def assert_polygon(polygon: Polygon, ros_msg: dict):

    if "header" in ros_msg:
        assert_polygon(polygon, ros_msg["polygon"])
        assert_header(polygon.header, ros_msg["header"])

        return

    for point3d, ros_point in zip(polygon.points, ros_msg["points"]):
        assert_point3d(point3d, ros_point)


def assert_inertia(inertia: Inertia, ros_msg: dict):

    if "header" in ros_msg:
        assert_inertia(inertia, ros_msg["inertia"])
        assert_header(inertia.header, ros_msg["header"])

        return

    assert inertia.mass == ros_msg["m"]
    assert_vector3(inertia.center_of_mass, ros_msg["com"])
    ixx, ixy, ixz = ros_msg["ixx"], ros_msg["ixy"], ros_msg["ixz"]
    iyy, iyz, izz = ros_msg["iyy"], ros_msg["iyz"], ros_msg["izz"]
    assert len(inertia.inertia) == 6
    assert inertia.inertia == [ixx, ixy, ixz, iyy, iyz, izz]


def assert_pose(pose: Pose, ros_msg):

    if "header" in ros_msg:
        assert_pose(pose, ros_msg["pose"])
        assert_header(pose.header, ros_msg["header"])

        return

    assert_point3d(pose.position, ros_msg["position"])
    assert_quaternion(pose.orientation, ros_msg["orientation"])


def assert_pose_w_cov(pose_w_cov: Pose, ros_msg):

    if "header" in ros_msg:
        assert_pose_w_cov(pose_w_cov, ros_msg["pose"])
        assert_header(pose_w_cov.header, ros_msg["header"])

        return

    assert_pose(pose_w_cov, ros_msg["pose"])

    if pose_w_cov.covariance is None:
        assert np.array_equal(ros_msg["covariance"], np.array(([0.0] * 36)))
    else:
        assert np.array_equal(pose_w_cov.covariance, ros_msg["covariance"])


def assert_twist(twist: Velocity, ros_msg):

    if "header" in ros_msg:
        assert_twist(twist, ros_msg["twist"])
        assert_header(twist.header, ros_msg["header"])

        return

    assert_vector3(twist.linear, ros_msg["linear"])
    assert_vector3(twist.angular, ros_msg["angular"])


def assert_twist_w_cov(twist_w_cov: Velocity, ros_msg):

    if "header" in ros_msg:
        assert_twist_w_cov(twist_w_cov, ros_msg["twist"])
        assert_header(twist_w_cov.header, ros_msg["header"])

        return

    assert_twist(twist_w_cov, ros_msg["twist"])

    if twist_w_cov.covariance is None:
        assert np.array_equal(ros_msg["covariance"], np.array(([0.0] * 36)))
    else:
        assert np.array_equal(twist_w_cov.covariance, ros_msg["covariance"])


def assert_accel(accel: Acceleration, ros_msg):

    if "header" in ros_msg:
        assert_accel(accel, ros_msg["accel"])
        assert_header(accel.header, ros_msg["header"])

        return

    assert_vector3(accel.linear, ros_msg["linear"])
    assert_vector3(accel.angular, ros_msg["angular"])


def assert_accel_w_cov(accel_w_cov: Acceleration, ros_msg):

    if "header" in ros_msg:
        assert_accel_w_cov(accel_w_cov, ros_msg["accel"])
        assert_header(accel_w_cov.header, ros_msg["header"])

        return

    assert_accel(accel_w_cov, ros_msg["accel"])

    if accel_w_cov.covariance is None:
        assert np.array_equal(ros_msg["covariance"], np.array(([0.0] * 36)))
    else:
        assert np.array_equal(accel_w_cov.covariance, ros_msg["covariance"])


def assert_roi(roi: ROI, ros_msg):
    assert roi.offset.x == ros_msg["x_offset"]
    assert roi.offset.y == ros_msg["y_offset"]
    assert roi.height == ros_msg["height"]
    assert roi.width == ros_msg["width"]
    assert roi.do_rectify == ros_msg["do_rectify"]


def assert_camera_info(camera_info: CameraInfo, ros_msg):

    assert_header(camera_info.header, ros_msg["header"])

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

    assert_header(gps.header, ros_msg["header"])
    assert_gps_status(gps.status, ros_msg["status"])
    assert gps.position.x == ros_msg["latitude"]
    assert gps.position.y == ros_msg["longitude"]
    assert gps.position.z == ros_msg["altitude"]

    if gps.position.covariance is None:
        assert np.array_equal(ros_msg["position_covariance"], np.array(([0.0] * 9)))
    else:
        assert np.array_equal(gps.position.covariance, ros_msg["position_covariance"])
        assert gps.position.covariance_type == ros_msg["position_covariance_type"]


def assert_imu(imu: IMU, ros_msg):

    assert_header(imu.header, ros_msg["header"])
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


def assert_nmea_sentence(nmea_sentence: NMEASentence, ros_msg: dict):
    assert_header(nmea_sentence.header, ros_msg["header"])
    assert nmea_sentence.sentence == ros_msg["sentence"]


def assert_compressed_image(compressed_image: CompressedImage, ros_msg: dict):
    assert_header(compressed_image.header, ros_msg["header"])
    assert compressed_image.format == ros_msg["format"]
    assert np.array_equal(
        np.frombuffer(compressed_image.data, dtype=np.uint8), ros_msg["data"]
    )


def assert_image(image: Image, ros_msg):
    assert_header(image.header, ros_msg["header"])

    assert image.height == ros_msg["height"]
    assert image.width == ros_msg["width"]
    assert image.encoding == ros_msg["encoding"]
    assert int(image.is_bigendian) == ros_msg["is_bigendian"]
    assert image.stride == ros_msg["step"]
    assert np.array_equal(
        np.frombuffer(bytes(image.to_linear_pixels()), dtype=np.uint8), ros_msg["data"]
    )


def assert_battery_state(battery_state: BatteryState, ros_msg):
    assert_header(battery_state.header, ros_msg["header"])

    assert battery_state.voltage == ros_msg["voltage"]
    assert battery_state.temperature == ros_msg["temperature"]
    assert battery_state.current == ros_msg["current"]

    if battery_state.charge:
        assert battery_state.charge == ros_msg["charge"]
    else:
        assert math.isnan(ros_msg["charge"])

    assert battery_state.capacity == ros_msg["capacity"]
    assert battery_state.design_capacity == ros_msg["design_capacity"]
    assert battery_state.percentage == ros_msg["percentage"]
    assert battery_state.power_supply_status == ros_msg["power_supply_status"]
    assert battery_state.power_supply_health == ros_msg["power_supply_health"]
    assert battery_state.power_supply_technology == ros_msg["power_supply_technology"]
    assert battery_state.present == ros_msg["present"]
    assert battery_state.location == ros_msg["location"]
    assert battery_state.serial_number == ros_msg["serial_number"]

    if battery_state.cell_voltage:
        assert np.array_equal(battery_state.cell_voltage, ros_msg["cell_voltage"])
    else:
        assert len(ros_msg["cell_voltage"]) == 0

    assert np.array_equal(battery_state.cell_temperature, ros_msg["cell_temperature"])


def assert_robot_joint(robot_joint: RobotJoint, ros_msg: dict):
    assert_header(robot_joint.header, ros_msg["header"])

    assert robot_joint.names == ros_msg["name"]
    assert np.array_equal(robot_joint.positions, ros_msg["position"])
    assert np.array_equal(robot_joint.velocities, ros_msg["velocity"])
    assert np.array_equal(robot_joint.efforts, ros_msg["effort"])


def assert_joy(joy: Joy, ros_msg: dict):
    assert_header(joy.header, ros_msg["header"])

    assert np.array_equal(joy.axes, ros_msg["axes"])
    assert np.array_equal(joy.buttons, ros_msg["buttons"])


def assert_magnetometer(magnetometer: Magnetometer, ros_msg: dict):
    assert_header(magnetometer.header, ros_msg["header"])
    assert_vector3(magnetometer.magnetic_field, ros_msg["magnetic_field"])

    if magnetometer.magnetic_field.covariance is not None:
        assert np.array_equal(
            magnetometer.magnetic_field.covariance,
            ros_msg["magnetic_field_covariance"],
        )
    else:
        assert np.array_equal([0] * 9, ros_msg["magnetic_field_covariance"])


def assert_pcl2(pcl2: PointCloud2, ros_msg):
    assert_header(pcl2.header, ros_msg["header"])
    assert pcl2.height == ros_msg["height"]
    assert pcl2.width == ros_msg["width"]

    for field, ros_field in zip(pcl2.fields, ros_msg["fields"]):
        assert field.name == ros_field["name"]
        assert field.offset == ros_field["offset"]
        assert field.datatype == ros_field["datatype"]
        assert field.count == ros_field["count"]

    assert pcl2.is_bigendian == ros_msg["is_bigendian"]
    assert pcl2.point_step == ros_msg["point_step"]
    assert pcl2.row_step == ros_msg["row_step"]

    buffer = np.frombuffer(pcl2.data, dtype=np.uint8)
    assert np.array_equal(buffer, ros_msg["data"])
    assert pcl2.is_dense == ros_msg["is_dense"]


def assert_laserscan(laserscan: futures.LaserScan, ros_msg):
    assert_header(laserscan.header, ros_msg["header"])
    assert laserscan.angle_min == ros_msg["angle_min"]
    assert laserscan.angle_max == ros_msg["angle_max"]
    assert laserscan.angle_increment == ros_msg["angle_increment"]
    assert laserscan.time_increment == ros_msg["time_increment"]
    assert laserscan.scan_time == ros_msg["scan_time"]
    assert laserscan.range_min == ros_msg["range_min"]
    assert laserscan.range_max == ros_msg["range_max"]
    assert np.array_equal(laserscan.ranges, ros_msg["ranges"])
    if laserscan.intensities is not None:
        assert np.array_equal(laserscan.intensities, ros_msg["intensities"])
    else:
        assert len(ros_msg["intensities"]) == 0


def assert_multiecho_laserscan(mels: futures.MultiEchoLaserScan, ros_msg):
    assert_header(mels.header, ros_msg["header"])
    assert mels.angle_min == ros_msg["angle_min"]
    assert mels.angle_max == ros_msg["angle_max"]
    assert mels.angle_increment == ros_msg["angle_increment"]
    assert mels.time_increment == ros_msg["time_increment"]
    assert mels.scan_time == ros_msg["scan_time"]
    assert mels.range_min == ros_msg["range_min"]
    assert mels.range_max == ros_msg["range_max"]
    for mels_range, ros_range in zip(mels.ranges, ros_msg["ranges"]):
        assert np.array_equal(mels_range, ros_range["echoes"])
    if mels.intensities is not None:
        for intensity, ros_intensity in zip(mels.intensities, ros_msg["intensities"]):
            assert np.array_equal(intensity, ros_intensity["echoes"])
    else:
        assert len(ros_msg["intensities"]) == 0


def assert_motion_state(motion_state: MotionState, ros_msg):

    assert_header(motion_state.header, ros_msg["header"])
    assert_pose_w_cov(motion_state.pose, ros_msg["pose"])
    assert_twist_w_cov(motion_state.velocity, ros_msg["twist"])

    assert motion_state.target_frame_id == ros_msg["child_frame_id"]


def assert_frame_transform(frame_trasform: FrameTransform, ros_msg):

    for mosaico_transform, ros_transform in zip(
        frame_trasform.transforms, ros_msg["transforms"]
    ):
        assert_transform(mosaico_transform, ros_transform["transform"])


def assert_temperature(temperature: Temperature, ros_msg):
    assert_header(temperature.header, ros_msg["header"])

    assert temperature.to_celsius() == ros_msg["temperature"]

    if temperature.variance is not None:
        assert temperature.variance == ros_msg["variance"]
    else:
        assert ros_msg["variance"] == 0


def assert_pressure(pressure: Pressure, ros_msg):
    assert_header(pressure.header, ros_msg["header"])

    assert pressure.value == ros_msg["fluid_pressure"]

    if pressure.variance is not None:
        assert pressure.variance == ros_msg["variance"]
    else:
        assert ros_msg["variance"] == 0


def assert_path(path: RobotPath, ros_msg):

    assert_header(path.header, ros_msg["header"])

    for pose, pose_stamped_ros in zip(path.poses, ros_msg["poses"]):
        assert_pose(pose, pose_stamped_ros["pose"])


def assert_grid_cells(grid_cells: GridCells, ros_msg: dict):

    assert_header(grid_cells.header, ros_msg["header"])

    assert grid_cells.cell_width == ros_msg["cell_width"]
    assert grid_cells.cell_height == ros_msg["cell_height"]
    for cell, ros_cell in zip(grid_cells.cells, ros_msg["cells"]):
        assert cell.x == ros_cell["x"]
        assert cell.y == ros_cell["y"]
        assert cell.z == ros_cell["z"]


def assert_map_metadata(map_metadata: MapMetadata, ros_msg: dict):
    assert map_metadata.map_load_time.seconds == ros_msg["map_load_time"]["sec"]
    assert map_metadata.map_load_time.nanoseconds == ros_msg["map_load_time"]["nanosec"]
    assert map_metadata.resolution == ros_msg["resolution"]
    assert map_metadata.width == ros_msg["width"]
    assert map_metadata.height == ros_msg["height"]
    assert_pose(map_metadata.origin, ros_msg["origin"])


def assert_occupancy_grid(occupancy_grid: OccupancyGrid, ros_msg: dict):

    assert_header(occupancy_grid.header, ros_msg["header"])
    assert_map_metadata(occupancy_grid.info, ros_msg["info"])
    assert list(occupancy_grid.data) == list(ros_msg["data"])
