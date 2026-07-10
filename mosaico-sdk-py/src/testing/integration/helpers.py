import itertools
import random
from dataclasses import dataclass
from typing import Iterable, List

from mosaicolabs.models.core import Message, Serializable
from mosaicolabs.models.data import (
    Point3d,
    Pose,
    Quaternion,
    RobotPath,
    Time,
    Vector3d,
)
from mosaicolabs.models.sensors import (
    GPS,
    IMU,
    GPSStatus,
    Magnetometer,
    RobotJoint,
    Temperature,
)
from testing.integration.config import (
    UPLOADED_GPS_METADATA,
    UPLOADED_GPS_TOPIC,
    UPLOADED_IMU_CAMERA_METADATA,
    UPLOADED_IMU_CAMERA_TOPIC,
    UPLOADED_IMU_FRONT_METADATA,
    UPLOADED_IMU_FRONT_TOPIC,
    UPLOADED_MAGNETOMETER_METADATA,
    UPLOADED_MAGNETOMETER_TOPIC,
    UPLOADED_ROBOT_JOINTS_METADATA,
    UPLOADED_ROBOT_JOINTS_TOPIC,
    UPLOADED_ROBOT_PATH_METADATA,
    UPLOADED_ROBOT_PATH_TOPIC,
    UPLOADED_TEMPERATURE_METADATA,
    UPLOADED_TEMPERATURE_TOPIC,
)


@dataclass
class DataStreamItem:
    topic: str
    msg: Message
    ontology_class: Serializable


@dataclass
class SequenceDataStream:
    tstamp_ns_start: int
    tstamp_ns_end: int
    dt_nanosec: int
    items: List[DataStreamItem]


def make_imu_front_msg(meas_time: Time):
    return Message(
        timestamp_ns=meas_time.to_nanoseconds(),
        data=IMU(
            acceleration=Vector3d(
                x=random.uniform(0, 1),
                y=random.uniform(0, 1),
                z=random.uniform(0, 1),
            ),
            angular_velocity=Vector3d(
                x=random.uniform(0, 1),
                y=random.uniform(0, 1),
                z=random.uniform(0, 1),
            ),
        ),
    )


def make_imu_cam_msg(meas_time: Time):
    return Message(
        timestamp_ns=meas_time.to_nanoseconds(),
        data=IMU(
            acceleration=Vector3d(
                x=random.uniform(0, 1),
                y=random.uniform(0, 1),
                z=random.uniform(0, 1),
            ),
            angular_velocity=Vector3d(
                x=random.uniform(0, 1),
                y=random.uniform(0, 1),
                z=random.uniform(0, 1),
            ),
        ),
    )


def make_gps_msg(meas_time: Time):
    return Message(
        timestamp_ns=meas_time.to_nanoseconds(),
        data=GPS(
            position=Point3d(
                x=random.uniform(0, 1),
                y=random.uniform(0, 1),
                z=random.uniform(0, 1),
            ),
            status=GPSStatus(
                status=0,
                service=2,
                # satellites=int(random.uniform(4, 20)),
            ),
        ),
    )


def make_magn_msg(meas_time: Time):
    return Message(
        timestamp_ns=meas_time.to_nanoseconds(),
        data=Magnetometer(
            magnetic_field=Vector3d(
                x=random.uniform(0, 1),
                y=random.uniform(0, 1),
                z=random.uniform(0, 1),
            ),
        ),
    )


def make_temperature_msg(meas_time: Time):

    return Message(
        timestamp_ns=meas_time.to_nanoseconds(),
        data=Temperature(value=330.0),
    )


def make_robotjoint_msg(meas_time: Time):

    joint_names = ["joint1", "joint2"]
    joint_values = [
        random.uniform(0, 1),
        random.uniform(0, 1),
    ]

    return Message(
        timestamp_ns=meas_time.to_nanoseconds(),
        data=RobotJoint(
            names=joint_names,
            positions=joint_values,
            velocities=joint_values,
            efforts=joint_values,
        ),
    )


def make_robotpath_msg(meas_time: Time):

    pose1 = Pose(
        position=Point3d(x=0.0, y=0.0, z=random.uniform(0, 1)),
        orientation=Quaternion(x=0.0, y=0.0, z=0.0, w=1.0),
    )
    pose2 = Pose(
        position=Point3d(x=0.0, y=0.0, z=random.uniform(0, -1)),
        orientation=Quaternion(x=0.0, y=0.0, z=0.0, w=1.0),
    )
    pose3 = Pose(
        position=Point3d(x=0.0, y=0.0, z=random.uniform(-1, 1)),
        orientation=Quaternion(x=0.0, y=0.0, z=0.0, w=1.0),
    )

    poses = [pose1, pose2, pose3]

    return Message(
        timestamp_ns=meas_time.to_nanoseconds(),
        data=RobotPath(
            poses=poses,
        ),
    )


topic_list = [
    UPLOADED_IMU_FRONT_TOPIC,
    UPLOADED_IMU_CAMERA_TOPIC,
    UPLOADED_GPS_TOPIC,
    UPLOADED_MAGNETOMETER_TOPIC,
]

topic_to_maker_factory = [
    (UPLOADED_IMU_FRONT_TOPIC, make_imu_front_msg),
    (UPLOADED_IMU_CAMERA_TOPIC, make_imu_cam_msg),
    (UPLOADED_GPS_TOPIC, make_gps_msg),
    (UPLOADED_MAGNETOMETER_TOPIC, make_magn_msg),
]

topic_to_listmaker_factory = [
    (UPLOADED_TEMPERATURE_TOPIC, make_temperature_msg),
    (UPLOADED_ROBOT_JOINTS_TOPIC, make_robotjoint_msg),
    (UPLOADED_ROBOT_PATH_TOPIC, make_robotpath_msg),
]

topic_to_ontology_class_dict = {
    UPLOADED_IMU_FRONT_TOPIC: IMU,
    UPLOADED_IMU_CAMERA_TOPIC: IMU,
    UPLOADED_GPS_TOPIC: GPS,
    UPLOADED_MAGNETOMETER_TOPIC: Magnetometer,
    UPLOADED_ROBOT_JOINTS_TOPIC: RobotJoint,
    UPLOADED_ROBOT_PATH_TOPIC: RobotPath,
    UPLOADED_TEMPERATURE_TOPIC: Temperature,
}

topic_to_metadata_dict = {
    UPLOADED_IMU_FRONT_TOPIC: UPLOADED_IMU_FRONT_METADATA,
    UPLOADED_IMU_CAMERA_TOPIC: UPLOADED_IMU_CAMERA_METADATA,
    UPLOADED_GPS_TOPIC: UPLOADED_GPS_METADATA,
    UPLOADED_MAGNETOMETER_TOPIC: UPLOADED_MAGNETOMETER_METADATA,
}

topic_to_listmetadata_dict = {
    UPLOADED_ROBOT_JOINTS_TOPIC: UPLOADED_ROBOT_JOINTS_METADATA,
    UPLOADED_ROBOT_PATH_TOPIC: UPLOADED_ROBOT_PATH_METADATA,
    UPLOADED_TEMPERATURE_TOPIC: UPLOADED_TEMPERATURE_METADATA,
}


def sequential_time_generator(
    start_sec: int,
    start_nanosec: int,
    step_nanosec: int,
    steps: int,
):
    sec = start_sec
    nsec = start_nanosec

    for _ in range(steps):
        yield Time(seconds=sec, nanoseconds=nsec)

        nsec += step_nanosec
        sec += nsec // 1_000_000_000
        nsec = nsec % 1_000_000_000


def topic_maker_generator(msg_maker: Iterable):
    return itertools.cycle(msg_maker)


def _validate_returned_topic_name(name: str):
    assert name.startswith("/")
