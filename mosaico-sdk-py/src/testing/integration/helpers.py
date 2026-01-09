from dataclasses import dataclass
import itertools
import random
from typing import Iterable, List

from mosaicolabs.models.data import Point3d, Vector3d
from mosaicolabs.models.sensors import IMU, GPS, GPSStatus, Magnetometer
from mosaicolabs.models import Serializable, Message, Header, Time
from testing.integration.config import (
    UPLOADED_GPS_FRAME_ID,
    UPLOADED_GPS_METADATA,
    UPLOADED_GPS_TOPIC,
    UPLOADED_IMU_CAMERA_FRAME_ID,
    UPLOADED_IMU_CAMERA_METADATA,
    UPLOADED_IMU_CAMERA_TOPIC,
    UPLOADED_IMU_FRONT_FRAME_ID,
    UPLOADED_IMU_FRONT_METADATA,
    UPLOADED_IMU_FRONT_TOPIC,
    UPLOADED_MAGNETOMETER_FRAME_ID,
    UPLOADED_MAGNETOMETER_METADATA,
    UPLOADED_MAGNETOMETER_TOPIC,
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
    items: List[DataStreamItem]


def make_imu_front_msg(msg_time: int, meas_time: Time):
    return Message(
        timestamp_ns=msg_time,
        data=IMU(
            header=Header(
                stamp=meas_time,
                frame_id=UPLOADED_IMU_FRONT_FRAME_ID,
            ),
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


def make_imu_cam_msg(msg_time: int, meas_time: Time):
    return Message(
        timestamp_ns=msg_time,
        data=IMU(
            header=Header(
                stamp=meas_time,
                frame_id=UPLOADED_IMU_CAMERA_FRAME_ID,
            ),
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


def make_gps_msg(msg_time: int, meas_time: Time):
    return Message(
        timestamp_ns=msg_time,
        data=GPS(
            header=Header(
                stamp=meas_time,
                frame_id=UPLOADED_GPS_FRAME_ID,
            ),
            position=Point3d(
                x=random.uniform(0, 1),
                y=random.uniform(0, 1),
                z=random.uniform(0, 1),
            ),
            status=GPSStatus(
                status=0,
                service=2,
                satellites=int(random.uniform(4, 20)),
            ),
        ),
    )


def make_magn_msg(msg_time: int, meas_time: Time):
    return Message(
        timestamp_ns=msg_time,
        data=Magnetometer(
            header=Header(
                stamp=meas_time,
                frame_id=UPLOADED_MAGNETOMETER_FRAME_ID,
            ),
            magnetic_field=Vector3d(
                x=random.uniform(0, 1),
                y=random.uniform(0, 1),
                z=random.uniform(0, 1),
            ),
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

topic_to_ontology_class_dict = {
    UPLOADED_IMU_FRONT_TOPIC: IMU,
    UPLOADED_IMU_CAMERA_TOPIC: IMU,
    UPLOADED_GPS_TOPIC: GPS,
    UPLOADED_MAGNETOMETER_TOPIC: Magnetometer,
}

topic_to_metadata_dict = {
    UPLOADED_IMU_FRONT_TOPIC: UPLOADED_IMU_FRONT_METADATA,
    UPLOADED_IMU_CAMERA_TOPIC: UPLOADED_IMU_CAMERA_METADATA,
    UPLOADED_GPS_TOPIC: UPLOADED_GPS_METADATA,
    UPLOADED_MAGNETOMETER_TOPIC: UPLOADED_MAGNETOMETER_METADATA,
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
        yield Time(sec=sec, nanosec=nsec)

        nsec += step_nanosec
        sec += nsec // 1_000_000_000
        nsec = nsec % 1_000_000_000


def topic_maker_generator(msg_maker: Iterable):
    return itertools.cycle(msg_maker)


def _validate_returned_topic_name(name: str):
    assert name.startswith("/")
