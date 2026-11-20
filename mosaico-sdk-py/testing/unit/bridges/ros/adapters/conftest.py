import pytest

from mosaicolabs import (
    Header,
    Message,
    Point3d,
    Quaternion,
    Serializable,
    Time,
    Vector3d,
)


@pytest.fixture
def invalid_ms_msg() -> Message:
    return Message(
        data=Serializable(),
        timestamp_ns=0,
    )


@pytest.fixture
def vector3d():
    return Vector3d(x=1.0, y=2.0, z=3.0)


@pytest.fixture
def point3d():
    return Point3d(x=1.0, y=2.0, z=3.0)


@pytest.fixture
def quaternion():
    return Quaternion(x=0.0, y=0.0, z=0.0, w=1.0)


@pytest.fixture
def ms_header():
    return Header(timestamp=Time(seconds=100, nanoseconds=200), frame_id="base_link")


@pytest.fixture
def ros_header():
    return {"frame_id": "base_link", "stamp": {"sec": 100, "nanosec": 200}}
