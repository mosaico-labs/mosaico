from mosaicolabs import Time
from mosaicolabs import Header
import pytest

from mosaicolabs import IMU, Vector3d
from mosaicolabs.models import Message


def test_message_not_serializable():
    """Test the correct exception raise if the data is not serializable"""
    data = int(0)
    with pytest.raises(
        ValueError,
        match="Input should be a valid dictionary or instance of Serializable",
    ):
        Message(timestamp_ns=0, data=data)


def test_message_no_timestamp():
    """Test the correct exception raise if the data has no timestamp."""
    data = IMU(
        acceleration=Vector3d(x=0, y=0, z=0),
        angular_velocity=Vector3d(x=0, y=0, z=0),
        # no header
    )
    with pytest.raises(ValueError, match="Timestamp data is needed."):
        Message(data=data)


def test_message_with_header_no_timestamp():
    """Test the correct timestamp assignment from the sensor time header."""
    tstamp = Time.from_float(123456789.54321)
    data = IMU(
        acceleration=Vector3d(x=0, y=0, z=0),
        angular_velocity=Vector3d(x=0, y=0, z=0),
        header=Header(stamp=tstamp),
    )
    msg = Message(data=data)
    assert msg.timestamp_ns == tstamp.to_nanoseconds()
    assert msg.data.header.stamp == tstamp


def test_message_with_timestamp_no_header():
    """Test the correct timestamp assignment from the message timestamp."""
    tstamp = Time.from_float(123456789.54321)
    data = IMU(
        acceleration=Vector3d(x=0, y=0, z=0),
        angular_velocity=Vector3d(x=0, y=0, z=0),
    )
    msg = Message(timestamp_ns=tstamp.to_nanoseconds(), data=data)
    assert msg.timestamp_ns == tstamp.to_nanoseconds()
    # The sensor header should have been set to the message timestamp
    assert msg.data.header is not None
    assert msg.data.header.stamp == tstamp


def test_message_with_timestamp_and_header():
    """Test the correct difference between the message and data timestamp."""
    sens_tstamp = Time.from_float(123456789.54321)
    msg_tstamp = 987654321123450000
    data = IMU(
        acceleration=Vector3d(x=0, y=0, z=0),
        angular_velocity=Vector3d(x=0, y=0, z=0),
        header=Header(stamp=sens_tstamp),
    )
    msg = Message(timestamp_ns=msg_tstamp, data=data)
    assert msg.timestamp_ns == msg_tstamp
    # The sensor header should have been set to the message timestamp
    assert msg.data.header is not None
    assert msg.data.header.stamp == sens_tstamp
