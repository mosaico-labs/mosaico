import pytest

from mosaicolabs import Point3d, Time, Vector2d
from mosaicolabs.models import Message


def test_no_timestamp():

    # Vector2d is an example of Serializable that has NO embedded meas_timestamp
    payload = Vector2d(x=0.0, y=0.0)

    with pytest.raises(
        ValueError,
        match=f"{Vector2d.__name__} does not support timestamp and Message does not define one."
        "Please define at least one among the two!",
    ):
        # This must fail: types with neither an internal timestamp nor a Message timestamp cannot be created
        Message(data=payload)

    # This is ok since timestamp is defined at Message lvl
    Message(timestamp_ns=100, data=payload)


def test_invalid_timestamp():

    # Point3d is an example of Serializable that has embedded meas_timestamp
    # However, if you do not define it will get an invalid value
    payload = Point3d(x=1.0, y=1.0, z=1.0)

    with pytest.raises(
        ValueError,
        match=f"Neither {Point3d.__name__} nor Message define a valid timestamp."
        "Please define at least one among the two!",
    ):
        # This must fail: types with neither an internal timestamp nor a Message timestamp cannot be created
        Message(data=payload)


def test_valid_timestamp():

    # This is ok since timestamp is defined at Payload lvl
    payload = Point3d(x=1.0, y=1.0, z=1.0, meas_timestamp=Time.from_nanoseconds(10000))
    Message(data=payload)

    # This is ok since timestamp is defined both at Message and Payload lvl
    Message(timestamp_ns=100, data=payload)
