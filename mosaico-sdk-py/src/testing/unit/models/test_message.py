import pytest

from mosaicolabs.models import Message


def test_message_not_serializable():
    """Test the correct exception raise if the data is not serializable"""
    data = int(0)
    with pytest.raises(
        ValueError,
        match="Input should be a valid dictionary or instance of Serializable",
    ):
        Message(timestamp_ns=0, data=data)
