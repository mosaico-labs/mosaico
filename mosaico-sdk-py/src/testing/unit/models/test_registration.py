import pydantic
import pytest

from mosaicolabs.models import Serializable, Message
from .my_project import RegisteredSensor, UnregisteredSensor


def test_ontology_type_registered():
    # Check injestion of 'Serializable' fields
    assert RegisteredSensor.__ontology_tag__ is not None
    assert hasattr(RegisteredSensor, "__msco_pyarrow_struct__")
    assert hasattr(RegisteredSensor, "__ontology_tag__")
    assert hasattr(RegisteredSensor, "__serialization_format__")
    assert RegisteredSensor.__ontology_tag__ == "registered_sensor"
    assert RegisteredSensor.__serialization_format__.value == "default"
    # Check inheritance
    assert issubclass(RegisteredSensor.__class_type__, Serializable)
    assert issubclass(RegisteredSensor, Serializable)
    # Check factory registration
    assert Serializable.is_registered(RegisteredSensor.__ontology_tag__)


def test_ontology_type_unregistered():
    # Type has all the correct fields provided by Serializable
    assert UnregisteredSensor.__ontology_tag__ is not None
    assert hasattr(UnregisteredSensor, "__msco_pyarrow_struct__")
    assert hasattr(UnregisteredSensor, "__ontology_tag__")
    assert hasattr(UnregisteredSensor, "__serialization_format__")
    assert UnregisteredSensor.__ontology_tag__ == "unregistered_sensor"
    assert UnregisteredSensor.__serialization_format__.value == "ragged"

    # However, it does not inherit from Serializable
    assert not issubclass(UnregisteredSensor.__class_type__, Serializable)
    assert not issubclass(UnregisteredSensor, Serializable)
    # It is not registered
    assert not Serializable.is_registered(UnregisteredSensor.__ontology_tag__)


def test_message_generation():
    # This must pass
    Message(timestamp_ns=0, data=RegisteredSensor(field=0))

    with pytest.raises(
        pydantic.ValidationError,
        match="Input should be a valid dictionary or instance of Serializable",
    ):
        # This must fail: Unregistered type cannot be sent to mosaico
        Message(timestamp_ns=0, data=UnregisteredSensor(field=0))  # type: ignore (disable pylance complaining)
