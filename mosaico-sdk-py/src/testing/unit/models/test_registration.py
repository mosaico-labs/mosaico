import subprocess
import sys
import textwrap

import pydantic
import pytest

from mosaicolabs import Message, Serializable

from .my_project import RegisteredSensor, UnregisteredSensor


def test_ontology_type_registered():
    # Check injestion of 'Serializable' fields
    assert RegisteredSensor.__ontology_tag__ is not None
    assert hasattr(RegisteredSensor, "__msco_pyarrow_struct__")
    assert hasattr(RegisteredSensor, "__ontology_tag__")
    assert hasattr(RegisteredSensor, "__serialization_format__")
    assert (
        RegisteredSensor.__ontology_tag__ == "registered_sensor" or "RegisteredSensor"
    )
    assert RegisteredSensor.__serialization_format__.value == "default"
    # Check inheritance
    assert issubclass(RegisteredSensor.__class_type__, Serializable)
    assert issubclass(RegisteredSensor, Serializable)
    # Check factory registration
    assert RegisteredSensor.is_registered()


def test_ontology_type_has_schema_fingerprint():
    # __schema_fingerprint__ is computed for every Serializable subclass upon
    # registration, not just for dynamically-generated (Unmodeled) ones.
    assert hasattr(RegisteredSensor, "__schema_fingerprint__")
    assert RegisteredSensor.__schema_fingerprint__ != ""


def test_ontology_type_registry_key_defaults_to_ontology_tag():
    # For every hand-authored class, the SDK-local registry key is identical to
    # the tag reported to the platform - they only diverge for dynamically
    # resolved schema variants (see test_helpers.py).
    assert RegisteredSensor.__registry_key__ == RegisteredSensor.__ontology_tag__
    assert RegisteredSensor.__registry_key__ == RegisteredSensor.ontology_tag()


def test_ontology_type_unregistered():
    # Type has all the correct fields provided by Serializable
    assert UnregisteredSensor.__ontology_tag__ is not None
    assert hasattr(UnregisteredSensor, "__msco_pyarrow_struct__")
    assert hasattr(UnregisteredSensor, "__ontology_tag__")
    assert hasattr(UnregisteredSensor, "__serialization_format__")
    assert (
        UnregisteredSensor.__ontology_tag__ == "unregistered_sensor"
        or "UnregisteredSensor"
    )
    assert UnregisteredSensor.__serialization_format__.value == "ragged"

    # However, it does not inherit from Serializable
    assert not issubclass(UnregisteredSensor.__class_type__, Serializable)
    assert not issubclass(UnregisteredSensor, Serializable)
    # It is not registered (the class does not inherit from Serializable and does not have a is_registered method)
    assert not Serializable._is_registered(UnregisteredSensor.__ontology_tag__)


def test_message_generation():
    # This must pass
    Message(timestamp_ns=0, data=RegisteredSensor(field=0))

    with pytest.raises(
        pydantic.ValidationError,
        match="Input should be a valid dictionary or instance of Serializable",
    ):
        # This must fail: Unregistered type cannot be sent to mosaico
        Message(timestamp_ns=0, data=UnregisteredSensor(field=0))  # type: ignore (disable pylance complaining)


def test_futures_registration():
    code = textwrap.dedent(
        # Generate a fresh environmnent
        """
            from mosaicolabs.models.core import Serializable

            _FUTURES_TAGS = [
                "RGBDCamera",
                "ToFCamera",
                "StereoCamera",
                "LaserScan",
                "MultiEchoLaserScan",
                "Lidar",
                "Radar",
            ]

            assert all(Serializable._is_registered(tag) for tag in _FUTURES_TAGS)
        """
    )

    result = subprocess.run(
        [sys.executable, "-c", code],
        capture_output=True,
        text=True,
    )

    assert result.returncode == 0, result.stderr
