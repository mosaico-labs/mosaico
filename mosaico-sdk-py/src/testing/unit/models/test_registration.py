import pydantic
import pytest
import pyarrow as pa

from mosaicolabs.models import Serializable, Message, CovarianceMixin, HeaderMixin
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
    assert RegisteredSensor.is_registered()


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


def test_ontology_type_schema_mismatch():
    """
    Verifies that a TypeError is raised when Pydantic fields
    do not match the PyArrow struct.
    """
    with pytest.raises(TypeError) as excinfo:
        # Define the class LOCALLY to trigger __init_subclass__ now
        class MismatchedSensorMissingField(Serializable, HeaderMixin, CovarianceMixin):
            __msco_pyarrow_struct__ = pa.struct(
                [
                    pa.field("data", pa.float64(), nullable=False),
                ]
            )
            # Missing: 'data' attribute
            # (header is inherited but often triggers the check
            # if the MRO/annotations aren't settled)
            pass

    # Verify the error message contains the expected mismatch details
    assert "Schema mismatch in ontology class 'MismatchedSensorMissingField'" in str(
        excinfo.value
    )

    with pytest.raises(TypeError) as excinfo:
        # Define the class LOCALLY to trigger __init_subclass__ now
        class MismatchedSensorRenamedField(Serializable, HeaderMixin):
            __msco_pyarrow_struct__ = pa.struct(
                [
                    pa.field(
                        "field",
                        pa.float32(),
                        nullable=False,
                    ),
                ]
            )
            other_field: float

    # Verify the error message contains the expected mismatch details
    assert "Schema mismatch in ontology class 'MismatchedSensorRenamedField'" in str(
        excinfo.value
    )

    with pytest.raises(TypeError) as excinfo:
        # Define the class LOCALLY to trigger __init_subclass__ now
        class MismatchedSensorMissingPAField(Serializable, HeaderMixin):
            __msco_pyarrow_struct__ = pa.struct([])
            field: float

    # Verify the error message contains the expected mismatch details
    assert "Schema mismatch in ontology class 'MismatchedSensorMissingPAField'" in str(
        excinfo.value
    )
