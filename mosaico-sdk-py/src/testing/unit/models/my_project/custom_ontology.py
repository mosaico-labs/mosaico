import pyarrow as pa

from mosaicolabs.enum import SerializationFormat
from mosaicolabs.models import BaseModel, Serializable


class RegisteredSensor(Serializable):
    """
    Test correctly registered data
    """

    __msco_pyarrow_struct__ = pa.struct(
        [
            pa.field(
                "field",
                pa.float32(),
                nullable=False,
            ),
        ]
    )

    field: float


class UnregisteredSensor(BaseModel):
    """
    Check not-correctly registered data
    """

    __msco_pyarrow_struct__ = pa.struct(
        [
            pa.field(
                "field",
                pa.float32(),
                nullable=False,
            ),
        ]
    )
    # Define Serializable inner variables, to make the test passing the getattr error
    __ontology_tag__ = "unregistered_sensor"
    __serialization_format__ = SerializationFormat.Ragged
    __class_type__ = type
    field: float
