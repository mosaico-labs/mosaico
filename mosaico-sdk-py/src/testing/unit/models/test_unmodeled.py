import pyarrow as pa

from mosaicolabs.enum.serialization_format import SerializationFormat
from mosaicolabs.models.core.message import Message
from mosaicolabs.models.core.unmodeled import Unmodeled, make_unmodeled_ontology_class

# Unmodeled ontology definition for local test usage
UnmodeledAcceleration = make_unmodeled_ontology_class(
    "UnmodeledAcceleration",
    None,
    SerializationFormat.Default,
    pa.struct(
        [
            pa.field(
                "acceleration",
                pa.struct(
                    [
                        pa.field("x", pa.float32()),
                        pa.field("y", pa.float32()),
                        pa.field("z", pa.float32()),
                    ]
                ),
                nullable=False,
                metadata={
                    "description": "Test Implementation of unmodeled accelerometer"
                },
            ),
        ]
    ),
)


def test_unmodeled_class_serializable():
    # Test Serializability of unmodeled ontology
    assert UnmodeledAcceleration.is_registered()

    # Test queryability of pyarrow schema
    assert "timestamp_ns" in UnmodeledAcceleration.Q.queryable_fields
    assert "acceleration" in UnmodeledAcceleration.Q.queryable_fields
    assert all(
        item in UnmodeledAcceleration.Q.acceleration.queryable_fields
        for item in ("x", "y", "z")
    )

    # If this is not an expression, will raise
    UnmodeledAcceleration.Q.timestamp_ns.eq(0)
    UnmodeledAcceleration.Q.acceleration.x.eq(0)

    raw_data = {"acceleration": {"x": 0, "y": 0, "z": 0}}
    # Test embedding into a Message
    msg = Message(
        timestamp_ns=123456789,
        data=UnmodeledAcceleration(raw_data=raw_data),
    )

    # Test retrieving from Message
    # Convert to Actual type
    umodeled_acc = msg.get_data(UnmodeledAcceleration)
    assert umodeled_acc is not None
    assert umodeled_acc.raw_data == raw_data

    # Test retrieving from Message
    # Convert to base type, common to all unmodeled ontologies
    umodeled_acc = msg.get_data(Unmodeled)
    assert umodeled_acc is not None
    assert umodeled_acc.raw_data == raw_data
