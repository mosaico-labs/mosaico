import pyarrow as pa

from mosaicolabs.comm.mosaico_client import MosaicoClient
from mosaicolabs.enum.serialization_format import SerializationFormat
from mosaicolabs.enum.session_level_error_policy import SessionLevelErrorPolicy
from mosaicolabs.models.core.message import Message
from mosaicolabs.models.core.unmodeled import make_unmodeled_ontology_class

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
                metadata={"description": "Test Implementation of Accelerometer"},
            ),
        ]
    ),
)

UnmodeledGyro = make_unmodeled_ontology_class(
    "UnmodeledGyro",
    None,
    SerializationFormat.Default,
    pa.struct(
        [
            pa.field(
                "gyro",
                pa.struct(
                    [
                        pa.field("x", pa.float32()),
                        pa.field("y", pa.float32()),
                        pa.field("z", pa.float32()),
                    ]
                ),
                nullable=False,
                metadata={"description": "Test Implementation of Gyroscope"},
            ),
        ]
    ),
)

# print(UnmodeledGyro.Q.queryable_fields)
# print(UnmodeledGyro.Q.queryable_schema)

# exit(0)

with MosaicoClient.connect("localhost", 6276) as client:
    with client.sequence_create(
        "unmodeled_seq", {}, on_error=SessionLevelErrorPolicy.Delete
    ) as seqw:
        # Create a new topic and attach the unmodeled ontology as it was a default one
        tw = seqw.topic_create("/sensors/acc/no_schema", {}, UnmodeledAcceleration)
        assert tw is not None
        # Make an unmodeled ontology instance
        unm_data = UnmodeledAcceleration(
            raw_data={"acceleration": {"x": 1, "y": 2, "z": 3}}
        )
        # Create and Push a message like a default ontology
        tw.push(Message(timestamp_ns=1, data=unm_data))

        # Make an unmodeled ontology instance
        unm_data = UnmodeledAcceleration(
            raw_data={"acceleration": {"x": 4, "y": 5, "z": 6}}
        )
        # Create and Push a message like a default ontology
        tw.push(Message(timestamp_ns=3, data=unm_data))

        # Create another new topic and attach the unmodeled ontology as it was a default one
        tw = seqw.topic_create("/sensors/gyro/no_schema", {}, UnmodeledGyro)
        assert tw is not None
        # Make an unmodeled ontology instance
        unm_data = UnmodeledGyro(raw_data={"gyro": {"x": 7, "y": 8, "z": 9}})
        # Create and Push a message like a default ontology
        tw.push(Message(timestamp_ns=2, data=unm_data))

        # Make an unmodeled ontology instance
        unm_data = UnmodeledGyro(raw_data={"gyro": {"x": 10, "y": 11, "z": 12}})
        # Create and Push a message like a default ontology
        tw.push(Message(timestamp_ns=4, data=unm_data))
