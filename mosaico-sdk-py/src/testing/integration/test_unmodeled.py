import pyarrow as pa

from mosaicolabs.comm.mosaico_client import MosaicoClient
from mosaicolabs.enum.serialization_format import SerializationFormat
from mosaicolabs.enum.session_level_error_policy import SessionLevelErrorPolicy
from mosaicolabs.models.core.message import Message
from mosaicolabs.models.core.unmodeled import Unmodeled, make_unmodeled_ontology_class

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


def test_unmodeled_ingestion_retrieval():
    with MosaicoClient.connect("localhost", 6276) as client:
        # -- Test ingestion --
        with client.sequence_create(
            "unmodeled_seq", {}, on_error=SessionLevelErrorPolicy.Delete
        ) as seqw:
            # Create a new topic and attach the unmodeled ontology as it was a default one
            tw = seqw.topic_create("/sensors/gyro/no_schema", {}, UnmodeledGyro)
            assert tw is not None
            # Make an unmodeled ontology instance
            unm_data = UnmodeledGyro(raw_data={"gyro": {"x": 1, "y": 2, "z": 3}})
            # Create and Push a message like a default ontology
            tw.push(Message(timestamp_ns=12345678, data=unm_data))

            # Make an unmodeled ontology instance
            unm_data = UnmodeledGyro(raw_data={"gyro": {"x": 4, "y": 5, "z": 6}})
            # Create and Push a message like a default ontology
            tw.push(Message(timestamp_ns=12345679, data=unm_data))

        # -- Test retrieval --
        th = client.topic_handler("unmodeled_seq", "/sensors/gyro/no_schema")
        assert th is not None
        # Info are correctly recognized
        assert th.ontology_tag == UnmodeledGyro.ontology_tag()
        assert th.ontology_schema == UnmodeledGyro.__msco_pyarrow_struct__

        # Test stream retrieval via TopicDataStreamer
        for msg in th.get_data_streamer():
            data = msg.get_data(UnmodeledGyro)
            assert data is not None
            assert "gyro" in data.raw_data
            assert all(item in data.raw_data["gyro"] for item in ("x", "y", "z"))
            assert all(data.raw_data["gyro"][item] > 0 for item in ("x", "y", "z"))
            data = msg.get_data(Unmodeled)
            assert data is not None
            assert "gyro" in data.raw_data
            assert all(item in data.raw_data["gyro"] for item in ("x", "y", "z"))
            assert all(data.raw_data["gyro"][item] > 0 for item in ("x", "y", "z"))

        # Test stream retrieval via SequenceDataStreamer
        sh = client.sequence_handler("unmodeled_seq")
        assert sh is not None
        for topic, msg in sh.get_data_streamer():
            data = msg.get_data(UnmodeledGyro)
            assert data is not None
            assert "gyro" in data.raw_data
            assert all(item in data.raw_data["gyro"] for item in ("x", "y", "z"))
            assert all(data.raw_data["gyro"][item] > 0 for item in ("x", "y", "z"))
            data = msg.get_data(Unmodeled)
            assert data is not None
            assert "gyro" in data.raw_data
            assert all(item in data.raw_data["gyro"] for item in ("x", "y", "z"))
            assert all(data.raw_data["gyro"][item] > 0 for item in ("x", "y", "z"))

        # Free resources
        client.sequence_delete("unmodeled_seq")
