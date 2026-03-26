import pytest

from mosaicolabs.comm import MosaicoClient
from mosaicolabs.enum.session_level_error_policy import SessionLevelErrorPolicy
from mosaicolabs.models import Message
from mosaicolabs.models.query.builders import QueryOntologyCatalog, QueryTopic
from mosaicolabs.models.sensors import Pressure, Temperature

from .config import UPLOADED_SEQUENCE_NAME
from .helpers import SequenceDataStream


def test_sequence_update_not_in_context(
    mosaico_client: MosaicoClient,
    inject_synthetic_sequence,  # Make sure data are available on the server
):
    seqhandler = mosaico_client.sequence_handler(UPLOADED_SEQUENCE_NAME)
    # Sequence must exist
    assert seqhandler is not None
    seq_updater = seqhandler.update()  # This does not create a session
    with pytest.raises(
        RuntimeError,
        match="SequenceWriter or SequenceUpdater must be used within a 'with' block.",
    ):
        seq_updater.topic_create(
            "test_topic",
            {},
            Pressure,
        )


def test_sequence_update_on_error_report(
    mosaico_client: MosaicoClient,
    inject_synthetic_sequence,  # Make sure data are available on the server
):
    seqhandler = mosaico_client.sequence_handler(UPLOADED_SEQUENCE_NAME)
    # Sequence must exist
    assert seqhandler is not None
    session_uuid = ""
    with seqhandler.update() as seq_updater:  # default SessionLevelErrorPolicy.Report
        session_uuid = seq_updater.session_uuid
        with pytest.raises(RuntimeError, match="__inner_exception__"):
            seq_updater.topic_create(
                "test_topic_report",
                {},
                Pressure,
            )
            raise RuntimeError("__inner_exception__")

    # Now we can read the data back
    assert seqhandler.reload() is True
    # The session and its data are still on the server
    assert "/test_topic_report" in seqhandler.topics

    mosaico_client.session_delete(session_uuid)


def test_sequence_update_on_error_delete(
    mosaico_client: MosaicoClient,
    inject_synthetic_sequence,  # Make sure data are available on the server
):
    seqhandler = mosaico_client.sequence_handler(UPLOADED_SEQUENCE_NAME)
    # Sequence must exist
    assert seqhandler is not None
    with pytest.raises(RuntimeError, match="__inner_exception__"):
        with seqhandler.update(on_error=SessionLevelErrorPolicy.Delete) as seq_updater:
            seq_updater.topic_create(
                "test_topic_delete",
                {},
                Pressure,
            )
            raise RuntimeError("__inner_exception__")

    # Now we can read the data back
    assert seqhandler.reload() is True
    # The session and its data are not on the server
    assert "/test_topic_delete" not in seqhandler.topics


def test_sequence_update(
    mosaico_client: MosaicoClient,
    synthetic_sequence_data_stream: SequenceDataStream,  # Get the data stream for comparisons
    inject_synthetic_sequence,  # Make sure data are available on the server
):
    seqhandler = mosaico_client.sequence_handler(UPLOADED_SEQUENCE_NAME)
    # Sequence must exist
    assert seqhandler is not None
    session_uuid = ""
    with seqhandler.update() as seq_updater:
        session_uuid = seq_updater.session_uuid
        twriter = seq_updater.topic_create(
            "/updated_topic/pressure",
            {"sensor_id": "pressure_1"},
            Pressure,
        )
        assert twriter is not None
        twriter.push(
            Message(
                timestamp_ns=synthetic_sequence_data_stream.tstamp_ns_start,
                data=Pressure(value=100),
            )
        )

        twriter.push(
            Message(
                timestamp_ns=synthetic_sequence_data_stream.tstamp_ns_start
                + synthetic_sequence_data_stream.dt_nanosec,
                data=Pressure(value=101),
            )
        )

        twriter = seq_updater.topic_create(
            "/updated_topic/temperature",
            {"sensor_id": "temperature_1"},
            Temperature,
        )
        assert twriter is not None
        twriter.push(
            Message(
                timestamp_ns=synthetic_sequence_data_stream.tstamp_ns_start,
                data=Temperature.from_celsius(value=37.5),
            )
        )

        twriter.push(
            Message(
                timestamp_ns=synthetic_sequence_data_stream.tstamp_ns_start
                + synthetic_sequence_data_stream.dt_nanosec,
                data=Temperature.from_celsius(value=38.0),
            )
        )

    # Now we can read the data back
    assert seqhandler.reload() is True
    assert "/updated_topic/pressure" in seqhandler.topics
    assert "/updated_topic/temperature" in seqhandler.topics

    # Query the catalog for the updated data
    # Query by topic name
    query_resp = mosaico_client.query(QueryTopic().with_name_match("temperature"))
    assert query_resp is not None and not query_resp.is_empty()
    assert len(query_resp) == 1
    assert len(query_resp[0].topics) == 1
    assert query_resp[0].topics[0].name == "/updated_topic/temperature"

    # Query by topic metadata
    query_resp = mosaico_client.query(
        QueryTopic().with_user_metadata("sensor_id", eq="temperature_1")
    )
    assert query_resp is not None and not query_resp.is_empty()
    assert len(query_resp) == 1
    assert len(query_resp[0].topics) == 1
    assert query_resp[0].topics[0].name == "/updated_topic/temperature"

    # Query by data catalog
    query_resp = mosaico_client.query(
        QueryOntologyCatalog().with_expression(Pressure.Q.value.geq(100))
    )
    assert query_resp is not None and not query_resp.is_empty()
    assert len(query_resp) == 1
    assert len(query_resp[0].topics) == 1
    assert query_resp[0].topics[0].name == "/updated_topic/pressure"

    mosaico_client.session_delete(session_uuid)
