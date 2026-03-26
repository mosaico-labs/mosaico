from mosaicolabs.comm import MosaicoClient
from mosaicolabs.models.query import QueryOntologyCatalog, QuerySequence, QueryTopic
from mosaicolabs.models.sensors import GPS, IMU
from mosaicolabs.types import Time
from testing.integration.config import (
    UPLOADED_GPS_TOPIC,
    UPLOADED_IMU_CAMERA_TOPIC,
    UPLOADED_IMU_FRONT_TOPIC,
    UPLOADED_SEQUENCE_NAME,
)

from .helpers import _validate_returned_topic_name


def test_query_ontology(
    mosaico_client: MosaicoClient,
    inject_synthetic_sequence,  # Ensure the data are available on the data platform
):
    # Query by single condition
    query_resp = mosaico_client.query(
        QueryOntologyCatalog().with_expression(
            IMU.Q.acceleration.x.geq(0.01)
        )  # set a very small value (data are random, so a small value is likely to be found)
    )
    # We do expect a successful query
    assert query_resp is not None and not query_resp.is_empty()
    # One (1) sequence corresponds to this query
    assert len(query_resp) == 1
    # The target topics are 'UPLOADED_IMU_FRONT_TOPIC' and 'UPLOADED_IMU_CAMERA_TOPIC'
    expected_topic_names = [
        UPLOADED_IMU_FRONT_TOPIC,
        UPLOADED_IMU_CAMERA_TOPIC,
    ]
    assert len(query_resp[0].topics) == len(expected_topic_names)

    # all the expected topics, and only them
    [_validate_returned_topic_name(topic.name) for topic in query_resp[0].topics]
    assert all([t.name in expected_topic_names for t in query_resp[0].topics])

    # Query by multiple condition: time and value
    tstamp = Time.from_float(1700000000.26)
    query_resp = mosaico_client.query(
        QueryOntologyCatalog().with_expression(
            IMU.Q.timestamp_ns.geq(tstamp.to_nanoseconds())
        )
    )
    # We do expect a successful query
    assert query_resp is not None and not query_resp.is_empty()
    # One (1) sequence corresponds to this query
    assert len(query_resp) == 1
    # The target topics are 'UPLOADED_IMU_FRONT_TOPIC' and 'UPLOADED_IMU_CAMERA_TOPIC'
    expected_topic_names = [
        UPLOADED_IMU_FRONT_TOPIC,
        UPLOADED_IMU_CAMERA_TOPIC,
    ]
    assert len(query_resp[0].topics) == len(expected_topic_names)
    # all the expected topics, and only them
    [_validate_returned_topic_name(topic.name) for topic in query_resp[0].topics]
    assert all([t.name in expected_topic_names for t in query_resp[0].topics])

    # Query by multiple condition: time and value (GPS)
    tstamp = Time.from_float(1700000000.26)
    query_resp = mosaico_client.query(
        QueryOntologyCatalog()
        .with_expression(GPS.Q.timestamp_ns.geq(tstamp.to_nanoseconds()))
        .with_expression(GPS.Q.status.service.eq(2))
    )
    # We do expect a successful query
    assert query_resp is not None and not query_resp.is_empty()
    # One (1) sequence corresponds to this query
    assert len(query_resp) == 1
    # Two (2) topics correspond to this query
    assert len(query_resp[0].topics) == 1
    # The target topics are 'UPLOADED_IMU_FRONT_TOPIC' and 'UPLOADED_IMU_CAMERA_TOPIC'
    expected_topic_name = UPLOADED_GPS_TOPIC

    _validate_returned_topic_name(query_resp[0].topics[0].name)
    assert query_resp[0].topics[0].name == expected_topic_name

    # free resources
    mosaico_client.close()


def test_query_ontology_between(
    mosaico_client: MosaicoClient,
    inject_synthetic_sequence,  # Ensure the data are available on the data platform
):
    # Query by single condition
    query_resp = mosaico_client.query(
        QueryOntologyCatalog().with_expression(
            IMU.Q.acceleration.x.between([0.0, 1.0])
        )  # set a very small value (data are random, so a small value is likely to be found)
    )
    # We do expect a successful query
    assert query_resp is not None and not query_resp.is_empty()
    # One (1) sequence corresponds to this query
    assert len(query_resp) == 1
    # The target topics are 'UPLOADED_IMU_FRONT_TOPIC' and 'UPLOADED_IMU_CAMERA_TOPIC'
    expected_topic_names = [
        UPLOADED_IMU_FRONT_TOPIC,
        UPLOADED_IMU_CAMERA_TOPIC,
    ]
    assert len(query_resp[0].topics) == len(expected_topic_names)
    # all the expected topics, and only them
    [_validate_returned_topic_name(topic.name) for topic in query_resp[0].topics]
    assert all([t.name in expected_topic_names for t in query_resp[0].topics])

    # Query by mixed conditions
    query_resp = mosaico_client.query(
        QueryOntologyCatalog().with_expression(
            IMU.Q.acceleration.x.between([0.0, 1.0])
        ),  # set a very small value (data are random, so a small value is likely to be found)
        QueryTopic().with_name_match("camera/left"),
    )
    # We do expect a successful query
    assert query_resp is not None and not query_resp.is_empty()
    # One (1) sequence corresponds to this query
    assert len(query_resp) == 1
    # The target topics are 'UPLOADED_IMU_FRONT_TOPIC' and 'UPLOADED_IMU_CAMERA_TOPIC'
    expected_topic_names = [
        UPLOADED_IMU_CAMERA_TOPIC,
    ]
    assert len(query_resp[0].topics) == len(expected_topic_names)
    # all the expected topics, and only them
    [_validate_returned_topic_name(topic.name) for topic in query_resp[0].topics]
    assert all([t.name in expected_topic_names for t in query_resp[0].topics])

    # free resources
    mosaico_client.close()


def test_mixed_query_ontology(
    mosaico_client: MosaicoClient,
    inject_synthetic_sequence,  # Ensure the data are available on the data platform
):
    # Query by multiple condition: time, topic metadata and sequence name
    tstamp = Time.from_float(1700000000.26)
    query_resp = mosaico_client.query(
        QueryOntologyCatalog().with_expression(
            IMU.Q.timestamp_ns.geq(tstamp.to_nanoseconds())
        ),
        QueryTopic().with_user_metadata("sensor_id", eq="imu_front_01"),
        QuerySequence().with_name(UPLOADED_SEQUENCE_NAME),
    )
    # We do expect a successful query
    assert query_resp is not None and not query_resp.is_empty()
    # One (1) sequence corresponds to this query
    assert len(query_resp) == 1
    # Two (2) topics correspond to this query
    assert len(query_resp[0].topics) == 1
    # The target topics are 'UPLOADED_IMU_FRONT_TOPIC' and 'UPLOADED_IMU_CAMERA_TOPIC'
    expected_topic_name = UPLOADED_IMU_FRONT_TOPIC
    _validate_returned_topic_name(query_resp[0].topics[0].name)
    assert query_resp[0].topics[0].name == expected_topic_name

    # Query by multiple condition: value and topic metadata
    tstamp = Time.from_float(1700000000.26)
    query_resp = mosaico_client.query(
        QueryOntologyCatalog().with_expression(GPS.Q.status.service.geq(1)),
        QueryTopic().with_user_metadata("interface.type", eq="UART"),
    )
    # We do expect a successful query
    assert query_resp is not None and not query_resp.is_empty()
    # One (1) sequence corresponds to this query
    assert len(query_resp) == 1
    # Two (2) topics correspond to this query
    assert len(query_resp[0].topics) == 1
    # The target topics are 'UPLOADED_IMU_FRONT_TOPIC' and 'UPLOADED_IMU_CAMERA_TOPIC'
    expected_topic_name = UPLOADED_GPS_TOPIC
    _validate_returned_topic_name(query_resp[0].topics[0].name)
    assert query_resp[0].topics[0].name == expected_topic_name

    # free resources
    mosaico_client.close()


def test_mixed_query_no_return(
    mosaico_client: MosaicoClient,
    inject_synthetic_sequence,  # Ensure the data are available on the data platform
):
    # Query by multiple condition: value and topic metadata
    query_resp = mosaico_client.query(
        QueryOntologyCatalog().with_expression(GPS.Q.status.service.geq(1)),
        QueryTopic().with_user_metadata("interface.type", eq="UART"),
        QuerySequence().with_name("nonexisting-seq"),
    )
    # We do expect a successful query
    assert query_resp is not None
    # One (1) sequence corresponds to this query
    assert len(query_resp) == 0

    # free resources
    mosaico_client.close()


def test_query_multi_tag_ontology(
    mosaico_client: MosaicoClient,
    inject_synthetic_sequence,  # Ensure the data are available on the data platform
):
    # Query by multiple condition: time and value
    query_resp = mosaico_client.query(
        QueryOntologyCatalog()
        .with_expression(IMU.Q.timestamp_ns.gt(0))
        .with_expression(GPS.Q.status.service.geq(1))
    )

    assert query_resp is not None and not query_resp.is_empty()

    # Check sequence
    assert len(query_resp) == 1

    for item in query_resp.items:
        topic_names = [t.name for t in item.topics]
        assert UPLOADED_GPS_TOPIC in topic_names
        assert UPLOADED_IMU_CAMERA_TOPIC in topic_names
        assert UPLOADED_IMU_FRONT_TOPIC in topic_names
        assert len(item.topics) == 3

    # free resources
    mosaico_client.close()
