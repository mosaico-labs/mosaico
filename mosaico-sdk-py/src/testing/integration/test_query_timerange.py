from mosaicolabs.comm import MosaicoClient
from mosaicolabs.models import Time
from mosaicolabs.models.query import QueryOntologyCatalog, QueryTopic
from mosaicolabs.models.sensors import IMU, GPS
from testing.integration.config import (
    UPLOADED_GPS_TOPIC,
    UPLOADED_IMU_CAMERA_TOPIC,
    UPLOADED_IMU_FRONT_TOPIC,
)
from .helpers import _validate_returned_topic_name


def test_query_ontology_with_timestamp_trivial(
    _client: MosaicoClient,
    _inject_sequence_data_stream,  # Ensure the data are available on the data platform
):
    # Query by multiple condition: time and value
    query_resp = _client.query(
        QueryOntologyCatalog(include_timestamp_range=True).with_expression(
            IMU.Q.acceleration.x.gt(0)
        ),
    )

    assert query_resp is not None and not query_resp.is_empty()

    # Check sequence
    assert len(query_resp) == 1

    expected_topic_names = [
        UPLOADED_IMU_FRONT_TOPIC,
        UPLOADED_IMU_CAMERA_TOPIC,
    ]
    assert len(query_resp[0].topics) == len(expected_topic_names)
    # all the expected topics, and only them
    [_validate_returned_topic_name(topic.name) for topic in query_resp[0].topics]
    assert all([t.name in expected_topic_names for t in query_resp[0].topics])

    # NOTE:(v0.1) data is hardcoded in tests!
    # assert expected data for UPLOADED_IMU_CAMERA_TOPIC
    expected_timerange = [5000000, 485000000]
    trange = next(
        t.timestamp_range
        for t in query_resp[0].topics
        if t.name == UPLOADED_IMU_CAMERA_TOPIC
    )
    assert trange is not None
    assert trange.start == expected_timerange[0]
    assert trange.end == expected_timerange[1]

    # assert expected data for UPLOADED_IMU_FRONT_TOPIC
    expected_timerange = [0, 480000000]
    trange = next(
        t.timestamp_range
        for t in query_resp[0].topics
        if t.name == UPLOADED_IMU_FRONT_TOPIC
    )
    assert trange is not None
    assert trange.start == expected_timerange[0]
    assert trange.end == expected_timerange[1]

    # free resources
    _client.close()


def test_query_ontology_with_timestamp_imu(
    _client: MosaicoClient,
    _inject_sequence_data_stream,  # Ensure the data are available on the data platform
):
    # Query by multiple condition: time and value
    tstamp = Time.from_float(1700000000.26)
    query_resp = _client.query(
        QueryOntologyCatalog(include_timestamp_range=True)
        .with_expression(IMU.Q.header.stamp.sec.eq(tstamp.sec))
        .with_expression(IMU.Q.header.stamp.nanosec.geq(tstamp.nanosec)),
    )

    assert query_resp is not None and not query_resp.is_empty()

    # Check sequence
    assert len(query_resp) == 1

    expected_topic_names = [
        UPLOADED_IMU_FRONT_TOPIC,
        UPLOADED_IMU_CAMERA_TOPIC,
    ]
    assert len(query_resp[0].topics) == len(expected_topic_names)
    # all the expected topics, and only them
    [_validate_returned_topic_name(topic.name) for topic in query_resp[0].topics]
    assert all([t.name in expected_topic_names for t in query_resp[0].topics])

    # NOTE:(v0.1) data is hardcoded in tests!
    # assert expected data for UPLOADED_IMU_CAMERA_TOPIC
    expected_timerange = [265000000, 485000000]
    trange = next(
        t.timestamp_range
        for t in query_resp[0].topics
        if t.name == UPLOADED_IMU_CAMERA_TOPIC
    )
    assert trange is not None
    assert trange.start == expected_timerange[0]
    assert trange.end == expected_timerange[1]

    # assert expected data for UPLOADED_IMU_FRONT_TOPIC
    expected_timerange = [260000000, 480000000]
    trange = next(
        t.timestamp_range
        for t in query_resp[0].topics
        if t.name == UPLOADED_IMU_FRONT_TOPIC
    )
    assert trange is not None
    assert trange.start == expected_timerange[0]
    assert trange.end == expected_timerange[1]

    # free resources
    _client.close()


def test_query_mixed_ontology_with_timestamp(
    _client: MosaicoClient,
    _inject_sequence_data_stream,  # Ensure the data are available on the data platform
):
    # Query by multiple condition: time and value
    tstamp = Time.from_float(1700000000.33)
    query_resp = _client.query(
        QueryOntologyCatalog(include_timestamp_range=True)
        .with_expression(IMU.Q.acceleration.x.geq(0))
        .with_expression(GPS.Q.header.stamp.sec.geq(tstamp.sec))
        .with_expression(GPS.Q.header.stamp.nanosec.geq(tstamp.nanosec)),
    )

    assert query_resp is not None and not query_resp.is_empty()

    # Check sequence
    assert len(query_resp) == 1

    expected_topic_names = [
        UPLOADED_IMU_FRONT_TOPIC,
        UPLOADED_IMU_CAMERA_TOPIC,
        UPLOADED_GPS_TOPIC,
    ]
    assert len(query_resp[0].topics) == len(expected_topic_names)
    # all the expected topics, and only them
    [_validate_returned_topic_name(topic.name) for topic in query_resp[0].topics]
    assert all([t.name in expected_topic_names for t in query_resp[0].topics])

    # NOTE:(v0.1) data is hardcoded in tests!
    # assert expected data for UPLOADED_IMU_CAMERA_TOPIC
    expected_timerange = [5000000, 485000000]
    trange = next(
        t.timestamp_range
        for t in query_resp[0].topics
        if t.name == UPLOADED_IMU_CAMERA_TOPIC
    )
    assert trange is not None
    assert trange.start == expected_timerange[0]
    assert trange.end == expected_timerange[1]

    # assert expected data for UPLOADED_IMU_FRONT_TOPIC
    expected_timerange = [0, 480000000]
    trange = next(
        t.timestamp_range
        for t in query_resp[0].topics
        if t.name == UPLOADED_IMU_FRONT_TOPIC
    )
    assert trange is not None
    assert trange.start == expected_timerange[0]
    assert trange.end == expected_timerange[1]

    # assert expected data for UPLOADED_GPS_TOPIC
    expected_timerange = [330000000, 490000000]
    trange = next(
        t.timestamp_range for t in query_resp[0].topics if t.name == UPLOADED_GPS_TOPIC
    )
    assert trange is not None
    assert trange.start == expected_timerange[0]
    assert trange.end == expected_timerange[1]

    # free resources
    _client.close()


def test_query_multi_criteria_with_timestamp(
    _client: MosaicoClient,
    _inject_sequence_data_stream,  # Ensure the data are available on the data platform
):
    # Query by multiple condition: time and value
    tstamp = Time.from_float(1700000000.33)
    query_resp = _client.query(
        QueryOntologyCatalog(include_timestamp_range=True)
        .with_expression(IMU.Q.header.stamp.sec.geq(tstamp.sec))
        .with_expression(IMU.Q.header.stamp.nanosec.geq(tstamp.nanosec)),
        QueryTopic().with_name(UPLOADED_IMU_FRONT_TOPIC),
    )

    assert query_resp is not None and not query_resp.is_empty()

    # Check sequence
    assert len(query_resp) == 1

    expected_topic_names = [
        UPLOADED_IMU_FRONT_TOPIC,
    ]
    assert len(query_resp[0].topics) == len(expected_topic_names)
    # all the expected topics, and only them
    [_validate_returned_topic_name(topic.name) for topic in query_resp[0].topics]
    assert all([t.name in expected_topic_names for t in query_resp[0].topics])

    # NOTE:(v0.1) data is hardcoded in tests!
    # assert expected data for UPLOADED_IMU_FRONT_TOPIC
    expected_timerange = [340000000, 480000000]
    trange = next(
        t.timestamp_range
        for t in query_resp[0].topics
        if t.name == UPLOADED_IMU_FRONT_TOPIC
    )
    assert trange is not None
    assert trange.start == expected_timerange[0]
    assert trange.end == expected_timerange[1]

    # free resources
    _client.close()
