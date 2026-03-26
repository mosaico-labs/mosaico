from mosaicolabs.comm import MosaicoClient
from mosaicolabs.models.query import QueryOntologyCatalog, QueryTopic
from mosaicolabs.models.sensors import GPS, IMU
from mosaicolabs.types import Time
from testing.integration.config import (
    UPLOADED_GPS_TOPIC,
    UPLOADED_IMU_CAMERA_TOPIC,
    UPLOADED_IMU_FRONT_TOPIC,
)

from .helpers import _validate_returned_topic_name


def test_query_ontology_with_timestamp_trivial(
    mosaico_client: MosaicoClient,
    inject_synthetic_sequence,  # Ensure the data are available on the data platform
):
    # Query by multiple condition: time and value
    query_resp = mosaico_client.query(
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
    expected_timerange = [1700000000005000000, 1700000000485000000]
    trange = next(
        t.timestamp_range
        for t in query_resp[0].topics
        if t.name == UPLOADED_IMU_CAMERA_TOPIC
    )
    assert trange is not None
    assert trange.start == expected_timerange[0]
    assert trange.end == expected_timerange[1]

    # assert expected data for UPLOADED_IMU_FRONT_TOPIC
    expected_timerange = [1700000000000000000, 1700000000480000000]
    trange = next(
        t.timestamp_range
        for t in query_resp[0].topics
        if t.name == UPLOADED_IMU_FRONT_TOPIC
    )
    assert trange is not None
    assert trange.start == expected_timerange[0]
    assert trange.end == expected_timerange[1]

    # free resources
    mosaico_client.close()


def test_query_ontology_with_timestamp_imu(
    mosaico_client: MosaicoClient,
    inject_synthetic_sequence,  # Ensure the data are available on the data platform
):
    # Query by multiple condition: time and value
    tstamp = Time.from_float(1700000000.26)
    query_resp = mosaico_client.query(
        QueryOntologyCatalog(include_timestamp_range=True).with_expression(
            IMU.Q.timestamp_ns.geq(tstamp.to_nanoseconds())
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
    expected_timerange = [1700000000265000000, 1700000000485000000]
    trange = next(
        t.timestamp_range
        for t in query_resp[0].topics
        if t.name == UPLOADED_IMU_CAMERA_TOPIC
    )
    assert trange is not None
    assert trange.start == expected_timerange[0]
    assert trange.end == expected_timerange[1]

    # assert expected data for UPLOADED_IMU_FRONT_TOPIC
    expected_timerange = [1700000000260000000, 1700000000480000000]
    trange = next(
        t.timestamp_range
        for t in query_resp[0].topics
        if t.name == UPLOADED_IMU_FRONT_TOPIC
    )
    assert trange is not None
    assert trange.start == expected_timerange[0]
    assert trange.end == expected_timerange[1]

    # free resources
    mosaico_client.close()


def test_query_mixed_ontology_with_timestamp(
    mosaico_client: MosaicoClient,
    inject_synthetic_sequence,  # Ensure the data are available on the data platform
):
    # Query by multiple condition: time and value
    tstamp = Time.from_float(1700000000.33)
    query_resp = mosaico_client.query(
        QueryOntologyCatalog(include_timestamp_range=True)
        .with_expression(IMU.Q.acceleration.x.geq(0))
        .with_expression(GPS.Q.timestamp_ns.geq(tstamp.to_nanoseconds())),
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
    expected_timerange = [1700000000005000000, 1700000000485000000]
    trange = next(
        t.timestamp_range
        for t in query_resp[0].topics
        if t.name == UPLOADED_IMU_CAMERA_TOPIC
    )
    assert trange is not None
    assert trange.start == expected_timerange[0]
    assert trange.end == expected_timerange[1]

    # assert expected data for UPLOADED_IMU_FRONT_TOPIC
    expected_timerange = [1700000000000000000, 1700000000480000000]
    trange = next(
        t.timestamp_range
        for t in query_resp[0].topics
        if t.name == UPLOADED_IMU_FRONT_TOPIC
    )
    assert trange is not None
    assert trange.start == expected_timerange[0]
    assert trange.end == expected_timerange[1]

    # assert expected data for UPLOADED_GPS_TOPIC
    expected_timerange = [1700000000330000000, 1700000000490000000]
    trange = next(
        t.timestamp_range for t in query_resp[0].topics if t.name == UPLOADED_GPS_TOPIC
    )
    assert trange is not None
    assert trange.start == expected_timerange[0]
    assert trange.end == expected_timerange[1]

    # free resources
    mosaico_client.close()


def test_query_multi_criteria_with_timestamp(
    mosaico_client: MosaicoClient,
    inject_synthetic_sequence,  # Ensure the data are available on the data platform
):
    # Query by multiple condition: time and value
    tstamp = Time.from_float(1700000000.33)
    query_resp = mosaico_client.query(
        QueryOntologyCatalog(include_timestamp_range=True).with_expression(
            IMU.Q.timestamp_ns.geq(tstamp.to_nanoseconds())
        ),
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
    expected_timerange = [1700000000340000000, 1700000000480000000]
    trange = next(
        t.timestamp_range
        for t in query_resp[0].topics
        if t.name == UPLOADED_IMU_FRONT_TOPIC
    )
    assert trange is not None
    assert trange.start == expected_timerange[0]
    assert trange.end == expected_timerange[1]

    # free resources
    mosaico_client.close()
