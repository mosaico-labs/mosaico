import pytest

from mosaicolabs import Time
from mosaicolabs.comm import MosaicoClient
from mosaicolabs.models.query import QuerySequence, QueryTopic
from testing.integration.config import (
    QUERY_SEQUENCES_MOCKUP,
    UPLOADED_GPS_TOPIC,
    UPLOADED_IMU_CAMERA_TOPIC,
    UPLOADED_IMU_FRONT_TOPIC,
    UPLOADED_MAGNETOMETER_TOPIC,
    UPLOADED_ROBOT_JOINTS_TOPIC,
    UPLOADED_ROBOT_PATH_TOPIC,
    UPLOADED_SEQUENCE_NAME,
    UPLOADED_TEMPERATURE_TOPIC,
)

from .helpers import (
    _validate_returned_topic_name,
    topic_to_metadata_dict,
    topic_to_ontology_class_dict,
)


@pytest.mark.parametrize("topic_name", list(topic_to_metadata_dict.keys()))
def test_query_topic_by_name(
    mosaico_client: MosaicoClient,
    topic_name,
    inject_synthetic_sequence,  # Ensure the data are available on the data platform
):
    # Trivial: query by topic name
    query_resp = mosaico_client.query(QueryTopic().with_name_match(topic_name))
    # We do expect a successful query
    assert query_resp is not None and not query_resp.is_empty()
    # One (1) sequence corresponds to this query
    assert len(query_resp) == 1
    # One (1) topic corresponds to this query
    assert len(query_resp[0].topics) == 1
    # The target topic is in 'topic_name'
    expected_topic_name = topic_name
    assert query_resp[0].topics[0].name == expected_topic_name
    _validate_returned_topic_name(query_resp[0].topics[0].name)

    # NOTE: the query 'with_name_match' is made via $match, so i am sure that this operator works;
    # The topics are stored with the resource name (seq/topic) so since this query by using
    # the topic name only succeeded, the operator works

    # free resources
    mosaico_client.close()


def test_query_topic_by_creation_timestamp(
    mosaico_client: MosaicoClient,
    inject_synthetic_sequence,  # Ensure the data are available on the data platform
):
    # Query by creation time, up to now (the sequence has been pushed few seconds ago)
    query_resp = mosaico_client.query(
        QuerySequence().with_name(
            UPLOADED_SEQUENCE_NAME
        ),  # limit to this sequence for avoiding other sequences created by other tests (ensure controllability)
        QueryTopic().with_created_timestamp(time_end=Time.now().to_nanoseconds()),
    )  # creation time <= now
    # We do expect a successful query
    assert query_resp is not None and not query_resp.is_empty()
    # One (1) sequence corresponds to this query
    assert len(query_resp) == 1
    # We expect to obtain all the topics
    expected_topic_names = list(topic_to_metadata_dict.keys())
    assert len(query_resp[0].topics) == len(expected_topic_names)

    # all the expected topics, and only them
    [_validate_returned_topic_name(topic.name) for topic in query_resp[0].topics]
    assert all([t.name in expected_topic_names for t in query_resp[0].topics])

    # free resources
    mosaico_client.close()


@pytest.mark.parametrize("topic_name", list(topic_to_metadata_dict.keys()))
def test_query_topic_by_sensor_tag(
    mosaico_client: MosaicoClient,
    topic_name,
    inject_synthetic_sequence,  # Ensure the data are available on the data platform
):
    # Query by ontology_tag
    ontology_tag = topic_to_ontology_class_dict[topic_name].ontology_tag()
    query_resp = mosaico_client.query(
        QuerySequence().with_name(
            UPLOADED_SEQUENCE_NAME
        ),  # limit to this sequence for avoiding other sequences created by other tests (ensure controllability)
        QueryTopic().with_ontology_tag(ontology_tag),
    )
    # We do expect a successful query
    assert query_resp is not None and not query_resp.is_empty()
    # One (1) sequence corresponds to this query
    assert len(query_resp) == 1
    # We expect to obtain all the topics with this ontology_tag
    expected_topic_names = [
        key
        for key, val in topic_to_ontology_class_dict.items()
        if val.ontology_tag() == ontology_tag
    ]
    # N topics may correspond to this query
    assert len(query_resp[0].topics) == len(expected_topic_names)
    # all the expected topics, and only them
    [_validate_returned_topic_name(topic.name) for topic in query_resp[0].topics]
    assert all([t.name in expected_topic_names for t in query_resp[0].topics])

    # free resources
    mosaico_client.close()


@pytest.mark.parametrize("topic_name", list(topic_to_metadata_dict.keys()))
def test_query_topic_multi_criteria(
    mosaico_client: MosaicoClient,
    topic_name,
    inject_synthetic_sequence,  # Ensure the data are available on the data platform
):
    # Test with multiple criteria
    # Query by ontology_tag
    ontology_tag = topic_to_ontology_class_dict[topic_name].ontology_tag()
    query_resp = mosaico_client.query(
        QuerySequence().with_name(
            UPLOADED_SEQUENCE_NAME
        ),  # limit to this sequence for avoiding other sequences created by other tests (ensure controllability)
        QueryTopic()
        .with_ontology_tag(ontology_tag)
        .with_created_timestamp(time_end=Time.now().to_nanoseconds()),
    )
    # We do expect a successful query
    assert query_resp is not None and not query_resp.is_empty()
    # One (1) sequence corresponds to this query
    assert len(query_resp) == 1
    # We expect to obtain all the topics with this ontology_tag
    expected_topic_names = [
        key
        for key, val in topic_to_ontology_class_dict.items()
        if val.ontology_tag() == ontology_tag
    ]
    # N topics may correspond to this query
    assert len(query_resp[0].topics) == len(expected_topic_names)
    # all the expected topics, and only them
    [_validate_returned_topic_name(topic.name) for topic in query_resp[0].topics]
    assert all([t.name in expected_topic_names for t in query_resp[0].topics])

    # Test with multiple criteria: trigger between
    # Query by ontology_tag
    ontology_tag = topic_to_ontology_class_dict[topic_name].ontology_tag()
    time_now_minus_10m = Time.from_float(
        Time.now().to_float() - 600.0
    )  # 10 minutes ago
    time_now_plus_1m = Time.from_float(
        Time.now().to_float() + 60.0
    )  # 1 minutes in the future
    query_resp = mosaico_client.query(
        QuerySequence().with_name(
            UPLOADED_SEQUENCE_NAME
        ),  # limit to this sequence for avoiding other sequences created by other tests (ensure controllability)
        QueryTopic()
        .with_ontology_tag(ontology_tag)
        .with_created_timestamp(
            time_start=time_now_minus_10m.to_nanoseconds(),
            time_end=time_now_plus_1m.to_nanoseconds(),
            # triggers '$between'
        ),
    )
    # We do expect a successful query
    assert query_resp is not None and not query_resp.is_empty()
    # One (1) sequence corresponds to this query
    assert len(query_resp) == 1
    # We expect to obtain all the topics with this ontology_tag
    expected_topic_names = [
        key
        for key, val in topic_to_ontology_class_dict.items()
        if val.ontology_tag() == ontology_tag
    ]
    # N topics may correspond to this query
    assert len(query_resp[0].topics) == len(expected_topic_names)
    # all the expected topics, and only them
    [_validate_returned_topic_name(topic.name) for topic in query_resp[0].topics]
    assert all([t.name in expected_topic_names for t in query_resp[0].topics])

    # free resources
    mosaico_client.close()


def test_query_topic_metadata(
    mosaico_client: MosaicoClient,
    inject_synthetic_sequence,  # Ensure the data are available on the data platform
):
    # Trivial: query by topic name
    query_resp = mosaico_client.query(
        QueryTopic().with_user_metadata("serial_number", eq="IMUF-9A31D72X")
    )
    # We do expect a successful query
    assert query_resp is not None and not query_resp.is_empty()
    # One (1) sequence corresponds to this query
    assert len(query_resp) == 1
    # One (1) topic corresponds to this query
    assert len(query_resp[0].topics) == 1
    # The target topic is 'UPLOADED_IMU_FRONT_TOPIC'
    expected_topic_name = UPLOADED_IMU_FRONT_TOPIC

    assert query_resp[0].topics[0].name == expected_topic_name
    _validate_returned_topic_name(query_resp[0].topics[0].name)

    # Test != operator on simple strings
    query_resp = mosaico_client.query(
        QueryTopic().with_user_metadata("calibration_version", neq="cal-2025.01.01"),
    )
    # We do expect a successful query
    assert query_resp is not None and not query_resp.is_empty()

    assert len(query_resp) == 1
    expected_topic_names = [
        UPLOADED_IMU_CAMERA_TOPIC,
        UPLOADED_GPS_TOPIC,
        UPLOADED_MAGNETOMETER_TOPIC,
    ]
    # N topics may correspond to this query
    assert len(query_resp[0].topics) == len(expected_topic_names)
    assert all([t.name in expected_topic_names for t in query_resp[0].topics])

    # Test with single condition
    query_resp = mosaico_client.query(
        QueryTopic().with_user_metadata("serial_number", eq="IMUF-9A31D72X")
    )
    # We do expect a successful query
    assert query_resp is not None and not query_resp.is_empty()
    # One (1) sequence corresponds to this query
    assert len(query_resp) == 1
    # One (1) topic corresponds to this query
    assert len(query_resp[0].topics) == 1
    # The target topic is 'UPLOADED_IMU_FRONT_TOPIC'
    expected_topic_name = UPLOADED_IMU_FRONT_TOPIC

    assert query_resp[0].topics[0].name == expected_topic_name
    _validate_returned_topic_name(query_resp[0].topics[0].name)

    # Test with multiple conditions
    query_resp = mosaico_client.query(
        QueryTopic()
        .with_user_metadata("serial_number", eq="IMUF-9A31D72X")
        .with_user_metadata("bias_stability", between=(0.005, 0.015))
    )
    # We do expect a successful query
    assert query_resp is not None and not query_resp.is_empty()
    # One (1) sequence corresponds to this query
    assert len(query_resp) == 1
    # One (1) topic corresponds to this query
    assert len(query_resp[0].topics) == 1
    # The target topic is 'UPLOADED_IMU_FRONT_TOPIC'
    expected_topic_name = UPLOADED_IMU_FRONT_TOPIC

    assert query_resp[0].topics[0].name == expected_topic_name
    _validate_returned_topic_name(query_resp[0].topics[0].name)

    # Test with multiple returned topic matches
    query_resp = mosaico_client.query(
        QueryTopic().with_user_metadata("bias_stability", geq=0.01)
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
    assert len(expected_topic_names) == len(query_resp[0].topics)
    # all the expected topics, and only them
    [_validate_returned_topic_name(topic.name) for topic in query_resp[0].topics]
    assert all([t.name in expected_topic_names for t in query_resp[0].topics])

    # Test with nested field
    query_resp = mosaico_client.query(
        QueryTopic().with_user_metadata("interface.type", eq="Ethernet")
    )
    # We do expect a successful query
    assert query_resp is not None and not query_resp.is_empty()
    # One (1) sequence corresponds to this query
    assert len(query_resp) == 1
    # One (1) topic corresponds to this query
    assert len(query_resp[0].topics) == 1
    # The target topic is 'UPLOADED_IMU_FRONT_TOPIC'
    expected_topic_name = UPLOADED_IMU_CAMERA_TOPIC

    _validate_returned_topic_name(query_resp[0].topics[0].name)
    assert query_resp[0].topics[0].name == expected_topic_name

    # Test 'ex' operator
    query_resp = mosaico_client.query(
        QueryTopic().with_user_metadata("accuracy_m", ex=True)
    )
    # We do expect a successful query
    assert query_resp is not None and not query_resp.is_empty()
    # One (1) sequence corresponds to this query
    assert len(query_resp) == 1
    # The target topic is 'UPLOADED_GPS_TOPIC'
    expected_topic_names = [
        UPLOADED_GPS_TOPIC,
    ]
    assert len(expected_topic_names) == len(query_resp[0].topics)
    # all the expected topics, and only them
    [_validate_returned_topic_name(topic.name) for topic in query_resp[0].topics]
    assert all([t.name in expected_topic_names for t in query_resp[0].topics])

    # Test 'nex' operator
    query_resp = mosaico_client.query(
        QueryTopic().with_user_metadata("accuracy_m", ex=False)
    )
    # We do expect a successful query
    assert query_resp is not None and not query_resp.is_empty()
    # Six (6) sequences corresponds to this query (data stream sequence + mockups + stream list sequence)
    assert len(query_resp) == 6
    # The target topics are 'UPLOADED_IMU_FRONT_TOPIC' and 'UPLOADED_IMU_CAMERA_TOPIC'
    expected_topic_names = [
        UPLOADED_IMU_FRONT_TOPIC,
        UPLOADED_IMU_CAMERA_TOPIC,
        UPLOADED_MAGNETOMETER_TOPIC,
        UPLOADED_ROBOT_JOINTS_TOPIC,
        UPLOADED_ROBOT_PATH_TOPIC,
        UPLOADED_TEMPERATURE_TOPIC,
    ] + [
        topic["name"]
        for sequence_info in QUERY_SEQUENCES_MOCKUP.values()
        for topic in sequence_info["topics"]
    ]
    assert len(expected_topic_names) == sum(len(resp.topics) for resp in query_resp)
    assert all(
        [
            topic.name in expected_topic_names
            for resp in query_resp
            for topic in resp.topics
        ]
    )

    # Test with nested field
    # free resources
    mosaico_client.close()


def test_query_topic_lexicographic_comparison(
    mosaico_client: MosaicoClient,
    inject_synthetic_sequence,  # Ensure the data are available on the data platform
):
    # Test > operator on simple strings
    query_resp = mosaico_client.query(
        QueryTopic().with_user_metadata("calibration_version", gt="cal-2025.04"),
    )
    # We do expect a successful query
    assert query_resp is not None and not query_resp.is_empty()
    # One (1) sequence corresponds to this query
    assert len(query_resp) == 1
    expected_topic_names = [UPLOADED_MAGNETOMETER_TOPIC]
    # N topics may correspond to this query
    assert len(query_resp[0].topics) == len(expected_topic_names)
    # all the expected topics, and only them
    [_validate_returned_topic_name(topic.name) for topic in query_resp[0].topics]
    assert all([t.name in expected_topic_names for t in query_resp[0].topics])

    # Test > operator on iso timestamps
    query_resp = mosaico_client.query(
        QueryTopic().with_user_metadata("last_calibrated_at", gt="2025-03-01T09"),
    )
    # We do expect a successful query
    assert query_resp is not None and not query_resp.is_empty()
    # One (1) sequence corresponds to this query
    assert len(query_resp) == 1
    expected_topic_names = [UPLOADED_MAGNETOMETER_TOPIC]
    # N topics may correspond to this query
    assert len(query_resp[0].topics) == len(expected_topic_names)
    # all the expected topics, and only them
    [_validate_returned_topic_name(topic.name) for topic in query_resp[0].topics]
    assert all([t.name in expected_topic_names for t in query_resp[0].topics])

    # Test <= operator on iso timestamps
    query_resp = mosaico_client.query(
        QueryTopic().with_user_metadata(
            "last_calibrated_at", leq="2025-03-01T07:17:08"
        ),
    )
    # We do expect a successful query
    assert query_resp is not None and not query_resp.is_empty()
    # One (1) sequence corresponds to this query
    assert len(query_resp) == 1
    expected_topic_names = [UPLOADED_IMU_CAMERA_TOPIC, UPLOADED_IMU_FRONT_TOPIC]
    # N topics may correspond to this query
    assert len(query_resp[0].topics) == len(expected_topic_names)
    # all the expected topics, and only them
    [_validate_returned_topic_name(topic.name) for topic in query_resp[0].topics]
    assert all([t.name in expected_topic_names for t in query_resp[0].topics])

    # free resources
    mosaico_client.close()


@pytest.mark.parametrize("topic_name", list(topic_to_metadata_dict.keys()))
def test_query_topic_from_response(
    mosaico_client: MosaicoClient,
    topic_name,
    inject_synthetic_sequence,  # Ensure the data are available on the data platform
):
    # Trivial: query by topic name
    query_resp = mosaico_client.query(QueryTopic().with_name_match(topic_name))
    # We do expect a successful query
    assert query_resp is not None and not query_resp.is_empty()
    # The other criteria have been tested above...
    # This translates to:
    # 'query among the topics included in the returned response'
    qtopic = query_resp.to_query_topic()
    query_resp = mosaico_client.query(qtopic)
    assert query_resp is not None and not query_resp.is_empty()
    assert len(query_resp) == 1
    # One (1) topic corresponds to this query
    assert len(query_resp[0].topics) == 1

    _validate_returned_topic_name(query_resp[0].topics[0].name)
    assert query_resp[0].topics[0].name == topic_name

    # Try a trivial query with a further expression
    query_resp = mosaico_client.query(
        qtopic.with_created_timestamp(time_end=Time.now().to_nanoseconds())
    )
    assert query_resp is not None and not query_resp.is_empty()
    assert len(query_resp) == 1
    # One (1) topic corresponds to this query
    assert len(query_resp[0].topics) == 1

    _validate_returned_topic_name(query_resp[0].topics[0].name)
    assert query_resp[0].topics[0].name == topic_name

    # free resources
    mosaico_client.close()


@pytest.mark.parametrize("topic_name", list(topic_to_metadata_dict.keys()))
def test_query_topic_from_response_fail(
    mosaico_client: MosaicoClient,
    topic_name,
    inject_synthetic_sequence,  # Ensure the data are available on the data platform
):
    # Trivial: query by topic name
    query_resp = mosaico_client.query(QueryTopic().with_name_match(topic_name))
    # We do expect a successful query
    assert query_resp is not None and not query_resp.is_empty()
    # The other criteria have been tested above...
    # This translates to:
    # 'query among the topics included in the returned response'
    qtopic = query_resp.to_query_topic()
    # This must fail: field 'name' is already queried
    with pytest.raises(
        NotImplementedError, match="Query builder already contains the key 'name'"
    ):
        query_resp = mosaico_client.query(qtopic.with_name_match(""))

    # free resources
    mosaico_client.close()
