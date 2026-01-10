from mosaicolabs.comm import MosaicoClient
from mosaicolabs.models import Time
from mosaicolabs.models.platform import Topic
from mosaicolabs.models.query import QuerySequence, QueryTopic
import pytest
from testing.integration.config import (
    UPLOADED_IMU_CAMERA_TOPIC,
    UPLOADED_IMU_FRONT_TOPIC,
    UPLOADED_SEQUENCE_NAME,
)
from .helpers import (
    topic_to_metadata_dict,
    topic_to_ontology_class_dict,
    _validate_returned_topic_name,
)


@pytest.mark.parametrize("topic_name", list(topic_to_metadata_dict.keys()))
def test_query_topic_by_name(
    _client: MosaicoClient,
    topic_name,
    _inject_sequence_data_stream,  # Ensure the data are available on the data platform
):
    # Trivial: query by topic name
    query_resp = _client.query(QueryTopic().with_name_match(topic_name))
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
    _client.close()


def test_query_topic_by_creation_timestamp(
    _client: MosaicoClient,
    _inject_sequence_data_stream,  # Ensure the data are available on the data platform
):
    # Query by creation time, up to now (the sequence has been pushed few seconds ago)
    query_resp = _client.query(
        QuerySequence().with_name(
            UPLOADED_SEQUENCE_NAME
        ),  # limit to this sequence for avoiding other sequences created by other tests (ensure controllability)
        QueryTopic().with_created_timestamp(time_end=Time.now()),
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
    _client.close()


@pytest.mark.parametrize("topic_name", list(topic_to_metadata_dict.keys()))
def test_query_topic_by_sensor_tag(
    _client: MosaicoClient,
    topic_name,
    _inject_sequence_data_stream,  # Ensure the data are available on the data platform
):
    # Query by ontology_tag
    ontology_tag = topic_to_ontology_class_dict[topic_name].ontology_tag()
    query_resp = _client.query(
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
    _client.close()


@pytest.mark.parametrize("topic_name", list(topic_to_metadata_dict.keys()))
def test_query_topic_multi_criteria(
    _client: MosaicoClient,
    topic_name,
    _inject_sequence_data_stream,  # Ensure the data are available on the data platform
):
    # Test with multiple criteria
    # Query by ontology_tag
    ontology_tag = topic_to_ontology_class_dict[topic_name].ontology_tag()
    query_resp = _client.query(
        QuerySequence().with_name(
            UPLOADED_SEQUENCE_NAME
        ),  # limit to this sequence for avoiding other sequences created by other tests (ensure controllability)
        QueryTopic()
        .with_ontology_tag(ontology_tag)
        .with_created_timestamp(time_end=Time.now()),
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
    query_resp = _client.query(
        QuerySequence().with_name(
            UPLOADED_SEQUENCE_NAME
        ),  # limit to this sequence for avoiding other sequences created by other tests (ensure controllability)
        QueryTopic()
        .with_ontology_tag(ontology_tag)
        .with_created_timestamp(
            time_start=time_now_minus_10m,
            time_end=time_now_plus_1m,
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
    _client.close()


def test_query_topic_metadata(
    _client: MosaicoClient,
    _inject_sequence_data_stream,  # Ensure the data are available on the data platform
):
    # Trivial: query by topic name
    query_resp = _client.query(
        QueryTopic().with_expression(
            Topic.Q.user_metadata["serial_number"].eq("IMUF-9A31D72X")
        )
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

    # Test with single condition
    query_resp = _client.query(
        QueryTopic().with_expression(
            Topic.Q.user_metadata["serial_number"].eq("IMUF-9A31D72X")
        )
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
    query_resp = _client.query(
        QueryTopic()
        .with_expression(Topic.Q.user_metadata["serial_number"].eq("IMUF-9A31D72X"))
        .with_expression(Topic.Q.user_metadata["bias_stability"].gt(0.01))
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
    query_resp = _client.query(
        QueryTopic().with_expression(Topic.Q.user_metadata["bias_stability"].geq(0.01))
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
    query_resp = _client.query(
        QueryTopic().with_expression(
            Topic.Q.user_metadata["interface.type"].eq("Ethernet")
        )
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

    # free resources
    _client.close()


@pytest.mark.parametrize("topic_name", list(topic_to_metadata_dict.keys()))
def test_query_topic_from_response(
    _client: MosaicoClient,
    topic_name,
    _inject_sequence_data_stream,  # Ensure the data are available on the data platform
):
    # Trivial: query by topic name
    query_resp = _client.query(QueryTopic().with_name_match(topic_name))
    # We do expect a successful query
    assert query_resp is not None and not query_resp.is_empty()
    # The other criteria have been tested above...
    # This translates to:
    # 'query among the topics included in the returned response'
    qtopic = query_resp.to_query_topic()
    query_resp = _client.query(qtopic)
    assert query_resp is not None and not query_resp.is_empty()
    assert len(query_resp) == 1
    # One (1) topic corresponds to this query
    assert len(query_resp[0].topics) == 1

    _validate_returned_topic_name(query_resp[0].topics[0].name)
    assert query_resp[0].topics[0].name == topic_name

    # Try a trivial query with a further expression
    query_resp = _client.query(qtopic.with_created_timestamp(time_end=Time.now()))
    assert query_resp is not None and not query_resp.is_empty()
    assert len(query_resp) == 1
    # One (1) topic corresponds to this query
    assert len(query_resp[0].topics) == 1

    _validate_returned_topic_name(query_resp[0].topics[0].name)
    assert query_resp[0].topics[0].name == topic_name

    # free resources
    _client.close()


@pytest.mark.parametrize("topic_name", list(topic_to_metadata_dict.keys()))
def test_query_topic_from_response_fail(
    _client: MosaicoClient,
    topic_name,
    _inject_sequence_data_stream,  # Ensure the data are available on the data platform
):
    # Trivial: query by topic name
    query_resp = _client.query(QueryTopic().with_name_match(topic_name))
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
        query_resp = _client.query(qtopic.with_name_match(""))

    # free resources
    _client.close()
