from mosaicolabs.comm import MosaicoClient
from mosaicolabs.models import Time
from mosaicolabs.models.platform import Sequence
from mosaicolabs.models.query import QuerySequence
import pytest
from testing.integration.config import (
    UPLOADED_SEQUENCE_NAME,
)
from .helpers import topic_to_metadata_dict, _validate_returned_topic_name


def test_query_sequence_by_name(
    _client: MosaicoClient,
    _inject_sequence_data_stream,  # Ensure the data are available on the data platform
):
    # Trivial: query by topic name
    query_resp = _client.query(QuerySequence().with_name(UPLOADED_SEQUENCE_NAME))
    # We do expect a successful query
    assert query_resp is not None and not query_resp.is_empty()
    # One (1) sequence corresponds to this query
    assert len(query_resp) == 1
    assert query_resp[0].sequence.name == UPLOADED_SEQUENCE_NAME
    # We expect to obtain all the topics
    expected_topic_names = list(topic_to_metadata_dict.keys())
    assert len(query_resp[0].topics) == len(expected_topic_names)
    expected_topic_names = [topic for topic in expected_topic_names]
    # all the expected topics, and only them
    [_validate_returned_topic_name(topic.name) for topic in query_resp[0].topics]
    assert all([t.name in expected_topic_names for t in query_resp[0].topics])

    # Query by partial name (operator should be a match)
    n_char = int(len(UPLOADED_SEQUENCE_NAME) / 2)  # half the length
    query_resp = _client.query(
        QuerySequence().with_name_match(UPLOADED_SEQUENCE_NAME[:n_char])
    )
    # We do expect a successful query
    assert query_resp is not None and not query_resp.is_empty()
    # One (1) sequence corresponds to this query
    assert len(query_resp) == 1
    assert query_resp[0].sequence.name == UPLOADED_SEQUENCE_NAME
    # We expect to obtain all the topics
    expected_topic_names = list(topic_to_metadata_dict.keys())
    assert len(query_resp[0].topics) == len(expected_topic_names)
    expected_topic_names = [topic for topic in expected_topic_names]

    # all the expected topics, and only them
    [_validate_returned_topic_name(topic.name) for topic in query_resp[0].topics]
    assert all([t.name in expected_topic_names for t in query_resp[0].topics])

    # free resources
    _client.close()


def test_query_sequence_by_creation_timestamp(
    _client: MosaicoClient,
    _inject_sequence_data_stream,  # Ensure the data are available on the data platform
):
    # Query by creation time, up to now (the sequence has been pushed few seconds ago)
    query_resp = _client.query(
        QuerySequence()
        .with_name(
            UPLOADED_SEQUENCE_NAME
        )  # limit to this sequence for avoiding other sequences created by other tests (ensure controllability)
        .with_created_timestamp(time_end=Time.now())
    )  # creation time <= now
    # We do expect a successful query
    assert query_resp is not None and not query_resp.is_empty()
    # One (1) sequence corresponds to this query
    assert len(query_resp) == 1
    assert query_resp[0].sequence.name == UPLOADED_SEQUENCE_NAME
    # We expect to obtain all the topics
    expected_topic_names = list(topic_to_metadata_dict.keys())
    assert len(query_resp[0].topics) == len(expected_topic_names)
    expected_topic_names = [topic for topic in expected_topic_names]

    # all the expected topics, and only them
    [_validate_returned_topic_name(topic.name) for topic in query_resp[0].topics]
    assert all([t.name in expected_topic_names for t in query_resp[0].topics])

    # free resources
    _client.close()


def test_query_sequence_metadata(
    _client: MosaicoClient,
    _inject_sequence_data_stream,  # Ensure the data are available on the data platform
):
    # Test with single condition
    query_resp = _client.query(
        QuerySequence()
        .with_name(
            UPLOADED_SEQUENCE_NAME
        )  # limit to this sequence for avoiding other sequences created by other tests (ensure controllability)
        .with_expression(Sequence.Q.user_metadata["status"].eq("processed"))
    )
    # We do expect a successful query
    assert query_resp is not None and not query_resp.is_empty()
    # One (1) sequence corresponds to this query
    assert len(query_resp) == 1
    assert query_resp[0].sequence.name == UPLOADED_SEQUENCE_NAME
    # We expect to obtain all the topics
    expected_topic_names = list(topic_to_metadata_dict.keys())
    assert len(query_resp[0].topics) == len(expected_topic_names)
    expected_topic_names = [topic for topic in expected_topic_names]
    # all the expected topics, and only them
    [_validate_returned_topic_name(topic.name) for topic in query_resp[0].topics]
    assert all([t.name in expected_topic_names for t in query_resp[0].topics])

    # Test with multiple conditions
    query_resp = _client.query(
        QuerySequence()
        .with_expression(Sequence.Q.user_metadata["status"].eq("processed"))
        .with_expression(Sequence.Q.user_metadata["environment.weather"].eq("sunny"))
    )
    # We do expect a successful query
    assert query_resp is not None and not query_resp.is_empty()
    # One (1) sequence corresponds to this query
    assert len(query_resp) == 1
    # We expect to obtain all the topics
    expected_topic_names = list(topic_to_metadata_dict.keys())
    assert len(query_resp[0].topics) == len(expected_topic_names)
    expected_topic_names = [topic for topic in expected_topic_names]
    # all the expected topics, and only them
    [_validate_returned_topic_name(topic.name) for topic in query_resp[0].topics]
    assert all([t.name in expected_topic_names for t in query_resp[0].topics])

    # Test with nested-fields condition
    query_resp = _client.query(
        QuerySequence()
        .with_expression(Sequence.Q.user_metadata["location.city"].eq("Milan"))
        .with_expression(Sequence.Q.user_metadata["location.facility"].eq("Downtown"))
        .with_expression(
            Sequence.Q.user_metadata["vehicle.software_stack.planning"].eq("plan-4.1.7")
        )
    )
    # We do expect a successful query
    assert query_resp is not None and not query_resp.is_empty()
    # One (1) sequence corresponds to this query
    assert len(query_resp) == 1
    assert query_resp[0].sequence.name == UPLOADED_SEQUENCE_NAME
    # We expect to obtain all the topics
    expected_topic_names = list(topic_to_metadata_dict.keys())
    assert len(query_resp[0].topics) == len(expected_topic_names)
    expected_topic_names = [topic for topic in expected_topic_names]
    # all the expected topics, and only them
    [_validate_returned_topic_name(topic.name) for topic in query_resp[0].topics]
    assert all([t.name in expected_topic_names for t in query_resp[0].topics])

    # free resources
    _client.close()


def test_query_sequence_from_response(
    _client: MosaicoClient,
    _inject_sequence_data_stream,  # Ensure the data are available on the data platform
):
    # Query by creation time, up to now (the sequence has been pushed few seconds ago)
    query_resp = _client.query(
        QuerySequence()
        .with_name(
            UPLOADED_SEQUENCE_NAME
        )  # limit to this sequence for avoiding other sequences created by other tests (ensure controllability)
        .with_created_timestamp(time_end=Time.now())
    )  # creation time <= now
    # We do expect a successful query
    assert query_resp is not None and not query_resp.is_empty()
    # The other criteria have been tested above...
    # This translates to:
    # 'query among the sequences in the returned response'
    qsequence = query_resp.to_query_sequence()
    # simply reprovide the same query to the client
    query_resp = _client.query(qsequence)
    # One (1) sequence corresponds to this query
    assert query_resp is not None and not query_resp.is_empty()
    assert len(query_resp) == 1
    assert query_resp[0].sequence.name == UPLOADED_SEQUENCE_NAME
    # The other criteria have been tested above...

    # Try a trivial query with a further expression
    query_resp = _client.query(qsequence.with_created_timestamp(time_end=Time.now()))
    # One (1) sequence corresponds to this query
    assert query_resp is not None and not query_resp.is_empty()
    assert len(query_resp) == 1
    assert query_resp[0].sequence.name == UPLOADED_SEQUENCE_NAME

    # free resources
    _client.close()


def test_query_sequence_from_response_fail(
    _client: MosaicoClient,
    _inject_sequence_data_stream,  # Ensure the data are available on the data platform
):
    # Query by creation time, up to now (the sequence has been pushed few seconds ago)
    query_resp = _client.query(
        QuerySequence()
        .with_name(
            UPLOADED_SEQUENCE_NAME
        )  # limit to this sequence for avoiding other sequences created by other tests (ensure controllability)
        .with_created_timestamp(time_end=Time.now())
    )  # creation time <= now
    # We do expect a successful query
    assert query_resp is not None and not query_resp.is_empty()
    # The other criteria have been tested above...
    # This translates to:
    # 'query among the sequences in the returned response'
    qsequence = query_resp.to_query_sequence()
    # This must fail: field 'name' is already queried
    with pytest.raises(
        NotImplementedError, match="Query builder already contains the key 'name'"
    ):
        query_resp = _client.query(qsequence.with_name(""))
    # This must fail: field 'name' is already queried
    with pytest.raises(
        NotImplementedError, match="Query builder already contains the key 'name'"
    ):
        query_resp = _client.query(qsequence.with_name_match(""))

    # free resources
    _client.close()
