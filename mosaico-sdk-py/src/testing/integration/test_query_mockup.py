from mosaicolabs.comm import MosaicoClient
from mosaicolabs.models.platform import Sequence
from mosaicolabs.models.query import QuerySequence
import pytest
from testing.integration.config import (
    QUERY_SEQUENCES_MOCKUP,
)
from .helpers import _validate_returned_topic_name

# ------ Tests with mockup ----


@pytest.mark.parametrize("sequence_name", list(QUERY_SEQUENCES_MOCKUP.keys()))
def test_query_mockup_sequence_by_name(
    _client: MosaicoClient,
    sequence_name,
    _inject_sequences_mockup,  # Ensure the data are available on the data platform
):
    # Trivial: query by topic name
    query_resp = _client.query(QuerySequence().with_name(sequence_name))
    # We do expect a successful query
    assert query_resp is not None
    # One (1) sequence corresponds to this query
    assert len(query_resp) == 1
    assert query_resp[0].sequence == sequence_name
    # We expect to obtain all the topics
    topics = [t["name"] for t in QUERY_SEQUENCES_MOCKUP[sequence_name]["topics"]]
    expected_topic_names = topics
    assert len(query_resp[0].topics) == len(expected_topic_names)
    expected_topic_names = [topic for topic in expected_topic_names]
    # all the expected topics, and only them
    [_validate_returned_topic_name(topic) for topic in query_resp[0].topics]
    assert all([t for t in query_resp[0].topics if t in expected_topic_names])
    assert all([t for t in expected_topic_names if t in query_resp[0].topics])

    # Query by partial name
    n_char = int(len(sequence_name) / 2)  # half the length
    seqname_substr = sequence_name[:n_char]
    query_resp = _client.query(QuerySequence().with_name_match(seqname_substr))
    # We do expect a successful query
    assert query_resp is not None
    matches = [
        sname for sname in QUERY_SEQUENCES_MOCKUP.keys() if seqname_substr in sname
    ]
    assert len(query_resp) == len(matches)
    for item in query_resp:
        seqname = item.sequence
        topics = [t["name"] for t in QUERY_SEQUENCES_MOCKUP[seqname]["topics"]]
        expected_topic_names = topics
        assert len(item.topics) == len(expected_topic_names)
        expected_topic_names = [topic for topic in expected_topic_names]
        # all the expected topics, and only them
        [_validate_returned_topic_name(topic) for topic in item.topics]
        assert all([t for t in item.topics if t in expected_topic_names])
        assert all([t for t in expected_topic_names if t in item.topics])

    # Query by partial name: startswith
    n_char = int(len(sequence_name) / 2)  # half the length
    seqname_substr = sequence_name[:n_char]
    query_resp = _client.query(QuerySequence().with_name_match(seqname_substr))
    # We do expect a successful query
    assert query_resp is not None
    matches = [
        sname
        for sname in QUERY_SEQUENCES_MOCKUP.keys()
        if sname.startswith(seqname_substr)
    ]
    assert len(query_resp) == len(matches)
    for item in query_resp:
        seqname = item.sequence
        topics = [t["name"] for t in QUERY_SEQUENCES_MOCKUP[seqname]["topics"]]
        expected_topic_names = topics
        assert len(item.topics) == len(expected_topic_names)
        expected_topic_names = [topic for topic in expected_topic_names]
        # all the expected topics, and only them
        [_validate_returned_topic_name(topic) for topic in item.topics]
        assert all([t for t in item.topics if t in expected_topic_names])
        assert all([t for t in expected_topic_names if t in item.topics])

    # Query by partial name: endswith
    n_char = int(len(sequence_name) / 2)  # half the length
    seqname_substr = sequence_name[-n_char:]
    query_resp = _client.query(QuerySequence().with_name_match(seqname_substr))
    # We do expect a successful query
    assert query_resp is not None
    matches = [
        sname
        for sname in QUERY_SEQUENCES_MOCKUP.keys()
        if sname.endswith(seqname_substr)
    ]
    assert len(query_resp) == len(matches)
    for item in query_resp:
        seqname = item.sequence
        topics = [t["name"] for t in QUERY_SEQUENCES_MOCKUP[seqname]["topics"]]
        expected_topic_names = topics
        assert len(item.topics) == len(expected_topic_names)
        expected_topic_names = [topic for topic in expected_topic_names]
        # all the expected topics, and only them
        [_validate_returned_topic_name(topic) for topic in item.topics]
        assert all([t for t in item.topics if t in expected_topic_names])
        assert all([t for t in expected_topic_names if t in item.topics])


def test_query_mockup_sequence_metadata(
    _client: MosaicoClient,
    _inject_sequences_mockup,  # Ensure the data are available on the data platform
):
    # Test 1: with single condition
    sequence_name_pattern = "test-query-"
    query_resp = _client.query(
        QuerySequence()
        .with_expression(Sequence.Q.user_metadata["status"].eq("raw"))
        .with_expression(Sequence.Q.user_metadata["visibility"].eq("private"))
        .with_name_match(sequence_name_pattern)
    )
    expected_sequence_name = "test-query-sequence-2"
    # We do expect a successful query
    assert query_resp is not None
    # One (1) sequence corresponds to this query
    assert len(query_resp) == 1
    assert query_resp[0].sequence == expected_sequence_name
    # We expect to obtain all the topics
    topics = [
        t["name"] for t in QUERY_SEQUENCES_MOCKUP[expected_sequence_name]["topics"]
    ]
    expected_topic_names = topics
    assert len(query_resp[0].topics) == len(expected_topic_names)
    expected_topic_names = [topic for topic in expected_topic_names]
    # all the expected topics, and only them
    [_validate_returned_topic_name(topic) for topic in query_resp[0].topics]
    assert all([t for t in query_resp[0].topics if t in expected_topic_names])
    assert all([t for t in expected_topic_names if t in query_resp[0].topics])

    # Test 2: with None return
    query_resp = _client.query(
        QuerySequence()
        .with_expression(Sequence.Q.user_metadata["status"].eq("processed"))
        .with_expression(Sequence.Q.user_metadata["visibility"].eq("public"))
    )

    assert query_resp is not None
    assert len(query_resp) == 0
