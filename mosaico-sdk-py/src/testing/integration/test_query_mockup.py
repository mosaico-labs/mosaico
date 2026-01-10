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
    assert query_resp is not None and not query_resp.is_empty()
    # One (1) sequence corresponds to this query
    assert len(query_resp) == 1
    assert query_resp[0].sequence.name == sequence_name
    # We expect to obtain all the topics
    topics = [t["name"] for t in QUERY_SEQUENCES_MOCKUP[sequence_name]["topics"]]
    expected_topic_names = topics
    assert len(query_resp[0].topics) == len(expected_topic_names)
    # all the expected topics, and only them
    [_validate_returned_topic_name(topic.name) for topic in query_resp[0].topics]
    assert all([t.name in expected_topic_names for t in query_resp[0].topics])

    # Query by partial name
    n_char = int(len(sequence_name) / 2)  # half the length
    seqname_substr = sequence_name[:n_char]
    query_resp = _client.query(QuerySequence().with_name_match(seqname_substr))
    # We do expect a successful query
    assert query_resp is not None and not query_resp.is_empty()
    matches = [
        sname for sname in QUERY_SEQUENCES_MOCKUP.keys() if seqname_substr in sname
    ]
    assert len(query_resp) == len(matches)
    for item in query_resp:
        seqname = item.sequence.name
        topics = [t["name"] for t in QUERY_SEQUENCES_MOCKUP[seqname]["topics"]]
        expected_topic_names = topics
        assert len(item.topics) == len(expected_topic_names)
        # all the expected topics, and only them
        [_validate_returned_topic_name(topic.name) for topic in item.topics]
        assert all([t.name in expected_topic_names for t in item.topics])

    # Query by partial name: startswith
    n_char = int(len(sequence_name) / 2)  # half the length
    seqname_substr = sequence_name[:n_char]
    query_resp = _client.query(QuerySequence().with_name_match(seqname_substr))
    # We do expect a successful query
    assert query_resp is not None and not query_resp.is_empty()
    matches = [
        sname
        for sname in QUERY_SEQUENCES_MOCKUP.keys()
        if sname.startswith(seqname_substr)
    ]
    assert len(query_resp) == len(matches)
    for item in query_resp:
        seqname = item.sequence.name
        topics = [t["name"] for t in QUERY_SEQUENCES_MOCKUP[seqname]["topics"]]
        expected_topic_names = topics
        assert len(item.topics) == len(expected_topic_names)
        # all the expected topics, and only them
        [_validate_returned_topic_name(topic.name) for topic in item.topics]
        assert all([t.name in expected_topic_names for t in item.topics])

    # Query by partial name: endswith
    n_char = int(len(sequence_name) / 2)  # half the length
    seqname_substr = sequence_name[-n_char:]
    query_resp = _client.query(QuerySequence().with_name_match(seqname_substr))
    # We do expect a successful query
    assert query_resp is not None and not query_resp.is_empty()
    matches = [
        sname
        for sname in QUERY_SEQUENCES_MOCKUP.keys()
        if sname.endswith(seqname_substr)
    ]
    assert len(query_resp) == len(matches)
    for item in query_resp:
        seqname = item.sequence.name
        topics = [t["name"] for t in QUERY_SEQUENCES_MOCKUP[seqname]["topics"]]
        expected_topic_names = topics
        assert len(item.topics) == len(expected_topic_names)
        # all the expected topics, and only them
        [_validate_returned_topic_name(topic.name) for topic in item.topics]
        assert all([t.name in expected_topic_names for t in item.topics])

    # free resources
    _client.close()


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
    assert query_resp is not None and not query_resp.is_empty()
    # One (1) sequence corresponds to this query
    assert len(query_resp) == 1
    assert query_resp[0].sequence.name == expected_sequence_name
    # We expect to obtain all the topics
    topics = [
        t["name"] for t in QUERY_SEQUENCES_MOCKUP[expected_sequence_name]["topics"]
    ]
    expected_topic_names = topics
    assert len(query_resp[0].topics) == len(expected_topic_names)
    # all the expected topics, and only them
    [_validate_returned_topic_name(topic.name) for topic in query_resp[0].topics]
    assert all([t.name in expected_topic_names for t in query_resp[0].topics])

    # Test 2: with None return
    query_resp = _client.query(
        QuerySequence()
        .with_expression(Sequence.Q.user_metadata["status"].eq("processed"))
        .with_expression(Sequence.Q.user_metadata["visibility"].eq("public"))
    )

    assert query_resp is not None
    assert len(query_resp) == 0

    # free resources
    _client.close()


def test_query_sequence_from_response(
    _client: MosaicoClient,
    _inject_sequences_mockup,  # Ensure the data are available on the data platform
):
    visibility_val = "private"
    query_resp = _client.query(
        QuerySequence().with_expression(
            Sequence.Q.user_metadata["visibility"].eq(visibility_val)
        )
    )
    # We do expect a successful query
    assert query_resp is not None and not query_resp.is_empty()
    # The other criteria have been tested above...
    expected_sequence_names = [
        key
        for key, val in QUERY_SEQUENCES_MOCKUP.items()
        if val.get("metadata", {}).get("visibility") == visibility_val
    ]
    assert len(query_resp) == len(expected_sequence_names)
    assert all([it.sequence.name in expected_sequence_names for it in query_resp])
    # This translates to:
    # 'query among the sequences in the returned response'
    qsequence = query_resp.to_query_sequence()
    # simply reprovide the same query to the client
    query_resp = _client.query(qsequence)
    # One (1) sequence corresponds to this query
    assert query_resp is not None and not query_resp.is_empty()
    assert len(query_resp) == len(expected_sequence_names)
    assert all([it.sequence.name in expected_sequence_names for it in query_resp])

    # The other criteria have been tested above...

    # free resources
    _client.close()


def test_query_topic_from_response(
    _client: MosaicoClient,
    _inject_sequences_mockup,  # Ensure the data are available on the data platform
):
    visibility_val = "private"
    query_resp = _client.query(
        QuerySequence().with_expression(
            Sequence.Q.user_metadata["visibility"].eq(visibility_val)
        )
    )
    # We do expect a successful query
    assert query_resp is not None and not query_resp.is_empty()
    # The other criteria have been tested above...
    expected_sequence_names = [
        key
        for key, val in QUERY_SEQUENCES_MOCKUP.items()
        if val.get("metadata", {}).get("visibility") == visibility_val
    ]
    assert len(query_resp) == len(expected_sequence_names)
    assert all([it.sequence.name in expected_sequence_names for it in query_resp])
    # This translates to:
    # 'query among the topics in the returned response'
    qtopic = query_resp.to_query_topic()
    # simply reprovide the same query to the client
    query_resp = _client.query(qtopic)
    # One (1) sequence corresponds to this query
    assert query_resp is not None and not query_resp.is_empty()
    assert len(query_resp) == len(expected_sequence_names)
    assert all([it.sequence.name in expected_sequence_names for it in query_resp])

    # The other criteria have been tested above...

    # Try restricting further the query...
    # get the first available ontology tag
    ontology_tag = "image"
    query_resp = _client.query(qtopic.with_ontology_tag(ontology_tag))
    # One (1) sequence corresponds to this query
    assert query_resp is not None and not query_resp.is_empty()

    expected_sequence_name = "test-query-sequence-1"
    expected_topic_name = "/topic11"
    assert len(query_resp) == 1
    assert query_resp[0].sequence.name == expected_sequence_name
    assert len(query_resp[0].topics) == 1
    assert query_resp[0].topics[0].name == expected_topic_name

    # free resources
    _client.close()
