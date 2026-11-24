import pytest

from mosaicolabs.comm import MosaicoClient
from mosaicolabs.models.core.serializable import _compute_schema_fingerprint
from testing.integration.config import (
    QUERY_SEQUENCES_MOCKUP,
    UPLOADED_SEQUENCE_METADATA,
    UPLOADED_SEQUENCE_NAME,
)

from .helpers import (
    _validate_returned_topic_name,
    topic_list,
    topic_to_metadata_dict,
    topic_to_ontology_class_dict,
)


def test_sequence_metadata_recvd(
    mosaico_client: MosaicoClient,
    inject_synthetic_sequence,  # Ensure the data are available on the data platform
):
    """Test that the sent and reconstructed sequence metadata are the same as original ones"""
    seqhandler = mosaico_client.sequence_handler(UPLOADED_SEQUENCE_NAME)
    # Sequence must exist
    assert seqhandler is not None
    # Deserialized metadata must be the same
    assert seqhandler.user_metadata == UPLOADED_SEQUENCE_METADATA


def test_sequence_reload(
    mosaico_client: MosaicoClient,
    inject_synthetic_sequence,  # Ensure the data are available on the data platform
):
    """Test that the sent and reconstructed sequence metadata are the same as original ones"""
    seqhandler = mosaico_client.sequence_handler(UPLOADED_SEQUENCE_NAME)
    # Sequence must exist
    assert seqhandler is not None
    original_topics = seqhandler.topics
    original_size_bytes = seqhandler.total_size_bytes
    original_created_timestamp = seqhandler.created_timestamp
    original_timestamp_ns_min = seqhandler.timestamp_ns_min
    original_timestamp_ns_max = seqhandler.timestamp_ns_max

    seqhandler.reload()
    assert seqhandler is not None
    assert len(seqhandler.topics) == len(original_topics)
    assert all(topic in original_topics for topic in seqhandler.topics)
    assert seqhandler.total_size_bytes == original_size_bytes
    assert seqhandler.created_timestamp == original_created_timestamp
    assert seqhandler.timestamp_ns_min == original_timestamp_ns_min
    assert seqhandler.timestamp_ns_max == original_timestamp_ns_max


def test_sequence_exists(
    mosaico_client: MosaicoClient,
    inject_synthetic_sequence,  # Ensure the data are available on the data platform
):
    """Test that the sent and reconstructed sequence metadata are the same as original ones"""
    # Sequence must exist
    assert mosaico_client.sequence_exists(UPLOADED_SEQUENCE_NAME)


def test_sequence_does_not_exist(
    mosaico_client: MosaicoClient,
):
    """Test that the sent and reconstructed sequence metadata are the same as original ones"""
    # Sequence must not exist
    assert not mosaico_client.sequence_exists("non_existent_sequence")


@pytest.mark.parametrize("topic_name", topic_list)
def test_topic_exists(
    mosaico_client: MosaicoClient,
    topic_name,
    inject_synthetic_sequence,  # Ensure the data are available on the data platform
):
    """Test that the sent and reconstructed topic metadata are the same as original ones"""
    # Topic must exist
    assert mosaico_client.topic_exists(
        sequence_name=UPLOADED_SEQUENCE_NAME, topic_name=topic_name
    )


def test_topic_does_not_exist(
    mosaico_client: MosaicoClient,
    inject_synthetic_sequence,  # Ensure the data are available on the data platform
):
    """Test that the sent and reconstructed topic metadata are the same as original ones"""
    # Topic must not exist
    assert not mosaico_client.topic_exists(
        sequence_name=UPLOADED_SEQUENCE_NAME, topic_name="non_existent_topic"
    )


@pytest.mark.parametrize("topic_name", topic_list)
def test_topic_metadata_recvd(
    mosaico_client: MosaicoClient,
    topic_name,
    inject_synthetic_sequence,  # Ensure the data are available on the data platform
):
    """Test that the sent and reconstructed topic metadata are the same as original ones"""
    tophandler = mosaico_client.topic_handler(
        sequence_name=UPLOADED_SEQUENCE_NAME, topic_name=topic_name
    )
    # Topic must exist
    assert tophandler is not None
    _validate_returned_topic_name(tophandler.name)
    # Deserialized metadata must be the same
    assert tophandler.user_metadata == topic_to_metadata_dict[topic_name]


@pytest.mark.parametrize("topic_name", topic_list)
def test_topic_handler_slash_in_name(
    mosaico_client: MosaicoClient,
    topic_name: str,
    inject_synthetic_sequence,  # Ensure the data are available on the data platform
):
    """Test that the sent and reconstructed topic metadata are the same as original ones"""
    tophandler = mosaico_client.topic_handler(
        sequence_name=UPLOADED_SEQUENCE_NAME, topic_name=topic_name
    )
    # Topic must exist
    assert tophandler is not None
    _validate_returned_topic_name(tophandler.name)
    mosaico_client.clear_topic_handlers_cache()

    if topic_name.startswith("/"):
        # I have tested the retrieve with the slash: remove and retest
        topic_name = topic_name[1:]
    else:
        # I have tested the retrieve without the slash: add and retest
        topic_name = "/" + topic_name

    tophandler = mosaico_client.topic_handler(
        sequence_name=UPLOADED_SEQUENCE_NAME, topic_name=topic_name
    )
    # Topic must exist
    assert tophandler is not None
    _validate_returned_topic_name(tophandler.name)
    mosaico_client.clear_topic_handlers_cache()

    tophandler = mosaico_client.topic_handler(
        sequence_name=UPLOADED_SEQUENCE_NAME, topic_name=topic_name + "/"
    )
    # Topic must exist
    assert tophandler is not None
    _validate_returned_topic_name(tophandler.name)
    mosaico_client.clear_topic_handlers_cache()


def test_sequence_handler_slash_in_name(
    mosaico_client: MosaicoClient,
    inject_synthetic_sequence,  # Ensure the data are available on the data platform
):
    """Test that the sent and reconstructed topic metadata are the same as original ones"""
    seqhandler = mosaico_client.sequence_handler(sequence_name=UPLOADED_SEQUENCE_NAME)
    assert seqhandler is not None
    mosaico_client.clear_sequence_handlers_cache()

    seqhandler = mosaico_client.sequence_handler(
        sequence_name=("/" + UPLOADED_SEQUENCE_NAME)
    )
    assert seqhandler is not None
    mosaico_client.clear_sequence_handlers_cache()

    seqhandler = mosaico_client.sequence_handler(
        sequence_name=(UPLOADED_SEQUENCE_NAME + "/")
    )
    assert seqhandler is not None
    mosaico_client.clear_sequence_handlers_cache()

    seqhandler = mosaico_client.sequence_handler(
        sequence_name=("/" + UPLOADED_SEQUENCE_NAME + "/")
    )
    assert seqhandler is not None
    mosaico_client.clear_sequence_handlers_cache()


@pytest.mark.parametrize("topic_name", topic_list)
def test_topic_handlers(
    mosaico_client: MosaicoClient,
    topic_name,
    inject_synthetic_sequence,  # Ensure the data are available on the data platform
):
    """Test if 'SequenceHandler.get_topic_handler' and 'MosaicoClient.topic_handler' return the very same entity"""
    seqhandler = mosaico_client.sequence_handler(UPLOADED_SEQUENCE_NAME)
    # Sequence must exist
    assert seqhandler is not None
    # This must raise if topic does not exist
    tophandler_from_seq = seqhandler.get_topic_handler(topic_name)
    _validate_returned_topic_name(tophandler_from_seq.name)

    # get the same handler from client
    tophandler = mosaico_client.topic_handler(
        sequence_name=UPLOADED_SEQUENCE_NAME, topic_name=topic_name
    )
    assert tophandler is not None
    _validate_returned_topic_name(tophandler.name)
    # TopicHandlers must be the same
    assert tophandler._topic == tophandler_from_seq._topic


@pytest.mark.parametrize("topic_name", topic_list)
def test_topic_handler_connect_does_not_fetch_schema(
    mosaico_client: MosaicoClient,
    topic_name,
    inject_synthetic_sequence,  # Ensure the data are available on the data platform
):
    """Test that connecting a 'TopicHandler' does not fetch the ontology schema"""
    tophandler = mosaico_client.topic_handler(
        sequence_name=UPLOADED_SEQUENCE_NAME, topic_name=topic_name
    )
    # Topic must exist
    assert tophandler is not None
    # The schema is fetched on the first access to 'ontology_schema' only
    assert tophandler._arrow_schema is None


@pytest.mark.parametrize("topic_name", topic_list)
def test_topic_handler_metadata_access_does_not_fetch_schema(
    mosaico_client: MosaicoClient,
    topic_name,
    inject_synthetic_sequence,  # Ensure the data are available on the data platform
):
    """Test that the metadata of a 'TopicHandler' are available without fetching the ontology schema"""
    tophandler = mosaico_client.topic_handler(
        sequence_name=UPLOADED_SEQUENCE_NAME, topic_name=topic_name
    )
    # Topic must exist
    assert tophandler is not None

    _validate_returned_topic_name(tophandler.name)
    assert tophandler.sequence_name == UPLOADED_SEQUENCE_NAME
    assert tophandler.user_metadata == topic_to_metadata_dict[topic_name]
    assert tophandler.timestamp_ns_min is not None
    assert tophandler.timestamp_ns_max is not None
    assert tophandler.timestamp_ns_min <= tophandler.timestamp_ns_max

    # None of the above requires the schema
    assert tophandler._arrow_schema is None


@pytest.mark.parametrize("topic_name", topic_list)
def test_topic_handler_ontology_schema_is_fetched_lazily_and_cached(
    mosaico_client: MosaicoClient,
    topic_name,
    inject_synthetic_sequence,  # Ensure the data are available on the data platform
):
    """Test that 'TopicHandler.ontology_schema' is fetched on first access and then cached"""
    tophandler = mosaico_client.topic_handler(
        sequence_name=UPLOADED_SEQUENCE_NAME, topic_name=topic_name
    )
    # Topic must exist
    assert tophandler is not None
    assert tophandler._arrow_schema is None

    # The first access fetches the schema of the requested topic from the server
    schema = tophandler.ontology_schema
    ontology_class = topic_to_ontology_class_dict[topic_name]
    assert _compute_schema_fingerprint(schema) == _compute_schema_fingerprint(
        ontology_class.__msco_pyarrow_struct__
    )

    # Further accesses return the cached schema, without asking the server again
    assert tophandler._arrow_schema is schema
    assert tophandler.ontology_schema is schema


@pytest.mark.parametrize("sequence", QUERY_SEQUENCES_MOCKUP.keys())
def test_topic_handlers_in_dataless_sequence(
    mosaico_client: MosaicoClient,
    inject_mockup_sequences,  # Ensure the data are available on the data platform
    sequence: str,
):
    """
    Test that the topic handler is correctly returned in a dataless sequence.
    """
    # All other tests are made somewhere else..
    for topic in QUERY_SEQUENCES_MOCKUP[sequence]["topics"]:
        tophandler = mosaico_client.topic_handler(
            sequence_name=sequence, topic_name=topic["name"]
        )
        # The topic handler is available anyway
        assert tophandler is not None


@pytest.mark.parametrize("sequence", QUERY_SEQUENCES_MOCKUP.keys())
def test_sequence_reader_in_dataless_sequence(
    mosaico_client: MosaicoClient,
    inject_mockup_sequences,  # Ensure the data are available on the data platform
    sequence: str,
):
    """
    Test that asking a data reader for a dataless sequence, raises an exception.
    """
    # All other tests are made somewhere else..
    seqhandler = mosaico_client.sequence_handler(sequence_name=sequence)
    assert seqhandler is not None
    with pytest.raises(
        ValueError,
        match="The sequence might contain no data",
    ):
        seqhandler.get_data_streamer()


@pytest.mark.parametrize("sequence", QUERY_SEQUENCES_MOCKUP.keys())
def test_topic_readers_in_dataless_sequence(
    mosaico_client: MosaicoClient,
    inject_mockup_sequences,  # Ensure the data are available on the data platform
    sequence: str,
):
    """
    Test that asking a data reader for a dataless topic, raises an exception.
    """
    # All other tests are made somewhere else..
    for topic in QUERY_SEQUENCES_MOCKUP[sequence]["topics"]:
        tophandler = mosaico_client.topic_handler(
            sequence_name=sequence, topic_name=topic["name"]
        )
        assert tophandler is not None
        with pytest.raises(
            ValueError,
            match="The topic might contain no data",
        ):
            tophandler.get_data_streamer()
