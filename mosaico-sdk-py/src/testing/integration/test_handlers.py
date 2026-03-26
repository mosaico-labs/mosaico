import pytest

from mosaicolabs.comm import MosaicoClient
from testing.integration.config import (
    QUERY_SEQUENCES_MOCKUP,
    UPLOADED_SEQUENCE_METADATA,
    UPLOADED_SEQUENCE_NAME,
)

from .helpers import (
    _validate_returned_topic_name,
    topic_list,
    topic_to_metadata_dict,
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
    # free resources
    mosaico_client.close()


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

    # free resources
    mosaico_client.close()


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
    # free resources
    mosaico_client.close()


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

    mosaico_client.close()


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

    mosaico_client.close()


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
    # free resources
    mosaico_client.close()


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
    # free resources
    mosaico_client.close()


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
    # free resources
    mosaico_client.close()


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
    # free resources
    mosaico_client.close()
