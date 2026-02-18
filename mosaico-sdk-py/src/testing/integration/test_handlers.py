from mosaicolabs.comm import MosaicoClient
import pytest
from testing.integration.config import (
    UPLOADED_SEQUENCE_METADATA,
    UPLOADED_SEQUENCE_NAME,
    QUERY_SEQUENCES_MOCKUP,
)
from .helpers import (
    topic_to_metadata_dict,
    topic_list,
    _validate_returned_topic_name,
)


def test_sequence_metadata_recvd(
    _client: MosaicoClient,
    _inject_sequence_data_stream,  # Ensure the data are available on the data platform
):
    """Test that the sent and reconstructed sequence metadata are the same as original ones"""
    seqhandler = _client.sequence_handler(UPLOADED_SEQUENCE_NAME)
    # Sequence must exist
    assert seqhandler is not None
    # Deserialized metadata must be the same
    assert seqhandler.user_metadata == UPLOADED_SEQUENCE_METADATA
    # free resources
    _client.close()


@pytest.mark.parametrize("topic_name", topic_list)
def test_topic_metadata_recvd(
    _client: MosaicoClient,
    topic_name,
    _inject_sequence_data_stream,  # Ensure the data are available on the data platform
):
    """Test that the sent and reconstructed topic metadata are the same as original ones"""
    tophandler = _client.topic_handler(
        sequence_name=UPLOADED_SEQUENCE_NAME, topic_name=topic_name
    )
    # Topic must exist
    assert tophandler is not None
    _validate_returned_topic_name(tophandler.name)
    # Deserialized metadata must be the same
    assert tophandler.user_metadata == topic_to_metadata_dict[topic_name]
    # free resources
    _client.close()


@pytest.mark.parametrize("topic_name", topic_list)
def test_topic_handler_slash_in_name(
    _client: MosaicoClient,
    topic_name: str,
    _inject_sequence_data_stream,  # Ensure the data are available on the data platform
):
    """Test that the sent and reconstructed topic metadata are the same as original ones"""
    tophandler = _client.topic_handler(
        sequence_name=UPLOADED_SEQUENCE_NAME, topic_name=topic_name
    )
    # Topic must exist
    assert tophandler is not None
    _validate_returned_topic_name(tophandler.name)
    _client.clear_topic_handlers_cache()

    if topic_name.startswith("/"):
        # I have tested the retrieve with the slash: remove and retest
        topic_name = topic_name[1:]
    else:
        # I have tested the retrieve without the slash: add and retest
        topic_name = "/" + topic_name

    tophandler = _client.topic_handler(
        sequence_name=UPLOADED_SEQUENCE_NAME, topic_name=topic_name
    )
    # Topic must exist
    assert tophandler is not None
    _validate_returned_topic_name(tophandler.name)
    _client.clear_topic_handlers_cache()

    tophandler = _client.topic_handler(
        sequence_name=UPLOADED_SEQUENCE_NAME, topic_name=topic_name + "/"
    )
    # Topic must exist
    assert tophandler is not None
    _validate_returned_topic_name(tophandler.name)
    _client.clear_topic_handlers_cache()

    _client.close()


def test_sequence_handler_slash_in_name(
    _client: MosaicoClient,
    _inject_sequence_data_stream,  # Ensure the data are available on the data platform
):
    """Test that the sent and reconstructed topic metadata are the same as original ones"""
    seqhandler = _client.sequence_handler(sequence_name=UPLOADED_SEQUENCE_NAME)
    assert seqhandler is not None
    _client.clear_sequence_handlers_cache()

    seqhandler = _client.sequence_handler(sequence_name=("/" + UPLOADED_SEQUENCE_NAME))
    assert seqhandler is not None
    _client.clear_sequence_handlers_cache()

    seqhandler = _client.sequence_handler(sequence_name=(UPLOADED_SEQUENCE_NAME + "/"))
    assert seqhandler is not None
    _client.clear_sequence_handlers_cache()

    seqhandler = _client.sequence_handler(
        sequence_name=("/" + UPLOADED_SEQUENCE_NAME + "/")
    )
    assert seqhandler is not None
    _client.clear_sequence_handlers_cache()

    _client.close()


@pytest.mark.parametrize("topic_name", topic_list)
def test_topic_handlers(
    _client: MosaicoClient,
    topic_name,
    _inject_sequence_data_stream,  # Ensure the data are available on the data platform
):
    """Test if 'SequenceHandler.get_topic_handler' and 'MosaicoClient.topic_handler' return the very same entity"""
    seqhandler = _client.sequence_handler(UPLOADED_SEQUENCE_NAME)
    # Sequence must exist
    assert seqhandler is not None
    # This must raise if topic does not exist
    tophandler_from_seq = seqhandler.get_topic_handler(topic_name)
    _validate_returned_topic_name(tophandler_from_seq.name)

    # get the same handler from client
    tophandler = _client.topic_handler(
        sequence_name=UPLOADED_SEQUENCE_NAME, topic_name=topic_name
    )
    assert tophandler is not None
    _validate_returned_topic_name(tophandler.name)
    # TopicHandlers must be the same
    assert tophandler._topic == tophandler_from_seq._topic
    # free resources
    _client.close()


@pytest.mark.parametrize("sequence", QUERY_SEQUENCES_MOCKUP.keys())
def test_topic_handlers_in_dataless_sequence(
    _client: MosaicoClient,
    _inject_sequences_mockup,  # Ensure the data are available on the data platform
    sequence: str,
):
    """
    Test that the topic handler is correctly returned in a dataless sequence.
    """
    # All other tests are made somewhere else..
    for topic in QUERY_SEQUENCES_MOCKUP[sequence]["topics"]:
        tophandler = _client.topic_handler(
            sequence_name=sequence, topic_name=topic["name"]
        )
        # The topic handler is available anyway
        assert tophandler is not None
    # free resources
    _client.close()


@pytest.mark.parametrize("sequence", QUERY_SEQUENCES_MOCKUP.keys())
def test_sequence_reader_in_dataless_sequence(
    _client: MosaicoClient,
    _inject_sequences_mockup,  # Ensure the data are available on the data platform
    sequence: str,
):
    """
    Test that asking a data reader for a dataless sequence, raises an exception.
    """
    # All other tests are made somewhere else..
    seqhandler = _client.sequence_handler(sequence_name=sequence)
    assert seqhandler is not None
    with pytest.raises(
        ValueError,
        match="The sequence might contain no data",
    ):
        seqhandler.get_data_streamer()
    # free resources
    _client.close()


@pytest.mark.parametrize("sequence", QUERY_SEQUENCES_MOCKUP.keys())
def test_topic_readers_in_dataless_sequence(
    _client: MosaicoClient,
    _inject_sequences_mockup,  # Ensure the data are available on the data platform
    sequence: str,
):
    """
    Test that asking a data reader for a dataless topic, raises an exception.
    """
    # All other tests are made somewhere else..
    for topic in QUERY_SEQUENCES_MOCKUP[sequence]["topics"]:
        tophandler = _client.topic_handler(
            sequence_name=sequence, topic_name=topic["name"]
        )
        assert tophandler is not None
        with pytest.raises(
            ValueError,
            match="The topic might contain no data",
        ):
            tophandler.get_data_streamer()
    # free resources
    _client.close()
