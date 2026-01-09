from mosaicolabs.comm import MosaicoClient
import pytest
from testing.integration.config import (
    UPLOADED_SEQUENCE_METADATA,
    UPLOADED_SEQUENCE_NAME,
)
from .helpers import (
    SequenceDataStream,
    topic_to_metadata_dict,
    topic_list,
    _validate_returned_topic_name,
)


def test_sequence_data_stream(
    _client: MosaicoClient,
    _make_sequence_data_stream: SequenceDataStream,  # Get the data stream for comparisons
    _inject_sequence_data_stream,  # Make sure data are available on the server
):
    """Test that the sequence data stream is correctly unpacked and provided"""
    msg_count = 0
    seqhandler = _client.sequence_handler(UPLOADED_SEQUENCE_NAME)
    # Sequence must exist
    assert seqhandler is not None
    # all the original topics are received
    [_validate_returned_topic_name(topic) for topic in seqhandler.topics]
    # ALL AND ONLY the original topics are received
    assert all([topic in seqhandler.topics for topic in topic_list])
    assert len(seqhandler.topics) == len(topic_list)

    # The metadata are coherent
    assert seqhandler.user_metadata == UPLOADED_SEQUENCE_METADATA
    sstream_handl = seqhandler.get_data_streamer()
    # Get the next timestamp, without consuming the related sample
    next_tstamp = sstream_handl.next_timestamp()
    assert next_tstamp is not None
    # assert the valid behavior of next_timestamp(): does not consume anything
    assert next_tstamp == sstream_handl.next_timestamp()
    assert sstream_handl.next_timestamp() == sstream_handl.next_timestamp()

    # Start consuming data stream
    for topic, message in sstream_handl:
        _validate_returned_topic_name(topic)
        # assert the valid behavior of next_timestamp()
        assert next_tstamp == message.timestamp_ns
        cached_item = _make_sequence_data_stream.items[msg_count]
        # all the received data are consistent with the timing of the native sequence
        # note: the important thing is the timing: when two measurements have the same timestamp
        # cannot ensure order
        if cached_item.topic != topic:
            assert message.timestamp_ns == cached_item.msg.timestamp_ns
        else:
            assert message == cached_item.msg
        msg_count += 1
        # Get the next timestamp for the next iteration, without consuming the related sample
        next_tstamp = sstream_handl.next_timestamp()

        # Test the correct return of the Message methods
        assert message.ontology_type() == cached_item.ontology_class
        assert message.ontology_tag() == cached_item.ontology_class.__ontology_tag__

    # check the total number of received sensors is the same of the original sequence
    assert msg_count == len(_make_sequence_data_stream.items)

    # free resources
    _client.close()


# Repeat for each topic
@pytest.mark.parametrize("topic", topic_list)
def test_topic_data_stream(
    _client: MosaicoClient,
    _make_sequence_data_stream: SequenceDataStream,  # Get the data stream for comparisons
    _inject_sequence_data_stream,  # Make sure data are available on the server
    topic: str,
):
    """Test that the topic data stream is correctly unpacked and provided"""
    # generate for easier inspection and debug (than using next)
    _cached_topic_data_stream = [
        dstream
        for dstream in _make_sequence_data_stream.items
        if dstream.topic == topic
    ]
    msg_count = 0
    seqhandler = _client.sequence_handler(UPLOADED_SEQUENCE_NAME)
    # just prevent IDE to complain about None
    assert seqhandler is not None
    # All other tests for this sequence have been done or will be done... skip.

    # This must raise if topic does not exist
    tophandler = seqhandler.get_topic_handler(topic)
    _validate_returned_topic_name(tophandler.name)
    # Trivial: Handler is consistent
    assert tophandler.name == topic

    # The metadata are coherent
    assert tophandler.user_metadata == topic_to_metadata_dict[topic]
    # This must raise if topic handler is malformed
    tstream_handl = tophandler.get_data_streamer()

    _validate_returned_topic_name(tstream_handl.name())
    assert tstream_handl.name() == topic

    # Topic reader must be valid
    assert tstream_handl is not None
    # Get the next timestamp, without consuming the related sample
    next_tstamp = tstream_handl.next_timestamp()
    assert next_tstamp is not None
    # assert the valid behavior of next_timestamp(): does not consume anything
    assert next_tstamp == tstream_handl.next_timestamp()
    assert tstream_handl.next_timestamp() == tstream_handl.next_timestamp()

    # Start consuming data stream
    for message in tstream_handl:
        # assert the valid behavior of next_timestamp()
        assert next_tstamp == message.timestamp_ns
        # get next cached message for the current topic from stream
        cached_item = _cached_topic_data_stream[msg_count]
        # all the received data are consistent with the timing of the native sequence
        # note: the important thing is the timing: when two measurements have the same timestamp
        # cannot ensure order
        assert message == cached_item.msg
        msg_count += 1
        # Get the next timestamp for the next iteration, without consuming the related sample
        next_tstamp = tstream_handl.next_timestamp()

        # Test the correct return of the Message methods
        assert message.ontology_type() == cached_item.ontology_class
        assert message.ontology_tag() == cached_item.ontology_class.__ontology_tag__

    # check the total number of received sensors is the same of the original sequence
    assert msg_count == len(_cached_topic_data_stream)

    # free resources
    _client.close()


def test_sequence_data_stream_filter_topics(
    _client: MosaicoClient,
    _inject_sequence_data_stream,  # necessary to make sure data are available on server
):
    """Test that the sequence data stream is correctly unpacked and provided"""
    seqhandler = _client.sequence_handler(UPLOADED_SEQUENCE_NAME)
    # Sequence must exist
    assert seqhandler is not None

    # get a subset of topics
    filtered_topics = topic_list[: round(len(topic_list) / 2)]
    assert len(filtered_topics) > 0
    ret_topics = set()
    for topic, _ in seqhandler.get_data_streamer(topics=filtered_topics):
        # only desired topics must be returned
        assert topic in filtered_topics
        ret_topics.add(topic)

    # ALL the desired topics are returned
    assert len(filtered_topics) == len(ret_topics)

    # free resources
    _client.close()
