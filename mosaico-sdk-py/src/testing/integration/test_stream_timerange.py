import bisect

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


def test_sequence_data_stream_timerange_trivial(
    _client: MosaicoClient,
    _make_sequence_data_stream: SequenceDataStream,  # Get the data stream for comparisons
    _inject_sequence_data_stream,  # Make sure data are available on the server
):
    """Test that the sequence time-windowed data stream from start to end is correctly unpacked and provided"""
    msg_count = 0
    seqhandler = _client.sequence_handler(UPLOADED_SEQUENCE_NAME)
    # Sequence must exist
    assert seqhandler is not None
    # all the original topics are received
    [_validate_returned_topic_name(topic) for topic in seqhandler.topics]
    # ALL AND ONLY the original topics are received
    assert all([topic in seqhandler.topics for topic in topic_list])
    assert len(seqhandler.topics) == len(topic_list)

    timestamp_ns_start = _make_sequence_data_stream.tstamp_ns_start
    timestamp_ns_end = _make_sequence_data_stream.tstamp_ns_end

    # The metadata are coherent
    assert seqhandler.user_metadata == UPLOADED_SEQUENCE_METADATA
    sstream_handl = seqhandler.get_data_streamer(
        start_timestamp_ns=timestamp_ns_start,
        end_timestamp_ns=timestamp_ns_end,
    )
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


def _test_sequence_data_stream_timerange_from_half(
    _client: MosaicoClient,
    _make_sequence_data_stream: SequenceDataStream,  # Get the data stream for comparisons
    _inject_sequence_data_stream,  # Make sure data are available on the server
):
    """Test that the sequence time-windowed data stream from start to end is correctly unpacked and provided"""
    seqhandler = _client.sequence_handler(UPLOADED_SEQUENCE_NAME)
    # Sequence must exist
    assert seqhandler is not None
    # other tests are done elsewhere...

    # start from the half of the sequence
    timestamp_ns_start = _make_sequence_data_stream.tstamp_ns_start + int(
        (
            _make_sequence_data_stream.tstamp_ns_start
            + _make_sequence_data_stream.tstamp_ns_end
        )
        / 2
    )
    timestamp_ns_end = _make_sequence_data_stream.tstamp_ns_end

    sstream_handl = seqhandler.get_data_streamer(
        start_timestamp_ns=timestamp_ns_start,
        end_timestamp_ns=timestamp_ns_end,
    )
    # Get the next timestamp, without consuming the related sample
    next_tstamp = sstream_handl.next_timestamp()
    assert next_tstamp is not None
    # assert the valid behavior of next_timestamp(): does not consume anything
    assert next_tstamp == sstream_handl.next_timestamp()
    assert sstream_handl.next_timestamp() == sstream_handl.next_timestamp()

    # find the index to start from (which corresponds to timestamp_ns_start)
    msg_count_start = bisect.bisect_left(
        [it.msg.timestamp_ns for it in _make_sequence_data_stream.items],
        timestamp_ns_start,
    )
    msg_count = msg_count_start

    # Start consuming data stream
    for topic, message in sstream_handl:
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
    assert msg_count == len(_make_sequence_data_stream.items) - msg_count_start - 1

    # free resources
    _client.close()


def _test_sequence_data_stream_timerange_to_half(
    _client: MosaicoClient,
    _make_sequence_data_stream: SequenceDataStream,  # Get the data stream for comparisons
    _inject_sequence_data_stream,  # Make sure data are available on the server
):
    """Test that the sequence time-windowed data stream from start to end is correctly unpacked and provided"""
    seqhandler = _client.sequence_handler(UPLOADED_SEQUENCE_NAME)
    # Sequence must exist
    assert seqhandler is not None
    # other tests are done elsewhere...

    # start from the half of the sequence
    timestamp_ns_start = _make_sequence_data_stream.tstamp_ns_start
    timestamp_ns_end = _make_sequence_data_stream.tstamp_ns_start + int(
        (
            _make_sequence_data_stream.tstamp_ns_start
            + _make_sequence_data_stream.tstamp_ns_end
        )
        / 2
    )

    sstream_handl = seqhandler.get_data_streamer(
        start_timestamp_ns=timestamp_ns_start,
        end_timestamp_ns=timestamp_ns_end,
    )
    # Get the next timestamp, without consuming the related sample
    next_tstamp = sstream_handl.next_timestamp()
    assert next_tstamp is not None
    # assert the valid behavior of next_timestamp(): does not consume anything
    assert next_tstamp == sstream_handl.next_timestamp()
    assert sstream_handl.next_timestamp() == sstream_handl.next_timestamp()

    msg_count = 0
    # find the index to start from (which corresponds to timestamp_ns_start)
    msg_count_stop = (
        bisect.bisect_left(
            [it.msg.timestamp_ns for it in _make_sequence_data_stream.items],
            timestamp_ns_end,
        )
        - 1
    )
    # Start consuming data stream
    for topic, message in sstream_handl:
        if msg_count > msg_count_stop:
            assert False and "Stream is expected toend before this point!"
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
    assert msg_count == msg_count_stop + 1

    # free resources
    _client.close()
