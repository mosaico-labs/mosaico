import bisect

from mosaicolabs.comm import MosaicoClient
import pytest
from testing.integration.config import (
    UPLOADED_SEQUENCE_NAME,
)
from .helpers import (
    SequenceDataStream,
    topic_list,
)


def _exec_test_sequence_data_stream_timerange(
    _client: MosaicoClient,
    _make_sequence_data_stream: SequenceDataStream,  # Get the data stream for comparisons
    time_start: int,
    time_end: int
):
    """Test that the sequence time-windowed data stream from start to end is correctly unpacked and provided"""
    seqhandler = _client.sequence_handler(UPLOADED_SEQUENCE_NAME)
    # Sequence must exist
    assert seqhandler is not None
    # other tests are done elsewhere...

    sstream_handl = seqhandler.get_data_streamer(
        start_timestamp_ns=time_start,
        end_timestamp_ns=time_end,
    )
    # Get the next timestamp, without consuming the related sample
    next_tstamp = sstream_handl.next_timestamp()
    assert next_tstamp is not None
    # assert the valid behavior of next_timestamp(): does not consume anything
    assert next_tstamp == sstream_handl.next_timestamp()
    assert sstream_handl.next_timestamp() == sstream_handl.next_timestamp()

    # find the index to start from (which corresponds to timestamp_ns_start)
    msg_idx_start = bisect.bisect_left(
        [it.msg.timestamp_ns for it in _make_sequence_data_stream.items],
        time_start,
    )
    msg_count = msg_idx_start

    # find the index to which end (which corresponds to timestamp_ns_end)
    msg_idx_stop = (
        bisect.bisect_left(
            [it.msg.timestamp_ns for it in _make_sequence_data_stream.items],
            time_end,
        )
        - 1
    )

    # Start consuming data stream
    for topic, message in sstream_handl:
        assert message.timestamp_ns >= time_start and message.timestamp_ns < time_end
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
    assert msg_count == msg_idx_stop + 1

    # free resources
    _client.close()


def _exec_test_topic_data_stream_timerange(
    _client: MosaicoClient,
    _make_sequence_data_stream: SequenceDataStream,  # Get the data stream for comparisons
    topic: str,
    time_start: int,
    time_end: int,
):
    """Test that the topic data stream is correctly unpacked and provided"""
    # generate for easier inspection and debug (than using next)
    _cached_topic_data_stream = [
        dstream
        for dstream in _make_sequence_data_stream.items
        if dstream.topic == topic
    ]
    seqhandler = _client.sequence_handler(UPLOADED_SEQUENCE_NAME)
    # just prevent IDE to complain about None
    assert seqhandler is not None
    # All other tests for this sequence have been done or will be done... skip.

    # This must raise if topic does not exist
    tophandler = seqhandler.get_topic_handler(topic)

    # find the index to start from (which corresponds to timestamp_ns_start)
    msg_idx_start = bisect.bisect_left(
        [it.msg.timestamp_ns for it in _cached_topic_data_stream],
        time_start,
    )
    msg_count = msg_idx_start

    # find the index to which end (which corresponds to timestamp_ns_end)
    msg_idx_stop = (
        bisect.bisect_left(
            [it.msg.timestamp_ns for it in _cached_topic_data_stream],
            time_end,
        )
        - 1
    )

    # This must raise if topic handler is malformed
    tstream_handl = tophandler.get_data_streamer(
        start_timestamp_ns=time_start, 
        end_timestamp_ns=time_end,
    )

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
        assert message.timestamp_ns >= time_start and message.timestamp_ns < time_end
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
    assert msg_count == msg_idx_stop + 1

    # free resources
    _client.close()

def test_sequence_data_stream_timerange_start_to_end(
    _client: MosaicoClient,
    _make_sequence_data_stream: SequenceDataStream,  # Get the data stream for comparisons
    _inject_sequence_data_stream,  # Make sure data are available on the server
):
    """Test that the sequence time-windowed data stream from start to end is correctly unpacked and provided"""
    _exec_test_sequence_data_stream_timerange(
        _client, 
        _make_sequence_data_stream, 
        _make_sequence_data_stream.tstamp_ns_start, 
        _make_sequence_data_stream.tstamp_ns_end,
    )



def test_sequence_data_stream_timerange_from_half(
    _client: MosaicoClient,
    _make_sequence_data_stream: SequenceDataStream,  # Get the data stream for comparisons
    _inject_sequence_data_stream,  # Make sure data are available on the server
):
    """Test that the sequence time-windowed data stream from start to end is correctly unpacked and provided"""
    # start from the half of the sequence
    timestamp_ns_start = _make_sequence_data_stream.tstamp_ns_start + int(
        (
            _make_sequence_data_stream.tstamp_ns_start
            + _make_sequence_data_stream.tstamp_ns_end
        )
        / 2
    )
    timestamp_ns_end = _make_sequence_data_stream.tstamp_ns_end

    _exec_test_sequence_data_stream_timerange(
        _client, 
        _make_sequence_data_stream, 
        timestamp_ns_start, 
        timestamp_ns_end,
    )




def test_sequence_data_stream_timerange_to_half(
    _client: MosaicoClient,
    _make_sequence_data_stream: SequenceDataStream,  # Get the data stream for comparisons
    _inject_sequence_data_stream,  # Make sure data are available on the server
):
    """Test that the sequence time-windowed data stream from start to end is correctly unpacked and provided"""
    # start from the half of the sequence
    timestamp_ns_start = _make_sequence_data_stream.tstamp_ns_start
    timestamp_ns_end = _make_sequence_data_stream.tstamp_ns_start + int(
        (
            _make_sequence_data_stream.tstamp_ns_start
            + _make_sequence_data_stream.tstamp_ns_end
        )
        / 2
    )

    _exec_test_sequence_data_stream_timerange(
        _client, 
        _make_sequence_data_stream, 
        timestamp_ns_start, 
        timestamp_ns_end,
    )


# Repeat for each topic
@pytest.mark.parametrize("topic", topic_list)
def test_topic_data_stream_to_half(
    _client: MosaicoClient,
    _make_sequence_data_stream: SequenceDataStream,  # Get the data stream for comparisons
    _inject_sequence_data_stream,  # Make sure data are available on the server
    topic: str,
):
    # start from the half of the sequence
    timestamp_ns_start = _make_sequence_data_stream.tstamp_ns_start
    timestamp_ns_end = _make_sequence_data_stream.tstamp_ns_start + int(
        (
            _make_sequence_data_stream.tstamp_ns_start
            + _make_sequence_data_stream.tstamp_ns_end
        )
        / 2
    )

    _exec_test_topic_data_stream_timerange(
        _client,
        _make_sequence_data_stream,
        topic,
        timestamp_ns_start,
        timestamp_ns_end,
    )




    # Repeat for each topic
@pytest.mark.parametrize("topic", topic_list)
def test_topic_data_stream_from_half(
    _client: MosaicoClient,
    _make_sequence_data_stream: SequenceDataStream,  # Get the data stream for comparisons
    _inject_sequence_data_stream,  # Make sure data are available on the server
    topic: str,
):
     # start from the half of the sequence
    timestamp_ns_start = _make_sequence_data_stream.tstamp_ns_start + int(
        (
            _make_sequence_data_stream.tstamp_ns_start
            + _make_sequence_data_stream.tstamp_ns_end
        )
        / 2
    )
    timestamp_ns_end = _make_sequence_data_stream.tstamp_ns_end

    _exec_test_topic_data_stream_timerange(
        _client,
        _make_sequence_data_stream,
        topic,
        timestamp_ns_start,
        timestamp_ns_end,
    )
