import bisect

import pytest

from mosaicolabs.comm import MosaicoClient
from mosaicolabs.handlers import (
    SequenceHandler,
    TopicDataStreamer,
    TopicHandler,
)
from mosaicolabs.platform.app_metadata import TopicAppMetadata
from testing.integration.config import (
    QUERY_SEQUENCES_MOCKUP,
    UPLOADED_SEQUENCE_NAME,
)

from .helpers import (
    SequenceDataStream,
    topic_list,
)


def test_app_metadata_in_data_sequence(
    mosaico_client: MosaicoClient,
    inject_synthetic_sequence,  # Ensure the data are available on the data platform
    synthetic_sequence_data_stream: SequenceDataStream,
):
    """
    Test that the time-information are coherent for sequence with data stored.
    This is a low level test: private members and methods are called
    """
    # All other tests are made somewhere else..
    seqhandler = mosaico_client.sequence_handler(sequence_name=UPLOADED_SEQUENCE_NAME)
    assert seqhandler is not None
    assert seqhandler.timestamp_ns_min == synthetic_sequence_data_stream.tstamp_ns_start
    assert seqhandler.timestamp_ns_max == synthetic_sequence_data_stream.tstamp_ns_end
    # free resources
    mosaico_client.close()


@pytest.mark.parametrize("sequence", QUERY_SEQUENCES_MOCKUP.keys())
def test_app_metadata_in_dataless_sequence(
    mosaico_client: MosaicoClient,
    inject_mockup_sequences,  # Ensure the data are available on the data platform
    sequence: str,
):
    """
    Test that the time-information are coherent for dataless sequence.
    This is a low level test: private members and methods are called
    """
    # All other tests are made somewhere else..
    seqhandler = mosaico_client.sequence_handler(sequence_name=sequence)
    assert seqhandler is not None
    assert seqhandler.timestamp_ns_min is None
    assert seqhandler.timestamp_ns_max is None
    # free resources
    mosaico_client.close()


@pytest.mark.parametrize("topic", topic_list)
def test_topic_name_in_endpoint_from_topic_handler(
    mosaico_client: MosaicoClient,
    inject_synthetic_sequence,  # Ensure the data are available on the data platform
    topic: str,
):
    """
    Test that the topic endpoint is coherent wrt expected.
    This is a low level test: private members and methods are called
    """
    # All other tests are made somewhere else..
    flight_info, _, _ = TopicHandler._get_flight_info(
        sequence_name=UPLOADED_SEQUENCE_NAME,
        topic_name=topic,
        client=mosaico_client._control_client,
    )
    # Topic exists!
    assert len(flight_info.endpoints) == 1 and "Expected 1 endpoint"
    ep = flight_info.endpoints[0]
    topic_app_metadata = TopicAppMetadata._from_flight_endpoint(ep)
    # Topic exists!
    assert (
        topic_app_metadata.name == topic
        and f"Expected matching topic name {topic} != {topic_app_metadata.name}"
    )
    assert (
        topic_app_metadata.sequence_name == UPLOADED_SEQUENCE_NAME
        and f"Expected matching topic name {UPLOADED_SEQUENCE_NAME} != {topic_app_metadata.sequence_name}"
    )

    # free resources
    mosaico_client.close()


def test_topic_names_in_endpoints_from_sequence_handler(
    mosaico_client: MosaicoClient,
    inject_synthetic_sequence,  # Ensure the data are available on the data platform
):
    """
    Test that the topic endpoints are coherent wrt expected.
    This is a low level test: private members and methods are called
    """
    # All other tests are made somewhere else..
    flight_info, _ = SequenceHandler._get_flight_info(
        sequence_name=UPLOADED_SEQUENCE_NAME,
        client=mosaico_client._control_client,
    )
    # Topic exists!
    assert (
        len(flight_info.endpoints) == len(topic_list)
        and f"Expected {len(topic_list)} endpoints, got {len(flight_info.endpoints)}"
    )
    for ep in flight_info.endpoints:
        topic_app_metadata = TopicAppMetadata._from_flight_endpoint(ep)
        # Topic exists!
        assert (
            topic_app_metadata.name in topic_list
            and f"Expected matching topic name {topic_app_metadata.name} in topics list {topic_list}"
        )
        assert (
            topic_app_metadata.sequence_name == UPLOADED_SEQUENCE_NAME
            and f"Expected matching topic name {UPLOADED_SEQUENCE_NAME} != {topic_app_metadata.sequence_name}"
        )

    # free resources
    mosaico_client.close()


@pytest.mark.parametrize("topic", topic_list)
def test_topics_app_metadata_timestamps(
    mosaico_client: MosaicoClient,
    inject_synthetic_sequence,  # Ensure the data are available on the data platform
    synthetic_sequence_data_stream: SequenceDataStream,
    topic: str,
):
    """
    Test that the topic app_metadata is coherent wrt the timestamps received.
    This is a low level test: private members and methods are called
    """
    # generate for easier inspection and debug (than using next)
    _cached_topic_data_stream = [
        dstream
        for dstream in synthetic_sequence_data_stream.items
        if dstream.topic == topic
    ]

    thandler = mosaico_client.topic_handler(
        sequence_name=UPLOADED_SEQUENCE_NAME, topic_name=topic
    )
    # Sequence must exist
    assert thandler is not None
    # All other tests are made somewhere else..
    flight_info, _, _ = thandler._get_flight_info(
        sequence_name=UPLOADED_SEQUENCE_NAME,
        topic_name=topic,
        client=mosaico_client._control_client,
    )
    # The length of the 'endpoints' list is tested elsewhere
    ep = flight_info.endpoints[0]
    topic_app_metadata = TopicAppMetadata._from_flight_endpoint(ep)
    # Not asked for time-windowed stream: the min/max timestamps must be equal to start/end
    assert (
        _cached_topic_data_stream[0].msg.timestamp_ns
        == topic_app_metadata.timestamp_ns_min
    )
    assert (
        _cached_topic_data_stream[-1].msg.timestamp_ns
        == topic_app_metadata.timestamp_ns_max
    )

    # free resources
    mosaico_client.close()


@pytest.mark.parametrize("topic", topic_list)
def test_topic_streamer_app_metadata_timestamps(
    mosaico_client: MosaicoClient,
    inject_synthetic_sequence,  # Ensure the data are available on the data platform
    synthetic_sequence_data_stream: SequenceDataStream,
    topic: str,
):
    """
    Test that the topic app_metadata is coherent wrt the timestamps received, when in time-windowed stream.
    This is a low level test: private members and methods are called
    """
    # generate for easier inspection and debug (than using next)
    _cached_topic_data_stream = [
        dstream
        for dstream in synthetic_sequence_data_stream.items
        if dstream.topic == topic
    ]
    # start from the half of the sequence
    timestamp_ns_start = synthetic_sequence_data_stream.tstamp_ns_start + int(
        (
            synthetic_sequence_data_stream.tstamp_ns_end
            - synthetic_sequence_data_stream.tstamp_ns_start
        )
        / 2
    )
    timestamp_ns_end = synthetic_sequence_data_stream.tstamp_ns_end

    # find the index to start from (which corresponds to timestamp_ns_start)
    msg_idx_start = bisect.bisect_left(
        [it.msg.timestamp_ns for it in _cached_topic_data_stream],
        timestamp_ns_start,
    )

    # find the index to which end (which corresponds to the one right before timestamp_ns_end)
    msg_idx_stop = (
        bisect.bisect_left(
            [it.msg.timestamp_ns for it in _cached_topic_data_stream],
            timestamp_ns_end,
        )
        - 1
    )
    flight_info = TopicDataStreamer._get_flight_info(
        sequence_name=UPLOADED_SEQUENCE_NAME,
        topic_name=topic,
        client=mosaico_client._control_client,
        start_timestamp_ns=timestamp_ns_start,
        end_timestamp_ns=timestamp_ns_end,
    )
    # The length of the 'endpoints' list is tested elsewhere
    ep = flight_info.endpoints[0]
    topic_app_metadata = TopicAppMetadata._from_flight_endpoint(ep)
    # The start/end timestamp must be equal to the first/last message timestamps WITHIN THE TIME WINDOW of the topic data stream
    assert (
        topic_app_metadata.timestamp_ns_min
        == _cached_topic_data_stream[msg_idx_start].msg.timestamp_ns
    )
    assert (
        topic_app_metadata.timestamp_ns_max
        == _cached_topic_data_stream[msg_idx_stop].msg.timestamp_ns
    )

    # free resources
    mosaico_client.close()


def test_msg_count(
    mosaico_client: MosaicoClient,
    inject_synthetic_sequence,  # Ensure the data are available on the data platform
):
    """
    Test that the sequence and topic streamer message count is coherent wrt the loaded data for each topic.
    """

    total_topics_msg_count = (
        0  # usefull to compare later with SequenceDataStreamer.msg_count
    )

    for topic in topic_list:
        t_handler = mosaico_client.topic_handler(UPLOADED_SEQUENCE_NAME, topic)

        assert t_handler is not None

        current_topic_msg_count = t_handler.get_data_streamer().msg_count
        assert current_topic_msg_count is not None
        assert current_topic_msg_count == 25

        total_topics_msg_count += current_topic_msg_count

    s_handler = mosaico_client.sequence_handler(UPLOADED_SEQUENCE_NAME)
    assert s_handler is not None
    assert s_handler.get_data_streamer().msg_count == total_topics_msg_count

    mosaico_client.close()
