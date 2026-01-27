import bisect
from math import ceil
from mosaicolabs.handlers.sequence_handler import SequenceHandler
from typing import List, Iterable, Optional

from mosaicolabs.comm import MosaicoClient
from mosaicolabs.ml import DataFrameExtractor, SyncTransformer
import numpy as np
import pytest
from testing.integration.config import (
    UPLOADED_SEQUENCE_NAME,
    UPLOADED_IMU_FRONT_TOPIC,
    UPLOADED_IMU_CAMERA_TOPIC,
    UPLOADED_GPS_TOPIC,
)
from .helpers import SequenceDataStream, topic_list


def _get_topic_timestamps(
    data_stream: SequenceDataStream,  # Get the data stream for comparisons
    topic: str,
    time_start: int,
    time_end: int,
):
    """Retrieve the data stream timestamps from a topic inside the time window"""
    _cached_topic_data_stream = [
        dstream for dstream in data_stream.items if dstream.topic == topic
    ]
    # find the index to start from (which corresponds to timestamp_ns_start)
    msg_idx_start = (
        0
        if time_start is None
        else bisect.bisect_left(
            [it.msg.timestamp_ns for it in _cached_topic_data_stream],
            time_start,
        )
    )

    # find the index to which end (which corresponds to timestamp_ns_end)
    msg_idx_end = (
        len(_cached_topic_data_stream) - 1
        if time_end is None
        else (
            bisect.bisect_left(
                [it.msg.timestamp_ns for it in _cached_topic_data_stream],
                time_end,
            )
        )
    )

    # return the (sorted!) list of timestamps >= timestamp_ns_star and < timestamp_ns_end
    return [
        it.msg.timestamp_ns
        for it in _cached_topic_data_stream[msg_idx_start:msg_idx_end]
    ]


def _exec_test_synch(
    data_stream: SequenceDataStream,
    topics: List[str],
    target_fps: int,
    timestamp_ns_start: Optional[int],
    timestamp_ns_end: Optional[int],
    seqhandler: SequenceHandler,
):
    min_time = (
        max(timestamp_ns_start, data_stream.tstamp_ns_start)
        if timestamp_ns_start is not None
        else data_stream.tstamp_ns_start
    )
    max_time = (
        min(timestamp_ns_end, data_stream.tstamp_ns_end)
        if timestamp_ns_end is not None
        else data_stream.tstamp_ns_end
    )
    total_sec = (max_time - min_time) / 1e9
    # make such that we receive more than 1 chunk
    window_chunk_sec = total_sec / 2

    # Get the selected topics
    topics = topics or topic_list

    # Get the original topic timestamps corresponding to 'timestamp_ns_start' to 'timestamp_ns_end'
    topic_timestamps = []
    for topic in topics:
        topic_timestamps.extend(
            _get_topic_timestamps(
                data_stream=data_stream,
                topic=topic,
                time_start=min_time,
                time_end=max_time,
            )
        )
    topic_timestamps = sorted(topic_timestamps)

    for chunk in DataFrameExtractor(seqhandler).to_pandas_chunks(
        topics=topics,
        window_sec=window_chunk_sec,
        timestamp_ns_start=timestamp_ns_start,
        timestamp_ns_end=timestamp_ns_end,
    ):
        stransformer = SyncTransformer(target_fps=target_fps)
        synched_df = stransformer.transform(chunk)
        deltas = synched_df["timestamp_ns"].diff(1)[
            1:
        ]  # remove the first diff which is NaN
        assert all(deltas == np.repeat(1e9 / target_fps, len(deltas)))


def test_single_selection_synch_unbounded(
    _client: MosaicoClient,
    _make_sequence_data_stream: SequenceDataStream,  # Get the data stream for comparisons
    # _inject_sequence_data_stream,  # Make sure data are available on the server
):
    """Test retrieving the topic data-stream from start to end, unbounded"""

    seqhandler = _client.sequence_handler(UPLOADED_SEQUENCE_NAME)
    # Sequence must exist
    assert seqhandler is not None
    # --- Topic 1 ---

    selection = [UPLOADED_IMU_FRONT_TOPIC]

    _exec_test_synch(
        data_stream=_make_sequence_data_stream,
        topics=selection,
        target_fps=1_000,
        seqhandler=seqhandler,
        timestamp_ns_start=None,
        timestamp_ns_end=None,
    )

    # free resources
    _client.close()
