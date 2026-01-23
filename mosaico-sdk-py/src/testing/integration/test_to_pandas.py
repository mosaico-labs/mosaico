import bisect
from math import ceil
from mosaicolabs.handlers.sequence_handler import SequenceHandler
from typing import List, Iterable, Optional

from mosaicolabs.comm import MosaicoClient
from mosaicolabs.ml import DataFrameExtractor
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


def _assert_chunk_topic_prefix(
    chunk_columns: Iterable[str],
    topics: List[str],
):
    """
    Assert that:
    - All other columns (than 'timestamp_ns') start with one of the given topics
    - No columns outside the given topics exist
    """
    # Columns minus timestamp
    non_ts_cols = [c for c in chunk_columns if c != "timestamp_ns"]

    # Check each column starts with one of the topics
    for col in non_ts_cols:
        if not any(col.startswith(f"{topic}.") for topic in topics):
            raise AssertionError(
                f"Column '{col}' does not start with any allowed topic prefix {topics}"
            )

    # Check that all topic prefixes are represented if needed
    prefixes_found = {col.split(".")[0] for col in non_ts_cols}
    unexpected_topics = prefixes_found - set(topics)
    assert not unexpected_topics, (
        f"Unexpected topic prefixes found: {unexpected_topics}"
    )


def _assert_chunk_topic_columns(
    chunk_columns: Iterable[str],
    topic: str,
    fields_leaf: Iterable[str],
    fields_all_subfields: Iterable[str],
):
    """
    Assert that a DataFrame chunk has:
    - all other columns (than 'timestamp_ns') are prefixed with topic
    - exactly the requested leaf fields
    - all subfields of requested group fields
    """
    # Columns minus timestamp
    non_ts_cols = [c for c in chunk_columns if c != "timestamp_ns"]

    # Strip namespace for semantic checks
    logical_cols = {c.removeprefix(f"{topic}.") for c in non_ts_cols}

    # Check leaf fields
    for leaf in fields_leaf:
        assert leaf in logical_cols, f"Missing leaf field: {leaf}"
        # Make sure no extra subfields exist for this leaf
        assert not any(c.startswith(leaf + ".") for c in logical_cols), (
            f"Unexpected subfields for leaf field: {leaf}"
        )

    # Check all-subfields fields
    for group in fields_all_subfields:
        subfields = {c for c in logical_cols if c.startswith(group + ".")}
        assert subfields, f"No subfields found for group field: {group}"

    # Make sure there are no extra columns
    allowed = set(fields_leaf)
    for group in fields_all_subfields:
        allowed |= {c for c in logical_cols if c.startswith(group + ".")}

    assert logical_cols == allowed, (
        f"Unexpected columns found: {logical_cols - allowed}"
    )


def _exec_test_chunks(
    data_stream: SequenceDataStream,
    selection_dict: dict[str, dict],
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
    expected_num_chunks = ceil(total_sec / window_chunk_sec)

    # Get the selected topics
    topics = list(selection_dict.keys()) if selection_dict else topic_list
    # Compose 'selection' from input dict
    selection = (
        [
            (topic, fields["fields_leaf"] + fields["fields_all_subfields"])
            for topic, fields in selection_dict.items()
        ]
        if selection_dict
        else None
    )

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

    # Counter for the number of received chunks
    num_chunks = 0
    # Cache for the list of packed timestamps
    chunk_timestamps = []
    for chunk in DataFrameExtractor(seqhandler).to_pandas_chunks(
        selection=selection,
        window_sec=window_chunk_sec,
        timestamp_ns_start=timestamp_ns_start,
        timestamp_ns_end=timestamp_ns_end,
    ):
        # Check timestamp
        assert "timestamp_ns" in chunk.columns, "'timestamp_ns' missing"

        # Assert the columns contain the selected topics only
        _assert_chunk_topic_prefix(chunk_columns=chunk.columns, topics=topics)

        # Assert the 'data' columns for each topic
        # FIXME: if not setting the selected column,
        # must test all the topic column (from data ontology)
        if selection:
            for topic in topics:
                item = selection_dict[topic]
                fields_leaf = item["fields_leaf"]
                fields_all_subfields = item["fields_all_subfields"]
                topic_columns = [
                    c
                    for c in chunk.columns
                    if c == "timestamp_ns" or c.startswith(topic)
                ]
                _assert_chunk_topic_columns(
                    chunk_columns=topic_columns,
                    topic=topic,
                    fields_leaf=fields_leaf,
                    fields_all_subfields=fields_all_subfields,
                )
        chunk_timestamps.extend(list(chunk["timestamp_ns"]))
        num_chunks += 1

    # We have received the expected number of chunks
    assert expected_num_chunks == num_chunks
    # We have received the expected sorted list of data points
    assert chunk_timestamps == topic_timestamps


def test_single_selection_chunks_unbounded(
    _client: MosaicoClient,
    _make_sequence_data_stream: SequenceDataStream,  # Get the data stream for comparisons
    _inject_sequence_data_stream,  # Make sure data are available on the server
):
    """Test retrieving the topic data-stream from start to end, unbounded"""

    seqhandler = _client.sequence_handler(UPLOADED_SEQUENCE_NAME)
    # Sequence must exist
    assert seqhandler is not None
    # --- Topic 1 ---

    selection_dict = {
        UPLOADED_IMU_FRONT_TOPIC: {
            "fields_leaf": ["acceleration.x", "acceleration.z"],
            "fields_all_subfields": ["angular_velocity", "header"],
        }
    }

    _exec_test_chunks(
        data_stream=_make_sequence_data_stream,
        selection_dict=selection_dict,
        seqhandler=seqhandler,
        timestamp_ns_start=None,
        timestamp_ns_end=None,
    )

    # --- Topic 2 ---

    selection_dict = {
        UPLOADED_GPS_TOPIC: {
            "fields_leaf": ["position.x", "status.service"],
            "fields_all_subfields": ["velocity"],
        }
    }

    _exec_test_chunks(
        data_stream=_make_sequence_data_stream,
        selection_dict=selection_dict,
        seqhandler=seqhandler,
        timestamp_ns_start=None,
        timestamp_ns_end=None,
    )

    # free resources
    _client.close()


def test_single_selection_chunks_from_half_to_end(
    _client: MosaicoClient,
    _make_sequence_data_stream: SequenceDataStream,  # Get the data stream for comparisons
    _inject_sequence_data_stream,  # Make sure data are available on the server
):
    """Test retrieving the topic data-stream from half to end"""
    # start from the half of the sequence
    timestamp_ns_start = _make_sequence_data_stream.tstamp_ns_start + int(
        (
            _make_sequence_data_stream.tstamp_ns_start
            + _make_sequence_data_stream.tstamp_ns_end
        )
        / 2
    )
    timestamp_ns_end = _make_sequence_data_stream.tstamp_ns_end

    seqhandler = _client.sequence_handler(UPLOADED_SEQUENCE_NAME)
    # Sequence must exist
    assert seqhandler is not None
    # --- Topic 1 ---

    selection_dict = {
        UPLOADED_IMU_FRONT_TOPIC: {
            "fields_leaf": ["acceleration.x", "acceleration.z"],
            "fields_all_subfields": ["angular_velocity", "header"],
        }
    }

    _exec_test_chunks(
        data_stream=_make_sequence_data_stream,
        selection_dict=selection_dict,
        seqhandler=seqhandler,
        timestamp_ns_start=timestamp_ns_start,
        timestamp_ns_end=timestamp_ns_end,
    )

    # --- Topic 2 ---

    selection_dict = {
        UPLOADED_GPS_TOPIC: {
            "fields_leaf": ["position.x", "status.service"],
            "fields_all_subfields": ["velocity"],
        }
    }

    _exec_test_chunks(
        data_stream=_make_sequence_data_stream,
        selection_dict=selection_dict,
        seqhandler=seqhandler,
        timestamp_ns_start=timestamp_ns_start,
        timestamp_ns_end=timestamp_ns_end,
    )

    # free resources
    _client.close()


def test_single_selection_chunks_from_half(
    _client: MosaicoClient,
    _make_sequence_data_stream: SequenceDataStream,  # Get the data stream for comparisons
    _inject_sequence_data_stream,  # Make sure data are available on the server
):
    """Test retrieving the topic data-stream from half (unbounded end)"""
    # start from the half of the sequence
    timestamp_ns_start = _make_sequence_data_stream.tstamp_ns_start + int(
        (
            _make_sequence_data_stream.tstamp_ns_start
            + _make_sequence_data_stream.tstamp_ns_end
        )
        / 2
    )

    seqhandler = _client.sequence_handler(UPLOADED_SEQUENCE_NAME)
    # Sequence must exist
    assert seqhandler is not None
    # --- Topic 1 ---

    selection_dict = {
        UPLOADED_IMU_FRONT_TOPIC: {
            "fields_leaf": ["acceleration.x", "acceleration.z"],
            "fields_all_subfields": ["angular_velocity", "header"],
        }
    }

    _exec_test_chunks(
        data_stream=_make_sequence_data_stream,
        selection_dict=selection_dict,
        seqhandler=seqhandler,
        timestamp_ns_start=timestamp_ns_start,
        timestamp_ns_end=None,
    )

    # --- Topic 2 ---

    selection_dict = {
        UPLOADED_GPS_TOPIC: {
            "fields_leaf": ["position.x", "status.service"],
            "fields_all_subfields": ["velocity"],
        }
    }

    _exec_test_chunks(
        data_stream=_make_sequence_data_stream,
        selection_dict=selection_dict,
        seqhandler=seqhandler,
        timestamp_ns_start=timestamp_ns_start,
        timestamp_ns_end=None,
    )

    # free resources
    _client.close()


def test_single_selection_chunks_to_half(
    _client: MosaicoClient,
    _make_sequence_data_stream: SequenceDataStream,  # Get the data stream for comparisons
    _inject_sequence_data_stream,  # Make sure data are available on the server
):
    """Test retrieving the topic data-stream to half (unbounded start)"""

    # start from the half of the sequence
    timestamp_ns_end = _make_sequence_data_stream.tstamp_ns_start + int(
        (
            _make_sequence_data_stream.tstamp_ns_start
            + _make_sequence_data_stream.tstamp_ns_end
        )
        / 2
    )

    seqhandler = _client.sequence_handler(UPLOADED_SEQUENCE_NAME)
    # Sequence must exist
    assert seqhandler is not None
    # --- Topic 1 ---

    selection_dict = {
        UPLOADED_IMU_FRONT_TOPIC: {
            "fields_leaf": ["acceleration.x", "acceleration.z"],
            "fields_all_subfields": ["angular_velocity", "header"],
        }
    }

    _exec_test_chunks(
        data_stream=_make_sequence_data_stream,
        selection_dict=selection_dict,
        seqhandler=seqhandler,
        timestamp_ns_start=None,
        timestamp_ns_end=timestamp_ns_end,
    )

    # --- Topic 2 ---

    selection_dict = {
        UPLOADED_GPS_TOPIC: {
            "fields_leaf": ["position.x", "status.service"],
            "fields_all_subfields": ["velocity"],
        }
    }

    _exec_test_chunks(
        data_stream=_make_sequence_data_stream,
        selection_dict=selection_dict,
        seqhandler=seqhandler,
        timestamp_ns_start=None,
        timestamp_ns_end=timestamp_ns_end,
    )

    # free resources
    _client.close()


def test_single_selection_chunks_extra_bounds(
    _client: MosaicoClient,
    _make_sequence_data_stream: SequenceDataStream,  # Get the data stream for comparisons
    _inject_sequence_data_stream,  # Make sure data are available on the server
):
    """Test retrieving the topic data-stream with time limits outside the valid sequence time boundaries."""

    seqhandler = _client.sequence_handler(UPLOADED_SEQUENCE_NAME)
    # Sequence must exist
    assert seqhandler is not None
    # --- Topic 1 ---

    selection_dict = {
        UPLOADED_IMU_FRONT_TOPIC: {
            "fields_leaf": ["acceleration.x", "acceleration.z"],
            "fields_all_subfields": ["angular_velocity", "header"],
        }
    }

    _exec_test_chunks(
        data_stream=_make_sequence_data_stream,
        selection_dict=selection_dict,
        seqhandler=seqhandler,
        timestamp_ns_start=0,
        timestamp_ns_end=_make_sequence_data_stream.tstamp_ns_end * 2,
    )

    # free resources
    _client.close()


def test_multi_selection_chunks_unbounded(
    _client: MosaicoClient,
    _make_sequence_data_stream: SequenceDataStream,  # Get the data stream for comparisons
    _inject_sequence_data_stream,  # Make sure data are available on the server
):
    """Test retrieving the data-stream from start to end, unbounded"""

    seqhandler = _client.sequence_handler(UPLOADED_SEQUENCE_NAME)
    # Sequence must exist
    assert seqhandler is not None
    # --- Topic 1 ---

    selection_dict = {
        UPLOADED_IMU_FRONT_TOPIC: {
            "fields_leaf": ["acceleration.x", "acceleration.z"],
            "fields_all_subfields": ["angular_velocity", "header"],
        },
        UPLOADED_GPS_TOPIC: {
            "fields_leaf": ["position.x", "status.service"],
            "fields_all_subfields": ["velocity"],
        },
        UPLOADED_IMU_CAMERA_TOPIC: {
            "fields_leaf": ["acceleration.x", "acceleration.y"],
            "fields_all_subfields": ["angular_velocity", "orientation"],
        },
    }

    _exec_test_chunks(
        data_stream=_make_sequence_data_stream,
        selection_dict=selection_dict,
        seqhandler=seqhandler,
        timestamp_ns_start=None,
        timestamp_ns_end=None,
    )

    # free resources
    _client.close()


def test_multi_selection_chunks_extra_bounds(
    _client: MosaicoClient,
    _make_sequence_data_stream: SequenceDataStream,  # Get the data stream for comparisons
    _inject_sequence_data_stream,  # Make sure data are available on the server
):
    """Test retrieving the data-stream from start to end, unbounded"""

    seqhandler = _client.sequence_handler(UPLOADED_SEQUENCE_NAME)
    # Sequence must exist
    assert seqhandler is not None
    # --- Topic 1 ---

    selection_dict = {
        UPLOADED_IMU_FRONT_TOPIC: {
            "fields_leaf": ["acceleration.x", "acceleration.z"],
            "fields_all_subfields": ["angular_velocity", "header"],
        },
        UPLOADED_GPS_TOPIC: {
            "fields_leaf": ["position.x", "status.service"],
            "fields_all_subfields": ["velocity"],
        },
        UPLOADED_IMU_CAMERA_TOPIC: {
            "fields_leaf": ["acceleration.x", "acceleration.y"],
            "fields_all_subfields": ["angular_velocity", "orientation"],
        },
    }

    _exec_test_chunks(
        data_stream=_make_sequence_data_stream,
        selection_dict=selection_dict,
        seqhandler=seqhandler,
        timestamp_ns_start=0,
        timestamp_ns_end=_make_sequence_data_stream.tstamp_ns_end * 2,
    )

    # free resources
    _client.close()


def test_multi_selection_chunks_from_half_to_end(
    _client: MosaicoClient,
    _make_sequence_data_stream: SequenceDataStream,  # Get the data stream for comparisons
    _inject_sequence_data_stream,  # Make sure data are available on the server
):
    """Test retrieving the data-stream from half to end"""
    # start from the half of the sequence
    timestamp_ns_start = _make_sequence_data_stream.tstamp_ns_start + int(
        (
            _make_sequence_data_stream.tstamp_ns_start
            + _make_sequence_data_stream.tstamp_ns_end
        )
        / 2
    )
    timestamp_ns_end = _make_sequence_data_stream.tstamp_ns_end

    seqhandler = _client.sequence_handler(UPLOADED_SEQUENCE_NAME)
    # Sequence must exist
    assert seqhandler is not None
    # --- Topic 1 ---

    selection_dict = {
        UPLOADED_IMU_FRONT_TOPIC: {
            "fields_leaf": ["acceleration.x", "acceleration.z"],
            "fields_all_subfields": ["angular_velocity", "header"],
        },
        UPLOADED_GPS_TOPIC: {
            "fields_leaf": ["position.x", "status.service"],
            "fields_all_subfields": ["velocity"],
        },
        UPLOADED_IMU_CAMERA_TOPIC: {
            "fields_leaf": ["acceleration.x", "acceleration.y"],
            "fields_all_subfields": ["angular_velocity", "orientation"],
        },
    }

    _exec_test_chunks(
        data_stream=_make_sequence_data_stream,
        selection_dict=selection_dict,
        seqhandler=seqhandler,
        timestamp_ns_start=timestamp_ns_start,
        timestamp_ns_end=timestamp_ns_end,
    )

    # free resources
    _client.close()


def test_multi_selection_chunks_from_half(
    _client: MosaicoClient,
    _make_sequence_data_stream: SequenceDataStream,  # Get the data stream for comparisons
    _inject_sequence_data_stream,  # Make sure data are available on the server
):
    """Test retrieving the data-stream from half (unbounded end)"""
    # start from the half of the sequence
    timestamp_ns_start = _make_sequence_data_stream.tstamp_ns_start + int(
        (
            _make_sequence_data_stream.tstamp_ns_start
            + _make_sequence_data_stream.tstamp_ns_end
        )
        / 2
    )

    seqhandler = _client.sequence_handler(UPLOADED_SEQUENCE_NAME)
    # Sequence must exist
    assert seqhandler is not None
    # --- Topic 1 ---

    selection_dict = {
        UPLOADED_IMU_FRONT_TOPIC: {
            "fields_leaf": ["acceleration.x", "acceleration.z"],
            "fields_all_subfields": ["angular_velocity", "header"],
        },
        UPLOADED_GPS_TOPIC: {
            "fields_leaf": ["position.x", "status.service"],
            "fields_all_subfields": ["velocity"],
        },
        UPLOADED_IMU_CAMERA_TOPIC: {
            "fields_leaf": ["acceleration.x", "acceleration.y"],
            "fields_all_subfields": ["angular_velocity", "orientation"],
        },
    }

    _exec_test_chunks(
        data_stream=_make_sequence_data_stream,
        selection_dict=selection_dict,
        seqhandler=seqhandler,
        timestamp_ns_start=timestamp_ns_start,
        timestamp_ns_end=None,
    )

    # free resources
    _client.close()


def test_multi_selection_chunks_to_half(
    _client: MosaicoClient,
    _make_sequence_data_stream: SequenceDataStream,  # Get the data stream for comparisons
    _inject_sequence_data_stream,  # Make sure data are available on the server
):
    """Test retrieving the data-stream to half (unbounded start)"""
    # start from the half of the sequence
    timestamp_ns_end = _make_sequence_data_stream.tstamp_ns_start + int(
        (
            _make_sequence_data_stream.tstamp_ns_start
            + _make_sequence_data_stream.tstamp_ns_end
        )
        / 2
    )

    seqhandler = _client.sequence_handler(UPLOADED_SEQUENCE_NAME)
    # Sequence must exist
    assert seqhandler is not None
    # --- Topic 1 ---

    selection_dict = {
        UPLOADED_IMU_FRONT_TOPIC: {
            "fields_leaf": ["acceleration.x", "acceleration.z"],
            "fields_all_subfields": ["angular_velocity", "header"],
        },
        UPLOADED_GPS_TOPIC: {
            "fields_leaf": ["position.x", "status.service"],
            "fields_all_subfields": ["velocity"],
        },
        UPLOADED_IMU_CAMERA_TOPIC: {
            "fields_leaf": ["acceleration.x", "acceleration.y"],
            "fields_all_subfields": ["angular_velocity", "orientation"],
        },
    }

    _exec_test_chunks(
        data_stream=_make_sequence_data_stream,
        selection_dict=selection_dict,
        seqhandler=seqhandler,
        timestamp_ns_start=None,
        timestamp_ns_end=timestamp_ns_end,
    )

    # free resources
    _client.close()


def test_single_selection_non_existing_field(
    _client: MosaicoClient,
    _make_sequence_data_stream: SequenceDataStream,  # Get the data stream for comparisons
    _inject_sequence_data_stream,  # Make sure data are available on the server
):
    """Test retrieving a non-existing column from topic data-stream"""
    # start from the half of the sequence
    timestamp_ns_start = _make_sequence_data_stream.tstamp_ns_start + int(
        (
            _make_sequence_data_stream.tstamp_ns_start
            + _make_sequence_data_stream.tstamp_ns_end
        )
        / 2
    )
    timestamp_ns_end = _make_sequence_data_stream.tstamp_ns_end

    seqhandler = _client.sequence_handler(UPLOADED_SEQUENCE_NAME)
    # Sequence must exist
    assert seqhandler is not None
    # --- Topic 1 ---

    selection = [
        (UPLOADED_IMU_FRONT_TOPIC, ["acceleration.h"])
    ]  # NOTE: <- Does not exist

    with pytest.raises(ValueError):
        for _ in DataFrameExtractor(seqhandler).to_pandas_chunks(
            selection=selection,
            window_sec=5,
            timestamp_ns_start=timestamp_ns_start,
            timestamp_ns_end=timestamp_ns_end,
        ):
            pass

    selection = [
        (
            UPLOADED_IMU_FRONT_TOPIC,
            [
                "acceleration",
                "non-existing-field",  # NOTE: <- Does not exist
            ],
        )
    ]

    with pytest.raises(
        ValueError,
        match="The field 'non-existing-field' does not exist in the columns.",
    ):
        for _ in DataFrameExtractor(seqhandler).to_pandas_chunks(
            selection=selection,
            window_sec=5,
            timestamp_ns_start=timestamp_ns_start,
            timestamp_ns_end=timestamp_ns_end,
        ):
            pass

    selection = ["non-existing-topic"]  # NOTE: <- Does not exist

    with pytest.raises(ValueError, match="Invalid input topic names"):
        for _ in DataFrameExtractor(seqhandler).to_pandas_chunks(
            selection=selection,
            window_sec=5,
            timestamp_ns_start=timestamp_ns_start,
            timestamp_ns_end=timestamp_ns_end,
        ):
            pass

    # free resources
    _client.close()


def test_sequence_chunks_unbounded(
    _client: MosaicoClient,
    _make_sequence_data_stream: SequenceDataStream,  # Get the data stream for comparisons
    _inject_sequence_data_stream,  # Make sure data are available on the server
):
    """Test retrieving the topic data-stream from start to end, unbounded"""

    seqhandler = _client.sequence_handler(UPLOADED_SEQUENCE_NAME)
    # Sequence must exist
    assert seqhandler is not None
    # --- Topic 1 ---

    _exec_test_chunks(
        data_stream=_make_sequence_data_stream,
        selection_dict={},
        seqhandler=seqhandler,
        timestamp_ns_start=None,
        timestamp_ns_end=None,
    )

    # free resources
    _client.close()


def test_sequence_chunks_from_half_to_end(
    _client: MosaicoClient,
    _make_sequence_data_stream: SequenceDataStream,  # Get the data stream for comparisons
    _inject_sequence_data_stream,  # Make sure data are available on the server
):
    """Test retrieving the topic data-stream from start to end, unbounded"""

    timestamp_ns_start = _make_sequence_data_stream.tstamp_ns_start + int(
        (
            _make_sequence_data_stream.tstamp_ns_start
            + _make_sequence_data_stream.tstamp_ns_end
        )
        / 2
    )
    timestamp_ns_end = _make_sequence_data_stream.tstamp_ns_end
    seqhandler = _client.sequence_handler(UPLOADED_SEQUENCE_NAME)
    # Sequence must exist
    assert seqhandler is not None
    # --- Topic 1 ---

    _exec_test_chunks(
        data_stream=_make_sequence_data_stream,
        selection_dict={},
        seqhandler=seqhandler,
        timestamp_ns_start=timestamp_ns_start,
        timestamp_ns_end=timestamp_ns_end,
    )

    # free resources
    _client.close()


def test_sequence_chunks_from_half(
    _client: MosaicoClient,
    _make_sequence_data_stream: SequenceDataStream,  # Get the data stream for comparisons
    _inject_sequence_data_stream,  # Make sure data are available on the server
):
    """Test retrieving the topic data-stream from start to end, unbounded"""

    timestamp_ns_start = _make_sequence_data_stream.tstamp_ns_start + int(
        (
            _make_sequence_data_stream.tstamp_ns_start
            + _make_sequence_data_stream.tstamp_ns_end
        )
        / 2
    )
    seqhandler = _client.sequence_handler(UPLOADED_SEQUENCE_NAME)
    # Sequence must exist
    assert seqhandler is not None
    # --- Topic 1 ---

    _exec_test_chunks(
        data_stream=_make_sequence_data_stream,
        selection_dict={},
        seqhandler=seqhandler,
        timestamp_ns_start=timestamp_ns_start,
        timestamp_ns_end=None,
    )

    # free resources
    _client.close()


def test_sequence_chunks_to_half(
    _client: MosaicoClient,
    _make_sequence_data_stream: SequenceDataStream,  # Get the data stream for comparisons
    _inject_sequence_data_stream,  # Make sure data are available on the server
):
    """Test retrieving the topic data-stream from start to end, unbounded"""

    timestamp_ns_end = _make_sequence_data_stream.tstamp_ns_start + int(
        (
            _make_sequence_data_stream.tstamp_ns_start
            + _make_sequence_data_stream.tstamp_ns_end
        )
        / 2
    )
    seqhandler = _client.sequence_handler(UPLOADED_SEQUENCE_NAME)
    # Sequence must exist
    assert seqhandler is not None
    # --- Topic 1 ---

    _exec_test_chunks(
        data_stream=_make_sequence_data_stream,
        selection_dict={},
        seqhandler=seqhandler,
        timestamp_ns_start=None,
        timestamp_ns_end=timestamp_ns_end,
    )

    # free resources
    _client.close()


def test_sequence_no_data(
    _client: MosaicoClient,
    _inject_sequences_mockup,  # Make sure data are available on the server
):
    """Test retrieving the dataframe from an empty (no data stream) sequence"""
    # start from the half of the sequence

    seqhandler = _client.sequence_handler("test-query-sequence-1")
    # Sequence must exist
    assert seqhandler is not None

    with pytest.raises(ValueError, match="The sequence might contain no data"):
        for _ in DataFrameExtractor(seqhandler).to_pandas_chunks(
            selection=None,  # all the topics
            window_sec=5,
            timestamp_ns_start=None,
            timestamp_ns_end=None,
        ):
            pass

    # free resources
    _client.close()
