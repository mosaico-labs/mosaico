from mosaicolabs.comm import MosaicoClient
from mosaicolabs.ml import DataFrameExtractor, SyncTransformer
import numpy as np
import pytest
from testing.integration.config import (
    UPLOADED_SEQUENCE_NAME,
    UPLOADED_IMU_FRONT_TOPIC,
    UPLOADED_GPS_TOPIC,
)
from .helpers import SequenceDataStream


def test_invalid_timestamp_column(
    _client: MosaicoClient,
    _inject_sequence_data_stream,  # Make sure data are available on the server
):
    seqhandler = _client.sequence_handler(UPLOADED_SEQUENCE_NAME)
    # Sequence must exist
    assert seqhandler is not None
    # --- Topic 1 ---

    for chunk in DataFrameExtractor(seqhandler).to_pandas_chunks():
        with pytest.raises(ValueError, match="Unable to find time column"):
            stransformer = SyncTransformer(
                target_fps=1, timestamp_column="invalid-name"
            )
            _ = stransformer.transform(chunk)

    # free resources
    _client.close()


def test_sync_unbounded(
    _client: MosaicoClient,
    _make_sequence_data_stream: SequenceDataStream,  # Get the data stream for comparisons
    _inject_sequence_data_stream,  # Make sure data are available on the server
):
    """Test retrieving the topic data-stream from start to end, unbounded"""

    seqhandler = _client.sequence_handler(UPLOADED_SEQUENCE_NAME)
    # Sequence must exist
    assert seqhandler is not None
    # --- Topic 1 ---

    selection = [UPLOADED_IMU_FRONT_TOPIC, UPLOADED_GPS_TOPIC]
    target_fps = 10.0

    imu_tstamps_ns = [
        dstream.msg.timestamp_ns
        for dstream in _make_sequence_data_stream.items
        if dstream.topic == UPLOADED_IMU_FRONT_TOPIC
    ]
    gps_tstamps_ns = [
        dstream.msg.timestamp_ns
        for dstream in _make_sequence_data_stream.items
        if dstream.topic == UPLOADED_GPS_TOPIC
    ]
    min_timestamp_ns = min(imu_tstamps_ns[0], gps_tstamps_ns[0])
    max_timestamp_ns = max(imu_tstamps_ns[-1], gps_tstamps_ns[-1])

    for chunk in DataFrameExtractor(seqhandler).to_pandas_chunks(
        topics=selection,
        window_sec=1,
    ):
        stransformer = SyncTransformer(target_fps=target_fps)
        synched_df = stransformer.transform(chunk)
        # remove the first diff which is NaN
        deltas = synched_df["timestamp_ns"].diff(1)[1:]

        assert all(deltas == np.repeat(1e9 / target_fps, len(deltas)))
        assert synched_df["timestamp_ns"].values[0] == min_timestamp_ns
        assert synched_df["timestamp_ns"].values[-1] <= max_timestamp_ns
        val_cols = (
            (
                synched_df.columns.str.contains("position")
                | synched_df.columns.str.contains("acceleration")
            )
            & ~synched_df.columns.str.contains("header")
            & ~synched_df.columns.str.contains("covariance")
        )
        selected = synched_df.loc[:, val_cols]
        # For every column in val_cols, there exists at least one non-NaN value.
        assert selected.notna().any().all()
    # free resources
    _client.close()
