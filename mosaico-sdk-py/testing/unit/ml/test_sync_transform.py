import numpy as np
import pandas as pd
import pytest

from mosaicolabs.ml.sync_policies.hold import SyncHold
from mosaicolabs.ml.sync_transformer import SyncTransformer


def _make_ndarray_chunks():
    chunk0 = pd.DataFrame(
        {
            "timestamp_ns": [0, 100_000_000, 200_000_000],  # 10Hz
            "joints": [
                np.array([1.0, 2.0, 3.0]),
                np.array([1.1, 2.1, 3.1]),
                np.array([1.2, 2.2, 3.2]),
            ],
        }
    )
    chunk1 = pd.DataFrame(
        {
            "timestamp_ns": [300_000_000, 400_000_000],  # 10Hz
            "joints": [
                np.array([11.0, 22.0, 33.0]),
                np.array([11.1, 22.1, 33.1]),
            ],
        }
    )
    chunk2 = pd.DataFrame(
        {
            "timestamp_ns": [500_000_000, 600_000_000, 700_000_000],  # 10Hz
            "joints": [
                np.array([111.0, 222.0, 333.0]),
                np.array([111.1, 222.1, 333.1]),
                np.array([111.2, 222.2, 333.2]),
            ],
        }
    )
    chunk3 = pd.DataFrame(
        {
            "timestamp_ns": [
                800_000_000,
                900_000_000,
                1_000_000_000,
                1_100_000_000,
                1_200_000_000,
            ],  # 10Hz
            "joints": [
                np.array([1111.0, 2222.0, 3333.0]),
                np.array([1111.1, 2222.1, 3333.1]),
                np.array([1111.2, 2222.2, 3333.2]),
                np.array([1111.3, 2222.3, 3333.3]),
                np.array([1111.4, 2222.4, 3333.4]),
            ],
        }
    )

    return chunk0, chunk1, chunk2, chunk3


def _make_numeric_chunks():
    chunk0 = pd.DataFrame(
        {
            "timestamp_ns": [0, 100_000_000, 200_000_000],  # 10Hz
            "joints": [
                1.0,
                1.1,
                1.2,
            ],
        }
    )
    chunk1 = pd.DataFrame(
        {
            "timestamp_ns": [300_000_000, 400_000_000],  # 10Hz
            "joints": [
                11.0,
                11.1,
            ],
        }
    )
    chunk2 = pd.DataFrame(
        {
            "timestamp_ns": [500_000_000, 600_000_000, 700_000_000],  # 10Hz
            "joints": [
                111.0,
                111.1,
                111.2,
            ],
        }
    )
    chunk3 = pd.DataFrame(
        {
            "timestamp_ns": [
                800_000_000,
                900_000_000,
                1_000_000_000,
                1_100_000_000,
                1_200_000_000,
            ],  # 10Hz
            "joints": [
                1111.0,
                1111.1,
                1111.2,
                1111.3,
                1111.4,
            ],
        }
    )

    return chunk0, chunk1, chunk2, chunk3


# Data are sampled at 10Hz: testing for fps below 5Hz can return empty timestamps grid
@pytest.mark.parametrize("fps", [5, 10, 15, 20, 50, 100])
def test_transform_handles_ndarray_cells_across_chunks(fps: int):

    chunks_tuple = _make_ndarray_chunks()
    assert chunks_tuple and "Empty chunks tuple"

    sync = SyncTransformer(target_fps=fps, policy=SyncHold())
    sync = sync.fit(chunks_tuple[0])
    outs_fit = []
    for chunk in chunks_tuple:
        out = sync.transform(chunk)
        assert len(out) > 0
        for _, row in out.iterrows():
            joints_row = row["joints"]
            assert isinstance(joints_row, np.ndarray) and joints_row.size == 3
        outs_fit.append(out)

    # the same result is expected here
    sync = SyncTransformer(target_fps=fps, policy=SyncHold())
    outs = []
    for chunk in chunks_tuple:
        out = sync.transform(chunk)
        assert len(out) > 0
        for _, row in out.iterrows():
            joints_row = row["joints"]
            assert isinstance(joints_row, np.ndarray) and joints_row.size == 3
        outs.append(out)

    assert all(df1.equals(df2) for df1, df2 in zip(outs, outs_fit))


# Data are sampled at 10Hz: testing for fps below 5Hz can return empty timestamps grid
@pytest.mark.parametrize("fps", [5, 10, 15, 20, 50, 100])
def test_transform_handles_numeric_cells_across_chunks(fps: int):

    chunks_tuple = _make_numeric_chunks()
    assert chunks_tuple and "Empty chunks tuple"

    sync = SyncTransformer(target_fps=fps, policy=SyncHold())
    sync = sync.fit(chunks_tuple[0])
    outs_fit = []
    for chunk in chunks_tuple:
        out = sync.transform(chunk)
        assert len(out) > 0
        for _, row in out.iterrows():
            joints_row = row["joints"]
            assert isinstance(joints_row, float)
        outs_fit.append(out)

    # the same result is expected here
    sync = SyncTransformer(target_fps=fps, policy=SyncHold())
    outs = []
    for chunk in chunks_tuple:
        out = sync.transform(chunk)
        assert len(out) > 0
        for _, row in out.iterrows():
            joints_row = row["joints"]
            assert isinstance(joints_row, float)

        outs.append(out)

    assert all(df1.equals(df2) for df1, df2 in zip(outs, outs_fit))
