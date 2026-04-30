import numpy as np
import pandas as pd
import pytest

from mosaicolabs.ml.sync_policies.hold import SyncHold
from mosaicolabs.ml.sync_transformer import SyncTransformer


def _make_chunks():
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
                np.array([1.0, 2.0, 3.0]),
                np.array([1.1, 2.1, 3.1]),
            ],
        }
    )
    chunk2 = pd.DataFrame(
        {
            "timestamp_ns": [500_000_000, 600_000_000, 700_000_000],  # 10Hz
            "joints": [
                np.array([1.0, 2.0, 3.0]),
                np.array([1.1, 2.1, 3.1]),
                np.array([1.2, 2.2, 3.2]),
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
                np.array([1.0, 2.0, 3.0]),
                np.array([1.1, 2.1, 3.1]),
                np.array([1.2, 2.2, 3.2]),
                np.array([1.3, 2.3, 3.3]),
                np.array([1.4, 2.4, 3.4]),
            ],
        }
    )

    return chunk0, chunk1, chunk2, chunk3


# Data are sampled at 10Hz: testing for fps below 5Hz can return empty timestamps grid
@pytest.mark.parametrize("fps", [5, 10, 15, 20, 50, 100])
def test_transform_handles_ndarray_cells_across_chunks(fps: int):
    sync = SyncTransformer(target_fps=fps, policy=SyncHold())

    chunks_tuple = _make_chunks()
    assert chunks_tuple and "Empty chunks tuple"

    sync = sync.fit(chunks_tuple[0])
    outs_fit = []
    for chunk in chunks_tuple:
        out = sync.transform(chunk)
        assert len(out) > 0
        outs_fit.append(out)

    # the same result is expected here
    sync = SyncTransformer(target_fps=fps, policy=SyncHold())
    outs = []
    for chunk in chunks_tuple:
        out = sync.transform(chunk)
        assert len(out) > 0
        outs.append(out)

    assert all(df1.equals(df2) for df1, df2 in zip(outs, outs_fit))
