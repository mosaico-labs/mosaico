import functools
from fractions import Fraction

import numpy as np
import pandas as pd
import pytest

from mosaicolabs.ml import SyncAsOf, SyncDrop, SyncHold, SyncTransformer


def test_sync_transformer_fps_to_ns_alignment():
    """
    Verifies that target_fps (Hz) correctly translates to nanosecond grid steps.
    Example: 10 Hz should result in ticks every 100,000,000 ns.
    """
    # 10 Hz Target
    transformer = SyncTransformer(target_fps=10, policy=SyncHold())

    # Sparse data spanning 300ms
    df = pd.DataFrame({"timestamp_ns": [0, 300_000_000], "data": [1, 2]})

    dense_df = transformer.fit(df).transform(df)

    # We expect ticks at 0, 100ms, 200ms, 300ms
    expected_ticks = [0, 100_000_000, 200_000_000, 300_000_000]
    assert dense_df["timestamp_ns"].tolist() == expected_ticks
    assert transformer._period_ns == 100_000_000


def test_sync_transformer_hold_policy():
    """
    Tests that a sensor arriving late results in 'None' for early ticks.
    """
    # 5 Hz Target (200ms steps)
    transformer = SyncTransformer(target_fps=5, policy=SyncHold())

    # IMU starts at 0, val2 arrives at 600ms
    sparse_data = {
        "timestamp_ns": [
            0,
            600_000_000,
            900_000_000,
            1_200_000_000,
            1_500_000_000,
        ],
        "val1": [10.0, 11.0, None, 12.0, 13.0],
        "val2": [None, 1.0, 2.0, None, 3.0],
    }
    df = pd.DataFrame(sparse_data)
    dense_df = transformer.fit(df).transform(df)
    expected_tstamp = [
        0,
        200_000_000,
        400_000_000,
        600_000_000,
        800_000_000,
        1_000_000_000,
        1_200_000_000,
        1_400_000_000,
    ]
    expected_val1 = [10, 10, 10, 11, 11, 11, 12, 12]
    expected_val2 = [None, None, None, 1, 1, 2, 2, 2]

    assert list(dense_df["timestamp_ns"]) == expected_tstamp
    assert list(dense_df["val1"]) == expected_val1
    assert list(dense_df["val2"]) == expected_val2


def test_sync_transformer_as_of_policy():
    """
    Tests that SyncAsOf correctly returns None when data becomes stale.
    Verifies that values are held only within the specified tolerance_ns.
    """
    # 5 Hz Target (200ms steps), Tolerance of 100ms
    # Any sample older than 100ms relative to the tick should be None
    policy = SyncAsOf(tolerance_ns=150_000_000)
    transformer = SyncTransformer(target_fps=5, policy=policy)

    sparse_data = {
        "timestamp_ns": [
            0,  # Sample 1: Fresh for tick 0
            250_000_000,  # Sample 2: Fresh for tick 200ms (delta 50ms < 100ms)
            600_000_000,  # Sample 3: Fresh for tick 600ms, stale for 800ms (delta 200ms > 100ms)
        ],
        "val": [10.0, 11.0, 12.0],
    }
    df = pd.DataFrame(sparse_data)
    dense_df = transformer.fit(df).transform(df)

    expected_tstamp = [0, 200_000_000, 400_000_000, 600_000_000]
    # at 200ms: last sample was at 0ms  (delta 200ms > 150ms) -> None
    # at 400ms: last sample was at 250ms (delta 150ms == 150ms) -> val
    # at 600ms: last sample was at 600ms (delta 0ms < 150ms) -> val
    expected_val = [10.0, None, 11.0, 12.0]

    assert list(dense_df["timestamp_ns"]) == expected_tstamp
    assert list(dense_df["val"]) == expected_val


def test_sync_transformer_drop_policy():
    """
    Tests that SyncDrop restricts values to the grid interval (t - step_ns, t].
    Ensures that if no new sample arrives within a step, the output is None.
    """
    # 5 Hz Target (200ms steps)
    # Policy uses step_ns (200ms) as the implicit window
    policy = SyncDrop(step_ns=200_000_000)
    transformer = SyncTransformer(target_fps=5, policy=policy)

    sparse_data = {
        "timestamp_ns": [
            0,  # Tick 0: (None, 0] -> Found
            150_000_000,  # Tick 200: (0, 200] -> Found
            450_000_000,  # Missing sample for tick 400ms interval (200, 400]
            600_000_000,  # Tick 600: (400, 600] -> Found
        ],
        "val": [1.0, 2.0, 3.0, 4.0],
    }
    df = pd.DataFrame(sparse_data)
    dense_df = transformer.fit(df).transform(df)

    expected_tstamp = [0, 200_000_000, 400_000_000, 600_000_000]
    # at 400ms: last sample was at 150ms. Delta 250ms >= 200ms (step_ns) -> None
    expected_val = [1.0, 2.0, None, 4.0]

    assert list(dense_df["timestamp_ns"]) == expected_tstamp
    assert list(dense_df["val"]) == expected_val


def test_sync_transformer_state_carry_over():
    """
    Verifies state persistence across two 5-second chunks (at 1 Hz).
    """
    # 1 Hz Target (1s steps)
    transformer = SyncTransformer(target_fps=1, policy=SyncHold())

    # Chunk 1: 0s to 2s
    df1 = pd.DataFrame(
        {"timestamp_ns": [0, 1_000_000_000, 2_000_000_000], "val": [1, 2, 3]}
    )
    transformer.transform(df1)

    # Chunk 2: Starts at 4s, but should carry '3' forward to the 3s and 4s ticks
    # (Note: the grid continues from _origin_ns and _tick)
    df2 = pd.DataFrame({"timestamp_ns": [4_000_000_000], "val": [5]})
    dense2 = transformer.transform(df2)

    expected_tstamp = [3_000_000_000, 4_000_000_000]

    # The grid should have generated 3s and 4s
    assert list(dense2["timestamp_ns"]) == expected_tstamp
    # Check that '3' was carried over into the first tick of the new chunk
    assert dense2.loc[dense2["timestamp_ns"] == 3_000_000_000, "val"].values[0] == 3


def test_sync_transformer_reset():
    """Verifies that reset() clears temporal alignment and sensor memory."""
    transformer = SyncTransformer(target_fps=10, policy=SyncHold())
    df = pd.DataFrame({"timestamp_ns": [0, 100_000_000], "val": [1, 2]})

    transformer.fit(df).transform(df)
    assert transformer._origin_ns is not None

    transformer.reset()
    assert transformer._origin_ns is None
    assert len(transformer._last_values) == 0


# Regression tests for #787: long runs must not drift from the ideal grid
T0 = 1_700_000_000_000_000_000  # epoch-like origin (ns)
HOUR_NS = 3600 * 10**9
DAY_NS = 24 * HOUR_NS
# Chunk count for the chunked test: chunk borders fall at irregular grid points
CHUNKS = 97

# (target fps, span of the data): every case produces millions of ticks
GRID_CASES = [
    pytest.param(1024, 2 * HOUR_NS, id="1024fps-2h"),
    pytest.param(59.94, DAY_NS, id="59.94fps-1day"),
    pytest.param(30, DAY_NS, id="30fps-1day"),
    pytest.param(29.97, DAY_NS, id="29.97fps-1day"),
    pytest.param(np.float32(29.97), DAY_NS, id="float32-29.97fps-1day"),
    pytest.param(23.976, DAY_NS, id="23.976fps-1day"),
    pytest.param(7, 7 * DAY_NS, id="7fps-7days"),
    pytest.param(1, 7 * DAY_NS, id="1fps-7days"),
]


@functools.cache
def _make_sparse_df(span_ns: int) -> pd.DataFrame:
    """
    Sensors at 100 Hz and 30 Hz (recording the first minute of every hour)
    and at 1 Hz (always on). Each value is the index of its sample.
    """
    hours = np.arange(span_ns // HOUR_NS, dtype=np.int64)[:, None] * HOUR_NS
    sensors = []
    for hz, seconds_per_hour in ((100, 60), (30, 60), (1, 3600)):
        in_hour = np.arange(seconds_per_hour * hz, dtype=np.int64) * 10**9 // hz
        ts = (T0 + hours + in_hour).ravel()
        values = np.arange(len(ts), dtype=float)
        sensors.append(pd.DataFrame({"timestamp_ns": ts, f"sensor_{hz}hz": values}))
    return pd.concat(sensors).groupby("timestamp_ns", as_index=False).first()


def _check_grid_and_values(dense_df, sparse_df, target_fps):
    # Tick k is at T0 + round_half_up(k * 1e9 / target_fps), computed exactly
    period = Fraction(10**9) / Fraction(str(target_fps))
    num, den = period.numerator, period.denominator
    q, r = divmod(num, den)
    span = int(sparse_df["timestamp_ns"].iloc[-1]) - T0
    # Ticks k with k * period + 0.5 < span + 1
    n_ticks = -(-(2 * span + 1) * den // (2 * num))
    k = np.arange(n_ticks, dtype=np.int64)
    expected = T0 + k * q + (2 * k * r + den) // (2 * den)

    grid = dense_df["timestamp_ns"].to_numpy()
    assert len(grid) == n_ticks
    # The period is a float, so a tick may be 1 ns off; drift would grow far beyond
    assert np.abs(grid - expected).max() <= 1

    # SyncHold: each tick holds the last sample at or before it
    for col in sparse_df.columns.drop("timestamp_ns"):
        ts = sparse_df.loc[sparse_df[col].notna(), "timestamp_ns"].to_numpy()
        last = np.searchsorted(ts, grid, side="right") - 1
        assert np.array_equal(dense_df[col].to_numpy(dtype=float), last)


@pytest.mark.parametrize("target_fps, span_ns", GRID_CASES)
def test_sync_transformer_grid_single_dataframe(target_fps, span_ns):
    """Verifies the grid and the held values over a single long dataframe."""
    sparse_df = _make_sparse_df(span_ns)
    transformer = SyncTransformer(target_fps=target_fps, policy=SyncHold())

    dense_df = transformer.fit(sparse_df).transform(sparse_df)

    _check_grid_and_values(dense_df, sparse_df, target_fps)


@pytest.mark.parametrize("target_fps, span_ns", GRID_CASES)
def test_sync_transformer_grid_chunked_dataframes(target_fps, span_ns):
    """Verifies that feeding the same data in chunks gives the same grid and values."""
    sparse_df = _make_sparse_df(span_ns)
    transformer = SyncTransformer(target_fps=target_fps, policy=SyncHold())
    chunks = sparse_df.groupby((sparse_df["timestamp_ns"] - T0) // (span_ns // CHUNKS))

    dense_df = pd.concat(
        [transformer.fit(chunk).transform(chunk) for _, chunk in chunks],
        ignore_index=True,
    )

    _check_grid_and_values(dense_df, sparse_df, target_fps)
