import pandas as pd
from mosaicolabs.ml import SyncTransformer, SyncHold, SyncAsOf, SyncDrop


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
    assert transformer._step_ns == 100_000_000


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
    # (Note: grid logic will generate ticks based on _next_timestamp_ns)
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
    assert transformer._next_timestamp_ns is not None

    transformer.reset()
    assert transformer._next_timestamp_ns is None
    assert len(transformer._last_values) == 0
