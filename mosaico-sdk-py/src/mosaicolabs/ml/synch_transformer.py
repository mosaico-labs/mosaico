import pandas as pd
import numpy as np
from typing import Dict, Any, Optional
from mosaicolabs.logging_config import get_logger

from .enums import SynchPolicy

logger = get_logger(__name__)


class SyncTransformer:
    """
    Stateful transformer for temporal synchronization of sparse DataFrames.

    This class aligns heterogeneous sensor data (e.g., IMU at 100Hz, GPS at 5Hz)
    onto a fixed-frequency grid. It is designed to handle memory-safe
    chunks yielded by the DataFrameExtractor.

    ### Late-Arrival & Initial Hole Management:
    If a sensor topic has no measurements at the start of a sequence, the transformer
    correctly yields 'None' for those ticks to maintain semantic honesty. It does
    not "hallucinate" data before the first physical acquisition occurs.
    Once a sample is detected, the selected policy (e.g., 'hold') takes over,
    storing that value in the internal state to bridge gaps both within the
    current chunk and into subsequent chunks.
    """

    def __init__(
        self,
        target_fps: float,
        policy: SynchPolicy = SynchPolicy.Hold,
        tolerance_ns: Optional[int] = None,
    ):
        """
        Args:
            target_fps: Output frequency in Hz.
            policy: 'hold' (classic), 'as_of' (tolerance), or 'drop' (window).
            tolerance_ns: Time boundary for 'as_of' and 'drop' logic.
        """
        self.target_fps = target_fps
        self.step_ns = int(1e9 / target_fps)  # Convert frequency to nanosecond period
        self.policy = policy
        self.tolerance_ns = tolerance_ns

        # Internal state for continuity
        self._last_values: Dict[str, Any] = {}
        self._next_timestamp_ns: Optional[int] = None

    def fit(self, X: pd.DataFrame, y=None):
        """Initializes the grid alignment based on the first data point."""
        if not X.empty and self._next_timestamp_ns is None:
            self._next_timestamp_ns = X["timestamp_ns"].iloc[0]
        return self

    def transform(self, X: pd.DataFrame) -> pd.DataFrame:
        """
        Synchronizes a single sparse DataFrame chunk into a dense one.
        """
        if X.empty:
            return pd.DataFrame()

        # Initialize grid if this is the first chunk
        if self._next_timestamp_ns is None:
            self._next_timestamp_ns = X["timestamp_ns"].iloc[0]

        # Generate the uniform grid for this chunk
        chunk_end_ns = X["timestamp_ns"].iloc[-1]
        ticks = np.arange(
            self._next_timestamp_ns, chunk_end_ns + 1, self.step_ns, dtype=np.int64
        )

        if len(ticks) == 0:
            return pd.DataFrame()

        dense_df = pd.DataFrame({"timestamp_ns": ticks})

        # Process each sensor column
        for col in [c for c in X.columns if c != "timestamp_ns"]:
            # Drop NaNs to get the sparse samples for this specific sensor
            samples = X[["timestamp_ns", col]].dropna()

            # Synchronize using vectorized searchsorted
            dense_df[col] = self._synchronize_column(ticks, samples, col)

        # Update state for the next chunk
        self._next_timestamp_ns = ticks[-1] + self.step_ns
        return dense_df

    def reset(self):
        """Reset the inner state"""
        self._next_timestamp_ns = None
        self._last_values = {}

    def _synchronize_column(
        self, ticks: np.ndarray, samples: pd.DataFrame, col_name: str
    ) -> np.ndarray:
        """Dispatches the synchronization to the specific policy implementation."""
        # Prep data: Combine previous state with current chunk samples
        if col_name in self._last_values:
            prev_ts, prev_val = self._last_values[col_name]
            s_ts = np.concatenate(
                (np.array([prev_ts]), np.array(samples["timestamp_ns"].values))
            )
            s_val = np.concatenate(
                (np.array([prev_val]), np.array(samples.iloc[:, 1].values))
            )
        else:
            s_ts = np.array(samples["timestamp_ns"].values)
            s_val = np.array(samples.iloc[:, 1].values)

        if len(s_ts) == 0:
            return np.full(len(ticks), None)

        # Update global state for next chunk before applying local policy
        self._last_values[col_name] = (s_ts[-1], s_val[-1])

        # Policy Dispatcher
        return self._dispatch_policy(ticks, s_ts, s_val)

    # --- Policy Implementations ---

    def _dispatch_policy(
        self, ticks: np.ndarray, s_ts: np.ndarray, s_val: np.ndarray
    ) -> np.ndarray:
        if self.policy == SynchPolicy.Hold:
            return self._sync_hold(ticks, s_ts, s_val)
        elif self.policy == SynchPolicy.AsOf:
            return self._sync_as_of(ticks, s_ts, s_val)
        elif self.policy == SynchPolicy.Drop:
            return self._sync_drop(ticks, s_ts, s_val)
        else:
            logger.error(f"Unknown policy '{self.policy}'. Falling back to None.")
            return np.full(len(ticks), None)

    def _sync_hold(
        self, ticks: np.ndarray, s_ts: np.ndarray, s_val: np.ndarray
    ) -> np.ndarray:
        """
        Classic Last-Value-Hold.

        Logic:
        For each grid tick 't', it finds the most recent sample 's' where s_timestamp <= t.

        Gap Handling:
        - Initial Gaps: If t < first_sample_timestamp, the result for that tick is 'None'.
        - Mid-Stream Gaps: If a sensor stops reporting, the last known value is
          carried forward indefinitely.
        - Chunk Continuity: If the last sample of Chunk N is at time T, and Chunk N+1
          starts at T+delta, the value at T is used to fill the start of Chunk N+1
          until a new sample arrives.
        """
        indices = np.searchsorted(s_ts, ticks, side="right") - 1
        results = np.full(len(ticks), None, dtype=object)

        mask = indices >= 0
        results[mask] = s_val[indices[mask]]
        return results

    def _sync_as_of(
        self, ticks: np.ndarray, s_ts: np.ndarray, s_val: np.ndarray
    ) -> np.ndarray:
        """
        As-of (Tolerance-based Hold).

        Logic:
        Similar to 'hold', but only yields a value if (tick_timestamp - sample_timestamp) <= tolerance_ns.

        Gap Handling:
        - If the gap between the last sample and the current tick exceeds tolerance_ns,
          the transformer yields 'None'. This is ideal for sensors that are
          considered "stale" after a certain period (e.g., high-speed localization).
        """
        if self.tolerance_ns is None:
            raise ValueError("tolerance_ns must be set for 'as_of' policy.")

        indices = np.searchsorted(s_ts, ticks, side="right") - 1
        results = np.full(len(ticks), None, dtype=object)

        mask = indices >= 0
        if any(mask):
            valid_indices = indices[mask]
            deltas = ticks[mask] - s_ts[valid_indices]
            tol_mask = deltas <= self.tolerance_ns

            # Apply only to those within tolerance
            final_mask = np.zeros(len(ticks), dtype=bool)
            final_mask[mask] = tol_mask
            results[final_mask] = s_val[indices[final_mask]]

        return results

    def _sync_drop(
        self, ticks: np.ndarray, s_ts: np.ndarray, s_val: np.ndarray
    ) -> np.ndarray:
        """
        Drop (Interval-based Hold).

        Logic:
        Only returns a value if a sample arrived within the specific interval (t - step_ns, t].

        Gap Handling:
        - This policy is highly restrictive. If a sensor skips even a single expected
          period, the resulting grid tick will be 'None'. This effectively
          "drops" the signal until the next hardware event occurs.
        """
        indices = np.searchsorted(s_ts, ticks, side="right") - 1
        results = np.full(len(ticks), None, dtype=object)

        mask = indices >= 0
        if any(mask):
            valid_indices = indices[mask]
            deltas = ticks[mask] - s_ts[valid_indices]
            # Use step_ns as the implicit tolerance for windowed drops
            drop_mask = deltas < self.step_ns

            final_mask = np.zeros(len(ticks), dtype=bool)
            final_mask[mask] = drop_mask
            results[final_mask] = s_val[indices[final_mask]]

        return results
