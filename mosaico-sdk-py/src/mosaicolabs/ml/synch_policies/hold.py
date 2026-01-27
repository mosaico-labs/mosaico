import numpy as np


class SynchHold:
    """
    Classic Last-Value-Hold (Zero-Order Hold) synchronization.

    This policy carries the most recent valid sample forward to all future
    grid ticks until a new sample is received.
    """

    def apply(
        self, grid: np.ndarray, s_ts: np.ndarray, s_val: np.ndarray
    ) -> np.ndarray:
        # Find the index of the latest sample s_ts <= tick.
        # side="right" - 1 ensures we pick the sample exactly at or just before the tick.
        indices = np.searchsorted(s_ts, grid, side="right") - 1
        results = np.full(len(grid), None, dtype=object)

        # Map values where a preceding sample exists (index >= 0)
        mask = indices >= 0
        results[mask] = s_val[indices[mask]]
        return results


class SynchAsOf:
    """
    Tolerance-based 'As-Of' synchronization.

    Similar to SynchHold, but invalidates the 'held' value if the time gap
    between the sample and the grid tick exceeds a specific threshold.
    """

    def __init__(self, tolerance_ns: int) -> None:
        """
        Args:
            tolerance_ns: Maximum allowed nanoseconds between a sample and a grid tick.
        """
        self._tolerance_ns = tolerance_ns

    def apply(
        self, grid: np.ndarray, s_ts: np.ndarray, s_val: np.ndarray
    ) -> np.ndarray:
        indices = np.searchsorted(s_ts, grid, side="right") - 1
        results = np.full(len(grid), None, dtype=object)

        mask = indices >= 0
        if any(mask):
            valid_indices = indices[mask]
            # Calculate the 'staleness' of the held value
            deltas = grid[mask] - s_ts[valid_indices]
            tol_mask = deltas <= self._tolerance_ns

            # Re-mask the results: only keep values within the tolerance window
            final_mask = np.zeros(len(grid), dtype=bool)
            final_mask[mask] = tol_mask
            results[final_mask] = s_val[indices[final_mask]]

        return results


class SynchDrop:
    """
    Strict Interval-based 'Drop' synchronization.

    Only yields a value if a sample was acquired within the specific
    temporal window of the current grid tick (t - step_ns, t].
    """

    def __init__(self, step_ns: int) -> None:
        """
        Args:
            step_ns: The duration of the grid interval in nanoseconds.
        """
        self._step_ns = step_ns

    def apply(
        self, grid: np.ndarray, s_ts: np.ndarray, s_val: np.ndarray
    ) -> np.ndarray:
        indices = np.searchsorted(s_ts, grid, side="right") - 1
        results = np.full(len(grid), None, dtype=object)

        mask = indices >= 0
        if any(mask):
            valid_indices = indices[mask]
            deltas = grid[mask] - s_ts[valid_indices]
            # A value is kept only if it arrived 'inside' the current step window
            drop_mask = deltas < self._step_ns

            final_mask = np.zeros(len(grid), dtype=bool)
            final_mask[mask] = drop_mask
            results[final_mask] = s_val[indices[final_mask]]

        return results
