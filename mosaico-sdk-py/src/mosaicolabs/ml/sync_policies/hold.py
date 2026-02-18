import numpy as np


class SyncHold:
    """
    Classic Last-Value-Hold (Zero-Order Hold) synchronization.

    This policy carries the most recent valid sample forward to all future
    grid ticks until a new sample is received. It effectively creates a "step"
    function from the sparse samples.
    """

    def apply(
        self, grid: np.ndarray, s_ts: np.ndarray, s_val: np.ndarray
    ) -> np.ndarray:
        """
        Applies the Zero-Order Hold logic.

        Args:
            grid (np.ndarray): The target dense timeline (nanosecond timestamps).
            s_ts (np.ndarray): The source acquisition timestamps.
            s_val (np.ndarray): The source sensor values.

        Returns:
            np.ndarray: Densely populated array where each point holds the last known value.
        """
        # Find the index of the latest sample s_ts <= tick.
        # side="right" - 1 ensures we pick the sample exactly at or just before the tick.
        indices = np.searchsorted(s_ts, grid, side="right") - 1
        results = np.full(len(grid), None, dtype=object)

        # Map values where a preceding sample exists (index >= 0)
        mask = indices >= 0
        results[mask] = s_val[indices[mask]]
        return results


class SyncAsOf:
    """
    Tolerance-based 'As-Of' synchronization.

    Similar to `SyncHold`, but limits how far a value can be carried forward.
    If the time difference between the grid tick and the last sample exceeds
    `tolerance_ns`, the value is considered stale and the slot is left as `None`.
    """

    def __init__(self, tolerance_ns: int) -> None:
        """
        Args:
            tolerance_ns (int): Maximum allowed age (in nanoseconds) for a sample to be valid.
        """
        self._tolerance_ns = tolerance_ns

    def apply(
        self, grid: np.ndarray, s_ts: np.ndarray, s_val: np.ndarray
    ) -> np.ndarray:
        """
        Applies the As-Of synchronization logic with tolerance check.

        Args:
            grid (np.ndarray): The target dense timeline.
            s_ts (np.ndarray): The source acquisition timestamps.
            s_val (np.ndarray): The source sensor values.

        Returns:
            np.ndarray: Densely populated array, with `None` where data is missing or stale.
        """
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


class SyncDrop:
    """
    Strict Interval-based 'Drop' synchronization.

    Only yields a value if a sample was acquired strictly within the
    current grid interval `(t_grid - step_ns, t_grid]`. If no sample falls
    in this window, the result is `None`. This is useful for event-based matching.
    """

    def __init__(self, step_ns: int) -> None:
        """
        Args:
            step_ns (int): The duration of the backward-looking window in nanoseconds.
        """
        self._step_ns = step_ns

    def apply(
        self, grid: np.ndarray, s_ts: np.ndarray, s_val: np.ndarray
    ) -> np.ndarray:
        """
        Applies the Drop synchronization logic.

        Args:
            grid (np.ndarray): The target dense timeline.
            s_ts (np.ndarray): The source acquisition timestamps.
            s_val (np.ndarray): The source sensor values.

        Returns:
            np.ndarray: Array containing values only for populated intervals, otherwise `None`.
        """
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
