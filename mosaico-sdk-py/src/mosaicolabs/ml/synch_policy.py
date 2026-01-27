import typing
import numpy as np


@typing.runtime_checkable
class SynchPolicy(typing.Protocol):
    """
    Structural protocol defining the interface for synchronization kernels.

    Any class implementing a `apply` method with this signature can be used
    by the SyncTransformer to resample sparse sensor data onto a dense grid.
    """

    def apply(
        self, grid: np.ndarray, s_ts: np.ndarray, s_val: np.ndarray
    ) -> np.ndarray:
        """
        Apply the synchronization logic to a set of sparse samples.

        Args:
            grid: A 1D array of target nanosecond timestamps (the dense grid).
            s_ts: A 1D array of acquisition nanosecond timestamps (the sparse samples).
            s_val: A 1D array of sensor values corresponding to s_ts.

        Returns:
            np.ndarray: An object-type array of the same length as `grid`,
                        where missing data is represented by None.
        """
        ...
