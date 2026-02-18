import typing
import numpy as np


@typing.runtime_checkable
class SyncPolicy(typing.Protocol):
    """
    Protocol defining the interface for data synchronization policies.

    A `SyncPolicy` determines how sparse data samples are mapped onto a standard, dense time grid.
    Classes implementing this protocol are used by `SyncTransformer` to resample sensor data
    (e.g., holding the last value, interpolating, or dropping old data).

    Common implementations include:

    * `SyncHold`: Zero-order hold (carries the last value forward).
    * `SyncAsOf`: Tolerance-based hold (carries value forward only for a specific duration).
    * `SyncDrop`: Strict interval matching (drops data outside the current grid step).
    """

    def apply(
        self, grid: np.ndarray, s_ts: np.ndarray, s_val: np.ndarray
    ) -> np.ndarray:
        """
        Applies the synchronization logic to sparse samples, mapping them to the target grid.

        Args:
            grid (np.ndarray): The target dense timeline (nanosecond timestamps).
            s_ts (np.ndarray): The source acquisition timestamps (sparse data).
            s_val (np.ndarray): The source sensor values corresponding to `s_ts`.

        Returns:
            np.ndarray: An object-array of the same length as `grid`, containing the
                synchronized values. Slots with no valid data are filled with `None`.
        """
        ...
