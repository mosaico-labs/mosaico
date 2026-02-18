import pandas as pd
import numpy as np
from typing import Any, Dict, Optional, Tuple
from mosaicolabs.logging_config import get_logger

from .sync_policy import SyncPolicy
from .sync_policies.hold import SyncHold

logger = get_logger(__name__)


class SyncTransformer:
    """
    Stateful resampler for Mosaico DataFrames.

    This class aligns heterogeneous sensor streams onto a uniform time grid.
    It is designed to consume the windowed outputs of the `DataFrameExtractor`
    sequentially, maintaining internal state to ensure signal continuity across
    batch boundaries.

    ### Scikit-Learn Compatibility
    The class implements the standard `fit`/`transform` interface, making it
    fully compliant with Scikit-learn `Pipeline` and `FeatureUnion` objects.

    * **fit(X)**: Captures the initial timestamp from the first chunk to align
      the grid.
    * **transform(X)**: Executes the temporal resampling logic for a single
      DataFrame chunk and returns a dense DataFrame.
    * **fit_transform(X)**: Fits the transformer to the data and then transforms it.

    Key Features:

    * Fixed Frequency: Normalizes multi-rate sensors to a target FPS.
    * Stateful Persistence: Carries the last known sensor state into the next chunk.
    * Semantic Integrity: Correctly handles 'Late Arrivals' by yielding None for
      ticks preceding the first physical measurement.
    """

    def __init__(
        self,
        target_fps: float,
        policy: SyncPolicy = SyncHold(),
        timestamp_column="timestamp_ns",
    ):
        """
        Args:
            target_fps (float): The desired output frequency in Hz.
            policy (SyncPolicy): A strategy implementing the `SyncPolicy` protocol.
            timestamp_column (str): The column name containing the timestamp data.
        """
        self._step_ns: int = int(1e9 / target_fps)
        self._policy: SyncPolicy = policy
        self._timestamp_column: str = timestamp_column

        # Internal state to bridge gaps between chunks
        self._last_values: Dict[str, Tuple[int, Any]] = {}
        self._next_timestamp_ns: Optional[int] = None

    def fit(self, X: pd.DataFrame, y=None) -> "SyncTransformer":
        """
        Initializes the grid alignment based on the first observed timestamp.
        """
        if not X.empty and self._next_timestamp_ns is None:
            self._next_timestamp_ns = int(X[self._timestamp_column].iloc[0])

        if self._timestamp_column not in X.columns:
            raise ValueError(
                f"Unable to find time column '{self._timestamp_column}' in dataframe."
            )
        return self

    def transform(self, X: pd.DataFrame) -> pd.DataFrame:
        """
        Syncronizes a sparse DataFrame chunk into a dense, uniform DataFrame.

        Example: **Example with generic dataframe**
            ```python
            from mosaicolabs.ml import SyncTransformer, SyncHold

            # 5 Hz Target (200ms steps)
            transformer = SyncTransformer(target_fps=5, policy=SyncHold())

            # Define a sparse dataframe with two sensors:
            # `sensor_a` starts at 0, `sensor_b` arrives at 600ms
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

            # Expected output
            # "timestamp_ns",   "sensor_a", "sensor_b"
            # 0,                10.0,       None        # <- avoid hallucination on `sensor_b`
            # 200_000_000,      10.0,       None        # <- avoid hallucination on `sensor_b`
            # 400_000_000,      10.0,       None        # <- avoid hallucination on `sensor_b`
            # 600_000_000,      11.0,       1.0
            # 800_000_000,      11.0,       2.0
            # 1_000_000_000,    11.0,       2.0
            # 1_200_000_000,    12.0,       2.0
            # 1_400_000_000,    12.0,       2.0
            ```

        Example: **Example with Mosaico dataframe**
            ```python
            # Obtain a dataframe with DataFrameExtractor
            from mosaicolabs import MosaicoClient, IMU, Image
            from mosaicolabs.ml import DataFrameExtractor, SyncTransformer

            with MosaicoClient.connect("localhost", 6726) as client:
                sequence_handler = client.get_sequence_handler("example_sequence")
                for df in DataFrameExtractor(sequence_handler).to_pandas_chunks(
                    topics = ["/front/imu", "/front/camera/image_raw"]
                ):
                    # Synch the data at 30 Hz:
                    sync_transformer = SyncTransformer(
                        target_fps = 30, # resample at 30 Hz and fill the Nans with a `Hold` policy
                    )
                    synced_df = sync_transformer.transform(df)
                    # Do something with the synced dataframe
                    # ...
            ```
        """
        if self._timestamp_column not in X.columns:
            raise ValueError(
                f"Unable to find time column '{self._timestamp_column}' in dataframe."
            )
        tstamp_grid = self._generate_grid(X=X)
        if len(tstamp_grid) == 0:
            return pd.DataFrame()

        dense_df = pd.DataFrame({self._timestamp_column: tstamp_grid})

        # Process each topic column independently
        for col in [c for c in X.columns if c != self._timestamp_column]:
            # Standard sparse extraction: drop NaNs to get real measurement events
            samples = X[[self._timestamp_column, col]].dropna()

            # Merge previous chunk's final state with current data
            s_ts, s_val = self._prepare_data(col, samples)

            if len(s_ts) == 0:
                dense_df[col] = np.full(len(tstamp_grid), None)
                continue

            # Cache the final state of this chunk for the next iteration
            self._last_values[col] = (int(s_ts[-1]), s_val[-1])

            # Delegate the mathematical resampling to the selected policy
            dense_df[col] = self._policy.apply(tstamp_grid, s_ts, s_val)

        return dense_df

    def fit_transform(self, X: pd.DataFrame, y=None) -> pd.DataFrame:
        """
        Fits the transformer to the data and then transforms it.
        """
        return self.fit(X, y).transform(X)

    def reset(self):
        """Resets the internal temporal state and cached sensor values."""
        self._next_timestamp_ns = None
        self._last_values = {}

    def _generate_grid(self, X: pd.DataFrame) -> np.ndarray:
        """Calculates the target nanosecond grid for the current chunk."""
        if X.empty:
            return np.array([], dtype=np.int64)

        X_tstamp = X[self._timestamp_column]
        if self._next_timestamp_ns is None:
            self._next_timestamp_ns = int(X_tstamp.iloc[0])

        chunk_end_ns = int(X_tstamp.iloc[-1])

        # We start exactly from the next expected tick to prevent drift
        grid = np.arange(
            self._next_timestamp_ns, chunk_end_ns + 1, self._step_ns, dtype=np.int64
        )

        # Advance the pointer for the next chunk
        if len(grid) > 0:
            self._next_timestamp_ns = int(grid[-1] + self._step_ns)

        return grid

    def _prepare_data(
        self, col_name: str, samples: pd.DataFrame
    ) -> Tuple[np.ndarray, np.ndarray]:
        """
        Prepends the carried-over sample from the previous chunk to ensure continuity.
        """
        current_ts = samples[self._timestamp_column].values
        current_val = samples.iloc[:, 1].values

        if col_name in self._last_values:
            last_ts, last_val = self._last_values[col_name]
            # Prepend stateful data to the start of current arrays
            s_ts = np.concatenate(([last_ts], np.array(current_ts)))
            s_val = np.concatenate(([last_val], np.array(current_val)))
        else:
            s_ts = np.array(current_ts)
            s_val = np.array(current_val)

        return s_ts, s_val
