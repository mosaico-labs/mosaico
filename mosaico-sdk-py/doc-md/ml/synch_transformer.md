# SyncTransformer

The `SyncTransformer` is a stateful temporal resampler designed to solve the **Heterogeneous Sampling** problem in robotics and Physical AI. It aligns multi-rate sensor streams (e.g., IMU at 100Hz and GPS at 5Hz) onto a uniform, fixed-frequency grid.

## Architecture & Philosophy

The `SyncTransformer` operates as a stateful processor that bridges the gaps between the windowed chunks yielded by the [`DataFrameExtractor`](https://www.google.com/search?q=./dataframe_extractor.md). Unlike standard resamplers that treat each data batch in isolation, this transformer maintains internal state to ensure signal continuity across batch boundaries.

### Key Design Principles

* **Stateful Continuity:** It maintains an internal cache of the last known sensor values and the next expected grid tick, allowing signals to bridge the gap between independent DataFrame chunks.
* **Semantic Integrity:** It respects the physical reality of data acquisition by yielding `None` for grid ticks that occur before a sensor's first physical measurement, avoiding data "hallucination".
* **Vectorized Performance:** Internal kernels leverage `numpy.searchsorted` for  lookup speeds, enabling high-throughput processing.
* **Protocol-Based Extensibility:** The mathematical logic for resampling is decoupled through a `SynchPolicy` protocol, allowing for custom kernel injection.


## Class Reference: `SyncTransformer`

### `__init__`

Initializes the transformer with a specific target frequency and synchronization strategy.

* **`target_fps`** (`float`): The desired output frequency in Hz (e.g., `30.0`).
* **`policy`** (`SynchPolicy`): A strategy object (default: `SynchHold()`).
* **`timestamp_column`** (`str`): The name of the column containing nanosecond timestamps (default: `"timestamp_ns"`).

### `fit(X, y=None)`

Captures the initial timestamp from the first chunk to align the grid and initializes the state.

* **Note**: If `timestamp_column` is missing from `X`, a `ValueError` is raised.

### `transform(X)`

Executes the temporal resampling logic for a single DataFrame chunk.

* **Input**: A sparse `pd.DataFrame` from the extractor.
* **Output**: A dense `pd.DataFrame` where every row matches a fixed grid tick.

### `fit_transform(X, y=None)`

Chains the `fit` and `transform` operations.

### `reset()`

Clears the internal temporal state and cached sensor values, effectively starting a new session.


## Implemented Synchronization Policies

The transformer (currently) provides discrete "Hold" policies defined in `ml/synch_policies/hold.py`. Each synchronization policy defines a specific logic for how the transformer bridges temporal gaps between sparse data points. These strategies determine how the system fills the uniform grid while maintaining the physical integrity of the sensor data.

#### **1. `SynchHold` (Last-Value-Hold)**

This is the most permissive policy. It finds the most recent valid measurement and "holds" that value constant until a new one arrives. If a grid tick occurs between two measurements, it simply repeats the previous value.

* **Behavior:** It carries data forward indefinitely across the timeline.
* **Best For:** Sensors where the state remains valid until explicitly changed, such as robot joint positions, battery levels, or operational status flags.

#### **2. `SynchAsOf` (Staleness Guard)**

This policy acts like a "Hold" with an expiration date. It will carry the last known value forward only if that value isn't too old. You define a maximum "tolerance" (in nanoseconds); if the gap between the last measurement and the current grid tick exceeds this limit, the transformer returns `None` instead of a stale value.

* **Behavior:** It keeps the signal alive only as long as the data is considered "fresh".
* **Best For:** High-speed signals that become unreliable or dangerous if they aren't updated frequently, such as localization coordinates or velocity estimates in dynamic environments.

#### **3. `SynchDrop` (Interval Filter)**

This is the strictest policy. It ensures that a grid tick only receives a value if a new measurement actually occurred within that specific grid interval `(t - delta_t, t]`. If no new data arrived since the last tick, the current tick is left empty (`None`).

* **Behavior:** It refuses to propagate data from the past, ensuring every grid point represents a unique hardware event from the current time window.
* **Best For:** Downsampling high-frequency data where you need a strict 1-to-1 relationship between time windows and sensor events, without any data repetition.

## Scikit-Learn Compatibility & Physical AI Readiness

The `SyncTransformer` serves as a bridge that enables robotics datasets to be utilized in a standardized, ML-ready format. For example, by implementing the standard `fit`/`transform` interface, Mosaico transforms complex, multi-rate sensor streams into "first-class citizens" of the Scikit-learn ecosystem. This compatibility allows for the plug&play integration of robotics data into classical ML algorithmic pipelines, such as `Pipeline` and `FeatureUnion`. It effectively abstracts the "data plumbing" of Physical AI—automating temporal normalization and gap handling—allowing researchers to move directly from raw sensor logs to optimized data structures for training and inference.

### Example: Basic Usage

```python
from mosaicolabs.ml import DataFrameExtractor, SyncTransformer
from mosaicolabs.ml.synch_policies import SynchHold

# Extract sparse data
extractor = DataFrameExtractor(seq_handler)
sparse_df = next(extractor.to_pandas_chunks(window_sec=5.0))

# Initialize and apply the transformer (Target 10Hz)
# The transformer will align all columns to a 100ms grid
transformer = SyncTransformer(target_fps=10.0, policy=SynchHold())
dense_df = transformer.transform(sparse_df)

```

### Example: Scikit-Learn Pipeline

Because it is stateful, the `SyncTransformer` is ideal for pipelines where data is processed in sequential chunks.

```python
from sklearn.pipeline import Pipeline
from sklearn.preprocessing import StandardScaler

# Define a pipeline for physical AI preprocessing
pipeline = Pipeline([
    ('sync', SyncTransformer(target_fps=30.0, policy=SynchHold())),
    ('scaler', StandardScaler())
])

# Process sequential chunks while maintaining signal continuity
for sparse_chunk in extractor.to_pandas_chunks(window_sec=5.0):
    # The transformer carries state (last values) automatically across calls
    normalized_dense_chunk = pipeline.transform(sparse_chunk)
    # Do something...

```
