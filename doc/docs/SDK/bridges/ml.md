---
title: Machine Learning & Analytics
description: Streamlining Data for Physical AI
---

The **Mosaico ML** module serves as the high-performance bridge between the Mosaico Data Platform and the modern Data Science ecosystem. While the platform is optimized for high-speed raw message streaming, this module provides the abstractions necessary to transform asynchronous sensor data into tabular formats compatible with **Physical AI**, **Deep Learning**, and **Predictive Analytics**.

Working with robotics and multi-modal datasets presents three primary technical hurdles that the ML module is designed to solve:

* **Heterogeneous Sampling**: Sensors like LIDAR (low frequency), IMU (high frequency), and GPS (intermittent) operate at different rates.
* **High Volume**: Datasets often exceed the available system RAM.
* **Nested Structures**: Robotics data is typically deeply nested with coordinate transformations and covariance matrices.


## From Sequences to DataFrames
??? question "API Reference"
    [`mosaicolabs.ml.DataFrameExtractor`][mosaicolabs.ml.DataFrameExtractor]

The [`DataFrameExtractor`][mosaicolabs.ml.DataFrameExtractor] is a specialized utility designed to convert Mosaico sequences into tabular formats. Unlike standard streamers that instantiate individual Python objects, this extractor operates at the **Batch Level** by pulling raw `RecordBatch` objects directly from the underlying stream to maximize throughput.

### Key Technical Features

* **Recursive Flattening**: Automatically "unpacks" deeply nested Mosaico Ontology structures into primitive columns.
* **Semantic Naming**: Columns use a `{topic_name}.{ontology_tag}.{field_path}` convention (e.g., `/front/camera/imu.imu.acceleration.x`) to remain self-describing.
* **Namespace Isolation**: Topic names are included in column headers to prevent collisions when multiple sensors of the same type are present.
* **Memory-Efficient Windowing**: Uses a generator-based approach to yield data in time-based "chunks" (e.g., 5-second windows) while handling straddling batches via a carry-over buffer.
* **Sparse Merging**: Creates a "sparse" DataFrame containing the union of all timestamps, using `NaN` for missing sensor readings at specific intervals.



This example demonstrates iterating through a sequence in 10-second tabular chunks.

```python
from mosaicolabs import MosaicoClient
from mosaicolabs.ml import DataFrameExtractor

with MosaicoClient.connect("localhost", 6726):
    # Initialize from an existing SequenceHandler
    seq_handler = client.sequence_handler("drive_session_01")
    extractor = DataFrameExtractor(seq_handler)

    # Iterate through 10-second chunks
    for df in extractor.to_pandas_chunks(window_sec=10.0):
        # 'df' is a pandas DataFrame with semantic columns
        # Example: df["/front/camera/imu.imu.acceleration.x"]
        print(f"Processing chunk with {len(df)} rows")

```

For complex types like images that require specialized decoding, Mosaico allows you to "inflate" a flattened DataFrame row back into a strongly-typed `Message` object.

```python
from mosaicolabs import MosaicoClient
from mosaicolabs.ml import DataFrameExtractor
from mosaicolabs.models import Message, Image

with MosaicoClient.connect("localhost", 6726):
    # Initialize from an existing SequenceHandler
    seq_handler = client.sequence_handler("drive_session_01")
    extractor = DataFrameExtractor(seq_handler)

    # Get data chunks
    for df in extractor.to_pandas_chunks(topics=["/sensors/front/image_raw"]):
        for _, row in df.iterrows():
            # Reconstruct the full Message (envelope + payload) from a row
            img_msg = Message.from_dataframe_row(
                row=row,
                topic_name="/sensors/front/image_raw",
            )
        
            if img_msg:
                img = img_msg.get_data(Image).to_pillow()
                # Access typed fields with IDE autocompletion
                print(f"Time: {img_msg.timestamp_ns}")
                img.show()

```

## Sparse to Dense Representation
??? question "API Reference"
    [`mosaicolabs.ml.SyncTransformer`][mosaicolabs.ml.SyncTransformer]

The [`SyncTransformer`][mosaicolabs.ml.SyncTransformer] is a temporal resampler designed to solve the **Heterogeneous Sampling** problem inherent in robotics and Physical AI. 
It aligns multi-rate sensor streams (for example, an IMU at 100Hz and a GPS at 5Hz) onto a uniform, fixed-frequency grid to prepare them for machine learning models.
The `SyncTransformer` operates as a processor that bridges the gaps between windowed chunks yielded by the [`DataFrameExtractor`][mosaicolabs.ml.DataFrameExtractor].
Unlike standard resamplers that treat each data batch in isolation, this transformer maintains internal state to ensure signal continuity across batch boundaries.

### Key Design Principles

* **Stateful Continuity**: It maintains an internal cache of the last known sensor values and the next expected grid tick, allowing signals to bridge the gap between independent DataFrame chunks.
* **Semantic Integrity**: It respects the physical reality of data acquisition by yielding `None` for grid ticks that occur before a sensor's first physical measurement, avoiding data "hallucination".
* **Vectorized Performance**: Internal kernels leverage high-speed lookups for high-throughput processing.
* **Protocol-Based Extensibility**: The mathematical logic for resampling is decoupled through a [`SyncPolicy`][mosaicolabs.ml.SyncPolicy] protocol, allowing for custom kernel injection.

### Implementation and Stateful Lifecycle

Architecturally, the [`SyncTransformer`][mosaicolabs.ml.SyncTransformer] is implemented as a **stateful state-machine** designed to maintain signal continuity across independent data chunks. Unlike standard library resamplers that often treat each input batch as an isolated event, this transformer is engineered to "stitch" the temporal gaps between the windowed DataFrames yielded by the [`DataFrameExtractor`][mosaicolabs.ml.DataFrameExtractor].

#### Internal State Management
The transformer maintains two critical internal buffers that persist for its entire lifecycle:

* **`_next_timestamp_ns`**: A scalar value tracking the exact nanosecond where the next grid tick must occur. This prevents temporal drift across hours of recording by ensuring the fixed-frequency grid remains globally aligned.
* **`_last_values`**: A dictionary that caches the final (timestamp, value) tuple for every topic processed in the previous chunk.


#### The Transformation Pipeline
When [`transform()`][mosaicolabs.ml.SyncTransformer.transform] is called on a new `sparse_chunk`, the following internal operations occur:

1.  **Grid Synthesis**: The transformer generates a new time grid starting precisely from `_next_timestamp_ns` and extending to the end of the current chunk.
2.  **Data Prepending**: Through the `_prepare_data` method, the transformer retrieves the last known value of each topic and **prepends** it to the current chunk's arrays. This ensures that the resampling algorithm (e.g., [`SyncHold`][mosaicolabs.ml.SyncHold]) has a reference point to fill the gap at the very beginning of the new window.
3.  **Policy Delegation**: The actual mathematical mapping is delegated to the configured [`SyncPolicy`][mosaicolabs.ml.SyncPolicy], which yields the final dense values for the synthesized grid.


!!! danger "The 'Re-instantiation' Trap"
    A common architectural error is re-instantiating the `SyncTransformer` inside a processing loop. Because the transformer is stateful, **it must be instantiated once outside the loop** to function correctly.

    Re-instantiating the transformer inside the loop destroys the internal cache, causing signal discontinuities and potential data "hallucination" at the start of every chunk.

    ```python
    # ERROR: Transformer is created inside the loop
    for sparse_chunk in extractor.to_pandas_chunks():
        # This resets the grid and last_values every iteration!
        transformer = SyncTransformer(target_fps=50) 
        dense_chunk = transformer.transform(sparse_chunk)
    ```

    #### Correct Pattern (Persistent State)
    By keeping the instance outside the loop, the transformer correctly bridges the gap between `sparse_chunk_N` and `sparse_chunk_N+1`.

    ```python
    # CORRECT: Instantiate once
    transformer = SyncTransformer(target_fps=50)

    for sparse_chunk in extractor.to_pandas_chunks():
        # transform() updates internal state and prepares it for the next call
        dense_chunk = transformer.transform(sparse_chunk)
    ```

    If you need to reuse the same transformer instance for a different recording or sequence, you must call the [`.reset()`][mosaicolabs.ml.SyncTransformer.reset] method to clear the internal buffers and prepare for a new grid alignment.

### Implemented Synchronization Policies
??? question "API Reference"
    [`mosaicolabs.ml.SyncPolicy`][mosaicolabs.ml.SyncPolicy]

Each policy defines a specific logic for how the transformer bridges temporal gaps between sparse data points.

#### 1. **[`SyncHold`][mosaicolabs.ml.SyncHold]** (Last-Value-Hold)

* **Behavior**: Finds the most recent valid measurement and "holds" it constant until a new one arrives.
* **Best For**: Sensors where states remain valid until explicitly changed, such as robot joint positions or battery levels.

#### 2. **[`SyncAsOf`][mosaicolabs.ml.SyncAsOf]** (Staleness Guard)

* **Behavior**: Carries the last known value forward only if it has not exceeded a defined maximum "tolerance" (fresher than a specific age).
* **Best For**: High-speed signals that become unreliable if not updated frequently, such as localization coordinates.

#### 3. **[`SyncDrop`][mosaicolabs.ml.SyncDrop]** (Interval Filter)

* **Behavior**: Ensures a grid tick only receives a value if a new measurement actually occurred within that specific grid interval; otherwise, it returns `None`.
* **Best For**: Downsampling high-frequency data where a strict 1-to-1 relationship between windows and unique hardware events is required.

### Memory & Performance Best Practices

The memory footprint of your ML pipeline is primarily governed by the product of two variables: the temporal window size (`window_sec`) in [`DataFrameExtractor.to_pandas_chunks()`][mosaicolabs.ml.DataFrameExtractor.to_pandas_chunks] and the synthesis frequency (`target_fps`) in [`SyncTransformer.transform()`][mosaicolabs.ml.SyncTransformer.transform].

#### Resource Consumption Matrix

| Parameter Configuration | Memory Impact | CPU Impact | Use Case |
| :--- | :--- | :--- | :--- |
| **Small Window / Low FPS**<br>(1s / 10Hz) | **Minimal**: Lowest RAM overhead per chunk. | Low | Real-time monitoring, low-power edge devices. |
| **Large Window / Low FPS**<br>(60s / 10Hz) | **Moderate**: High memory usage in the `DataFrameExtractor` due to large sparse buffers. | Moderate | Batch analysis where global context is needed before transformation. |
| **Small Window / High FPS**<br>(1s / 1000Hz) | **High**: Rapid creation of many dense rows. Heavy overhead on Python's Garbage Collector. | High | High-fidelity signal processing (e.g., vibration analysis). |
| **Large Window / High FPS**<br>(60s / 1000Hz) | **Critical**: Risk of `MemoryError`. Can generate millions of rows in a single `transform()` call. | Very High | Deep Learning training on high-end workstations with 64GB+ RAM. |

#### Optimization Strategies

1.  **Avoid the "Full Load" Trap**:
    The `DataFrameExtractor` is designed for (batched) streaming. If you set `window_sec` to a value greater than the total sequence duration, the extractor will fall back to a "Full Load" mode, attempting to load the entire recording into RAM, which can mean multi-GB batch for heavy datasets.

2.  **The Rule of 100k Rows**:
    As a general architectural guideline, aim for a configuration where `window_sec * target_fps < 100,000` rows per chunk (for time-series). This keeps the Pandas operations within the L3 cache limits of most modern CPUs and ensures the garbage collector can keep up with the loop iterations.

3.  **Downsampling before Transformation**:
    If you have high-frequency raw data (e.g., IMU at 400Hz) but only need 50Hz for your ML model, use the `SyncDrop` policy. It is significantly faster than `SyncHold` because it discards redundant intermediate samples before the dense grid is synthesized, reducing the internal array sizes during the `_prepare_data` phase.

4.  **Garbage Collection in Long Loops**:
    When processing sequences that span several hours, Python's automatic reference counting may not trigger fast enough. If you observe a "memory leak" (steadily increasing RAM usage), explicitly delete the `dense_chunk` at the end of your loop:

    ```python
    for sparse_chunk in extractor.to_pandas_chunks():
        dense_chunk = transformer.transform(sparse_chunk)
        # ... process ...
        del dense_chunk # Explicitly signal for GC
    ```

### Scikit-Learn Compatibility

By implementing the standard `fit`/`transform` interface, the [`SyncTransformer`][mosaicolabs.ml.SyncTransformer] makes robotics data a "first-class citizen" of the [Scikit-learn](https://scikit-learn.org/stable/) ecosystem. This allows for the plug-and-play integration of multi-rate sensor data into standard [pipelines](https://scikit-learn.org/stable/api/sklearn.pipeline.html).

```python
from sklearn.pipeline import Pipeline
from sklearn.preprocessing import StandardScaler
from mosaicolabs import MosaicoClient
from mosaicolabs.ml import DataFrameExtractor, SyncTransformer, SynchHold


# Define a pipeline for physical AI preprocessing
pipeline = Pipeline([
    ('sync', SyncTransformer(target_fps=30.0, policy=SynchHold())),
    ('scaler', StandardScaler())
])

with MosaicoClient.connect("localhost", 6726):
    # Initialize from an existing SequenceHandler
    seq_handler = client.sequence_handler("drive_session_01")
    extractor = DataFrameExtractor(seq_handler)

    # Process sequential chunks while maintaining signal continuity
    for sparse_chunk in extractor.to_pandas_chunks(window_sec=5.0):
        # The transformer automatically carries state across sequential calls
        normalized_dense_chunk = pipeline.transform(sparse_chunk)

```
