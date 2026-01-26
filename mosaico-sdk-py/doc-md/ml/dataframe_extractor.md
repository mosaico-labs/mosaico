# DataFrameExtractor

The `DataFrameExtractor` is a high-performance utility designed to convert Mosaico sequences into tabular formats. It acts as a specialized bridge between the Mosaico data stream and the ML community, optimized for **columnar data structures** and semantic integrity.

## Architecture & Design Principles

Unlike the standard `SequenceDataStreamer`, which yields one `Message` at a time, the `DataFrameExtractor` operates at the **Batch Level**. It pulls raw `RecordBatch` objects directly from the underlying stream, significantly reducing the overhead of Python object instantiation.

### Key Technical Features

1. **Recursive Flattening:** Mosaico's Ontology can be deeply nested. The extractor recursively "unpacks" these structures into primitive columns.
2. **Semantic Naming:** To ensure that data remains self-describing, columns are named using the convention: `{topic_name}.{ontology_tag}.{field_path}`.
* Example: `/front/camera/imu.imu.acceleration.x`, `/front/gps.gps.status`.


3. **Namespace Isolation:** The inclusion of the topic name and ontology tag prevents column collisions even when multiple sensors of the same type (e.g., two IMUs) are present in the same sequence.
4. **Memory-Efficient Windowing:** It utilizes a **generator-based windowing approach**. Data is yielded in time-based "chunks" (e.g., 5-second windows). The extractor handles "straddling batches" using a carry-over buffer to ensure zero data loss.
5. **Sparse Merging:** Since sensors have different frequencies, the resulting DataFrame is "sparse." It contains the union of all timestamps, with `NaN` values where a specific sensor did not produce a reading at that exact timestamp.

## API Reference: `DataFrameExtractor`

### `to_pandas_chunks(...)`

This is the primary entry point for converting Mosaico data into Pandas DataFrames.

```python
def to_pandas_chunks(
    topics: Optional[Sequence[str]] = None,
    window_sec: float = 5.0,
    timestamp_ns_start: Optional[int] = None,
    timestamp_ns_end: Optional[int] = None,
) -> Generator[pd.DataFrame, None, None]

```

#### Arguments

* **`topics`**: A list of topic names to extract.
* *Note: Selection is performed at the Topic level to ensure that the resulting rows contain enough information to reconstruct the full Ontology model.*
* Example: `["/front/camera/imu", "/front/gps"]`.


* **`window_sec`**: The temporal size of each DataFrame "chunk". Default is 5s.
* **`timestamp_ns_start / timestamp_ns_end`**: Temporal slicing (nanoseconds). These are automatically clamped to the sequence's valid time range.
* **`include_ontology_tag`**: If `True`, columns are named `topic_name.ontology_tag.field`. If `False`, `topic_name.field`.
    * **`include_ontology_tag=True` (Default):** Recommended for complex sequences where a topic name might be ambiguous, or for strict data validation.
    * **`include_ontology_tag=False`:** Ideal for clean, readable DataFrames when you are certain of the sensor types.

> [!WARNING]
> **Memory Usage**
> 
> When setting `window_sec` to a very large value, ensure your machine has enough RAM. The extractor will load the entire requested range into memory.

#### Returns

A generator yielding `pd.DataFrame` objects.

## Usage Examples

### Basic Extraction (All Topics)

```python
from mosaicolabs.ml import DataFrameExtractor

# Initialize from a SequenceHandler
seq_handler = client.sequence_handler("drive_session_01")
extractor = DataFrameExtractor(seq_handler)

# Iterate through 10-second chunks
for df in extractor.to_pandas_chunks(window_sec=10.0):
    # 'df' is a pandas DataFrame with semantic columns
    # Example column: "/front/camera/imu.imu.acceleration.x"
    print(df.columns)

```

### Targeted Selection with Specific Fields

```python
# Extract only the Z-axis of the IMU and the GPS status
selection = [
    ("/sensors/imu", ["acceleration.y", "angular_velocity.z"]), # Keeps these two columns only
    ("/sensors/front/gps", "status") # Keeps all the columns of the `status` field
]

for df in extractor.to_pandas_chunks(selection=selection):
    # Columns will be: 
    # ['timestamp_ns', '/sensors/imu.imu.*', /sensors/front/gps.gps.*', ...]
    current_accel_z = df['/sensors/imu.imu.acceleration.y'].dropna()
```