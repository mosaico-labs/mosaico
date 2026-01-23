# DataFrameExtractor

The `DataFrameExtractor` is a high-performance utility designed to convert Mosaico sequences into tabular formats. It acts as a specialized wrapper around the `SequenceDataStreamer`, optimized for the ML community's preference for **columnar data structures**.

## Architecture & Design Principles

Unlike the standard `SequenceDataStreamer`, which yields one `Message` at a time, the `DataFrameExtractor` operates at the **Batch Level**. It pulls raw `RecordBatch` objects directly from the underlying stream, significantly reducing the overhead of Python object instantiation.

### Key Technical Features

1.  **Recursive Flattening:** Mosaico's Ontology can be deeply nested. The extractor recursively "unpacks" these structures. For example, a `MotionState` object is automatically flattened into individual columns: `pose.position.x`, `pose.position.y`, etc.
2.  **Namespace Isolation:** To support sequences with multiple sensors of the same type (e.g., two IMUs), the extractor prefixes all columns with the topic name: `/imu_front.acceleration.x`, `/camera/left/imu.acceleration.x`.
3.  **Memory-Efficient Windowing:** It utilizes a **generator-based windowing approach**. You can request data in time-based "chunks" (e.g., 5-second windows). The extractor handles "straddling batches", using a carry-over buffer to ensure zero data loss.
4.  **Sparse Merging:** Since sensors have different frequencies, the resulting DataFrame is "sparse." It contains the union of all timestamps, with `NaN` values where a specific sensor did not produce a reading at that exact micro-moment.

## API Reference: `DataFrameExtractor`

### `to_pandas_chunks(...)`

This is the primary entry point for converting Mosaico data into Pandas DataFrames.

```python
def to_pandas_chunks(
    selection: Optional[Sequence[Union[str, Tuple[str, Union[str, List[str]]]]]] = None,
    window_sec: float = 5.0,
    timestamp_ns_start: Optional[int] = None,
    timestamp_ns_end: Optional[int] = None,
) -> Generator[pd.DataFrame, None, None]
```

#### Arguments

* **`selection`**: Defines what to extract. Various configurations are supported:
    * `["/front_imu", "/gps"]`: Extract all fields from these topics.
    * `[("/front_imu", ["acceleration.x", "acceleration.y"]), "/gps"]`: Extract specific fields for IMU and all for GPS.
* **`window_sec`**: The temporal size of each DataFrame "chunk". Default is 5s.
* **`timestamp_ns_start / timestamp_ns_end`**: Temporal slicing (nanoseconds). These are automatically clamped to the sequence's valid time range.

> [!WARNING]
> **Memory Usage**
>
> When setting `window_sec` to a very large value, ensure your machine has enough RAM. The extractor will warn you if the requested window exceeds the sequence duration.

#### Returns

A generator yielding `pd.DataFrame` objects.

---

## Usage Examples

### Basic Extraction (All Topics)

```python
from mosaicolabs.ml import DataFrameExtractor

# Initialize from a SequenceHandler
seq_handler = client.sequence_handler("drive_session_01")
extractor = DataFrameExtractor(seq_handler)

# Iterate through 10-second chunks
for df in extractor.to_pandas_chunks(window_sec=10.0):
    # 'df' is a pandas DataFrame
    print(df.info())
    # Process your ML batch here

```

### Targeted Selection with Specific Fields

```python
# Extract only the Z-axis of the IMU and the GPS status
selection = [
    ("/sensors/imu", ["acceleration.y", "angular_velocity.z"]), # Keeps these two columns only
    ("/sensors/gps", "status") # Keeps all the columns of the `status` field
]

for df in extractor.to_pandas_chunks(selection=selection):
    # Columns will be: 
    # ['timestamp_ns', '/sensors/imu.acceleration.y', '/sensors/imu.angular_velocity.z', '/sensors/gps.status.*', ...]
    current_accel_z = df['/sensors/imu.acceleration.y'].dropna()
```