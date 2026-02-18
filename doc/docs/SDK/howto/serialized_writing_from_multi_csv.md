# Writing Multiple Topics Serially

This guide demonstrates how to ingest data from multiple custom files into the Mosaico Data Platform. While the logic below uses CSV files as the primary example, the SDK's modular design is compatible with any file format (JSON, Parquet, binary) and any I/O library.

You will learn how to use the Mosaico SDK to:

* **Open a connection** to the Mosaico server.
* **Creating a sequence**.
* **Creating topics**.
* **Pushing data into topics**, via **Controlled Ingestion Patterns** to prevent a single file failure from aborting the entire upload.

### Step 1: Chunked Loading for Heterogeneous Data

The following implementation defines three distinct generators to stream IMU, GPS, and Pressure data.
In this example, we assume our CSV files contain the following columns:

* IMU.csv: `timestamp`, `acc_x`, `acc_y`, `acc_z`, `gyro_x`, `gyro_y`, `gyro_z`
* GPS.csv: `timestamp`, `latitude`, `longitude`, `altitude`, `status`, `service`
* Pressure.csv: `timestamp`, `pressure`

When dealing with massive datasets spread across multiple files, we adopt a **chunked loading approach** for each sensor type.

```python
"""
Import the necessary classes from the Mosaico SDK.
"""
import pandas as pd
from mosaicolabs import (
    MosaicoClient, # The gateway to the Mosaico Platform
    OnErrorPolicy, # The error policy for the SequenceWriter
    Message, # The base class for all data messages
    IMU, # The IMU sensor data class
    Vector3d, # The 3D vector class, needed to populate the IMU and GPS data
    GPS, # The GPS sensor data class
    GPSStatus, # The GPS status enum, needed to populate the GPS data
    Pressure, # The Pressure sensor data class
)

"""
Define the generator functions that yield `Message` objects.
For each file, open the reading process and yield the messages one by one.
"""
def stream_imu_from_csv(file_path: str, chunk_size: int = 1000):
    """Efficiently streams IMU data."""
    for chunk in pd.read_csv(file_path, chunksize=chunk_size):
        for _, row in chunk.iterrows():
            yield Message(
                timestamp_ns=int(row["timestamp"]),
                data=IMU(
                    acceleration=Vector3d(
                        x=row["acc_x"], 
                        y=row["acc_y"], 
                        z=row["acc_z"]
                    ),
                    angular_velocity=Vector3d(
                        x=row["gyro_x"], 
                        y=row["gyro_y"], 
                        z=row["gyro_z"]
                    )
                )
            )

def stream_gps_from_csv(file_path: str, chunk_size: int = 1000):
    """Efficiently streams GPS data."""
    for chunk in pd.read_csv(file_path, chunksize=chunk_size):
        for _, row in chunk.iterrows():
            yield Message(
                timestamp_ns=int(row["timestamp"]),
                data=GPS(
                    position=Vector3d(
                        x=row["latitude"], 
                        y=row["longitude"], 
                        z=row["altitude"]
                    ),
                    status=GPSStatus(
                        status=row["status"], 
                        service=row["service"]
                    )
                )
            )

def stream_pressure_from_csv(file_path: str, chunk_size: int = 1000):
    """Efficiently streams Barometric Pressure data."""
    for chunk in pd.read_csv(file_path, chunksize=chunk_size):
        for _, row in chunk.iterrows():
            yield Message(
                timestamp_ns=int(row["timestamp"]),
                data=Pressure(value=row["pressure"])
            )

```

#### Understanding the Output

The Mosaico [`Message`][mosaicolabs.models.Message] object is an in-memory object wrapping the sensor data with necessary metadata (e.g. timestamp), and ensuring it is ready for serialization and network transmission.

In this specific case, the data are instances of the [`IMU`][mosaicolabs.models.sensors.IMU], [`GPS`][mosaicolabs.models.sensors.GPS] and [`Pressure`][mosaicolabs.models.sensors.Pressure] models. These are built-in parts of the Mosaico default ontology, meaning the platform already understands their schema and how to optimize their storage.

For a more in-depth explanation:

* **[Documentation: Data Models & Ontology](../ontology.md)**
* **[API Reference: Sensor Models](../API_reference/models/sensors.md)**


### Step 2: Orchestrating the Multi-Topic Sequence

To write data, we first establish a connection to the Mosaico server via the [`MosaicoClient.connect()`][mosaicolabs.comm.MosaicoClient.connect] method and create a [`SequenceWriter`][mosaicolabs.handlers.SequenceWriter].
A sequence writer acts as a logical container for related sensor data streams (topics).

When initializing your data handling pipeline, it is highly recommended to wrap the **Mosaico Client** within a `with` statement. This context manager pattern ensures that underlying network connections and shared resource pools are correctly shut down and released when your operations conclude.

```python
"""
Connect to the Mosaico server and create a sequence writer.
"""
with MosaicoClient.connect("localhost", 6726) as client:
    # Initialize the Orchestrator for the entire mission
    with client.sequence_create(
        sequence_name="multi_sensor_ingestion",
        metadata={"mission": "alpha_test", "environment": "laboratory"},
        on_error=OnErrorPolicy.Delete # Deletes the whole sequence if a fatal crash occurs
    ) as swriter:
        # Steps 3 and 4 (Topic Creation & serial Pushing) happen here...

```

!!! warning "Context Management"
    It is **mandatory** to use the `SequenceWriter` instance returned by `client.sequence_create()` inside its own `with` context. The following code will raise an exception:

    ```python
    swriter = client.sequence_create(
        sequence_name="multi_sensor_ingestion",
        metadata={...},
    ) 
    # Performing operations using `swriter` will raise an exception
    swriter.topic_create(...) # Raises here
    ```
    This choice ensures that the sequence writing orchestrator is closed and cataloged when the block is exited, even if your application encounters a crash or is manually interrupted.

#### Sequence-Level Error Handling

The behavior of the orchestrator during a failure is governed by the `on_error` policy. This is a *Last-Resort* automated error policy, which dictates how the server manages a sequence if an unhandled exception bubbles up to the `SequenceWriter` context manager. By default, this is set to [`OnErrorPolicy.Delete`][mosaicolabs.enum.OnErrorPolicy.Delete], which signals the server to physically remove the incomplete sequence and its associated topic directories, if any errors occurred. Alternatively, you can specify [`OnErrorPolicy.Report`][mosaicolabs.enum.OnErrorPolicy.Report]: in this case, the SDK will not delete the data but will instead send an error notification to the server, allowing the platform to flag the sequence as failed while retaining whatever records were successfully transmitted before the error occurred.

For a more in-depth explanation:

* **[Documentation: The Writing Workflow](../handling/writing.md)**
* **[API Reference: Writing Data](../API_reference/handlers/writing.md)**

### Step 3: Topic Creation and Resource Allocation

Inside the sequence, we create individual **Topic Writers** to manage data streams. Each writer is an independent "lane" assigned its own internal buffer and background thread for serialization.

```python
        # Inside the SequenceWriter context...
        
        # Create dedicated Topic Writers for each sensor stream
        imu_twriter = swriter.topic_create(
            topic_name="sensors/imu",
            metadata={"sensor_id": "accel_01"},
            ontology_type=IMU,
            on_error=OnErrorTopicPolicy.Finalize  # Default behavior
        )
        
        gps_twriter = swriter.topic_create(
            topic_name="sensors/gps",
            metadata={"sensor_id": "gps_01"},
            ontology_type=GPS,
            on_error=OnErrorTopicPolicy.Finalize
        )
        
        pressure_twriter = swriter.topic_create(
            topic_name="sensors/pressure",
            metadata={"sensor_id": "pressure_01"},
            ontology_type=Pressure,
            on_error=OnErrorTopicPolicy.Ignore  # Non-critical sensor
        )

```

#### Topic-Level Error Handling

The `TopicWriter` accepts an `on_error` policy parameter that determines how the system reacts to failures occurring within that specific topic's processing block.

| Policy | Behavior | Use Case |
| --- | --- | --- |
| **`Finalize`** | Notifies the server of the error and immediately closes the topic channel. | **Recommended Default**: Best for sensors where data integrity is critical and further "corrupted" pushes should be prevented. |
| **`Ignore`** | Notifies the server of the error but keeps the writer alive for subsequent `push()` calls. | Best for non-critical sensors where a transient error (e.g., one malformed CSV row) should not stop the entire stream. |
| **`Raise`** | Notifies the server and allows the exception to bubble up outside the `with` block. | Used when a specific topic's failure is so severe that it should trigger the global `SequenceWriter` error policy. |

This mechanism is only active when the `TopicWriter` is used as a context manager (highly recommended). If a failure occurs inside the `with imu_twriter:` block ([Step 4](#step-4-pushing-data-into-the-pipeline)), the specified policy is automatically enforced, ensuring local errors are handled without necessarily crashing the parent sequence.


### Step 4: Pushing Data into the Pipeline

The final stage of the ingestion process involves iterating through your data generators and transmitting records to the Mosaico platform by calling the [`TopicWriter.push()`][mosaicolabs.handlers.TopicWriter.push] method for each record. The `push()` method optimizes the throughput by accumulating messages into internal batches.

```python
        # --- 1. Push IMU Data ---
        # The 'with' context handles automatic finalization if an error occurs
        with imu_twriter:
            for msg in stream_imu_from_csv("imu_data.csv"):
                imu_twriter.push(message=msg)
        
        # --- 2. Push GPS Data with Custom Processing ---
        with gps_twriter:
            for msg in stream_gps_from_csv("gps_data.csv"):
                # This custom processing might fail
                process_gps_message(msg) 
                gps_twriter.push(message=msg)

        # --- 3. Push Pressure Data ---
        with pressure_twriter:
            for msg in stream_pressure_from_csv("pressure_data.csv"):
                pressure_twriter.push(message=msg)

    # All buffers are flushed and the sequence is committed when exiting the SequenceWriter 'with' block
    print("Multi-topic ingestion completed!")

```

#### Ingestion Best-Practices

It is recommended to use the **Topic Writer Context Manager** to implement a **Controlled Ingestion Pattern**. On the contrary, if data unpacking or processing fails (e.g., within a custom method like `process_gps_message`), the SDK cannot natively distinguish which specific topic failed within your custom code. In this situation, an exception would bubble up to the `SequenceWriter`, triggering the global `OnErrorPolicy` and potentially terminating the entire ingestion process.

To prevent a single corrupted file or transformation bug from aborting the entire mission, we implement a **Controlled Ingestion Pattern**:

* Isolate failures to that specific stream, by wrapping each topic loop in its own `with topic_twriter:` block.
* If an error occurs within the `with` block, the `TopicWriter` automatically invokes its `on_error` policy (e.g., `Finalize`).
* The failing topic is handled according to its policy, while healthy streams continue their ingestion uninterrupted. The final sequence on the server will contain all successful data alongside a reported error for the failed sensor.

!!! note "Defensive Ingestion Patterns"
    Always use the `with topic_writer:` context manager for each sensor stream. This is significantly cleaner than manual `try-except` blocks and ensures that even if a transformation function bugs out, your overall ingestion process remains resilient.

