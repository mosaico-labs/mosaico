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

```csv title="imu.csv"
timestamp, acc_x, acc_y, acc_z, gyro_x, gyro_y, gyro_z
1110022, 0.0032, 0.001, -0.002, 0.01, 0.005, -0.003
```

```csv title="gps.csv"
timestamp, latitude, longitude, altitude, status, service
1110022, 45.123456, -93.123456, 250.0, 1, 1
```

```csv title="pressure.csv"
timestamp, pressure
1110022, 101325.0
```

When dealing with massive datasets spread across multiple files, we adopt a **chunked loading approach** for each sensor type.

```python title="Define the generator functions that yield Message objects"
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


# Define the generator functions that yield `Message` objects.
# For each file, open the reading process and yield the messages one by one.


def stream_imu_from_csv(file_path: str, chunk_size: int = 1000):
    for chunk in pd.read_csv(file_path, chunksize=chunk_size):
        for row in chunk.itertuples(index=False):
            yield Message(
                timestamp_ns=int(row.timestamp),
                data=IMU(
                    acceleration=Vector3d(
                        x=float(row.acc_x),
                        y=float(row.acc_y),
                        z=float(row.acc_z),
                    ),
                    angular_velocity=Vector3d(
                        x=float(row.gyro_x),
                        y=float(row.gyro_y),
                        z=float(row.gyro_z),
                    )
                )
            )
       


def stream_gps_from_csv(file_path: str, chunk_size: int = 1000):
    for chunk in pd.read_csv(file_path, chunksize=chunk_size):
        for row in chunk.itertuples(index=False):            
            yield Message(
                timestamp_ns=int(row.timestamp),
                data=GPS(
                    position=Vector3d(
                        x=float(row.latitude),
                        y=float(row.longitude),
                        z=float(row.altitude),
                    ),
                    status=GPSStatus(
                        status=int(row.status), 
                        service=int(row.service),
                    )
                )
            )


def stream_pressure_from_csv(file_path: str, chunk_size: int = 1000):
    for chunk in pd.read_csv(file_path, chunksize=chunk_size):
        for row in chunk.itertuples(index=False):
            yield Message(
                timestamp_ns=int(row.timestamp),
                data=Pressure(value=row.pressure)
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

```python title="Connect to the Mosaico server and create a sequence writer"

with MosaicoClient.connect("localhost", 6726) as client:
    with client.sequence_create(
        sequence_name="multi_sensor_ingestion",
        metadata={"mission": "alpha_test", "environment": "laboratory"},
        on_error=OnErrorPolicy.Delete # (1)!
    ) as swriter:
        # Steps 3 and 4 (Topic Creation & serial Pushing) happen here...

```

1. Mosaico supports two distinct error policies for sequences: `OnErrorPolicy.Delete` and `OnErrorPolicy.Report`.

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
with client.sequence_create(...) as swriter:
    
    # Create dedicated Topic Writers for each sensor stream
    imu_twriter = swriter.topic_create( # (1)!
        topic_name="sensors/imu",
        metadata={"sensor_id": "accel_01"},
        ontology_type=IMU,
    )
    
    gps_twriter = swriter.topic_create( # (2)!
        topic_name="sensors/gps",
        metadata={"sensor_id": "gps_01"},
        ontology_type=GPS,
    )
    
    pressure_twriter = swriter.topic_create( # (3)!
        topic_name="sensors/pressure",
        metadata={"sensor_id": "pressure_01"},
        ontology_type=Pressure,
    )
```

1. Here we are creating a dedicated writer for the IMU topic
2. Here we are creating a dedicated writer for the GPS topic
3. Here we are creating a dedicated writer for the Pressure topic

### Step 4: Pushing Data into the Pipeline

The final stage of the ingestion process involves iterating through your data generators and transmitting records to the Mosaico platform by calling the [`TopicWriter.push()`][mosaicolabs.handlers.TopicWriter.push] method for each record. The `push()` method optimizes the throughput by accumulating messages into internal batches.

```python
        # 1. Push IMU Data
        for msg in stream_imu_from_csv("imu.csv"):
            if msg is None:
                # Log and skip, or raise if incomplete data is disallowed
                print("Skipping row due to parsing error")
                continue # Ignore malformed records
            try:
                imu_twriter.push(message=msg)
            except Exception as e:
                # Log and skip, or raise if incomplete data is disallowed
                print(f"Error processing IMU at time: {msg.timestamp_ns}. Inner err: {e}")

        # 2. Push GPS Data with Custom Processing
        for msg in stream_gps_from_csv("gps.csv"):
            if msg is None:
                # Log and skip, or raise if incomplete data is disallowed
                print("Skipping row due to parsing error")
                continue # Ignore malformed records
            try:
                # This custom processing might fail
                process_gps_message(msg) 
                gps_twriter.push(message=msg)
            except Exception as e:
                # Log and skip, or raise if incomplete data is disallowed
                print(f"Error processing GPS at time: {msg.timestamp_ns}. Inner err: {e}")

        # 3. Push Pressure Data
        for msg in stream_pressure_from_csv("pressure.csv"):
            if msg is None:
                # Log and skip, or raise if incomplete data is disallowed
                print("Skipping row due to parsing error")
                continue # Ignore malformed records
            try:
                pressure_twriter.push(message=msg)
            except Exception as e:
                # Log and skip, or raise if incomplete data is disallowed
                print(f"Error processing pressure at time: {msg.timestamp_ns}. Inner err: {e}")

```

#### Topic-Level Error Management

In the code snippet above, we implemented a **Controlled Ingestion** by wrapping the topic-specific processing and pushing logic within a local `try-except` block.
Because the `SequenceWriter` cannot natively distinguish which specific topic failed within your custom processing code (such as a coordinate transformation or a malformed CSV row), an unhandled exception will bubble up and trigger the global sequence-level error policy. To avoid this, you should catch errors locally for each topic.

Upcoming versions of the SDK will introduce native **Topic-Level Error Policies**. This feature will allow you to define the error behavior directly when creating the topic, removing the need for boilerplate `try-except` blocks around every sensor stream.

## The full example code

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
        for row in chunk.itertuples(index=False):
            try:
                yield Message(
                    timestamp_ns=int(row.timestamp),
                    data=IMU(
                        acceleration=Vector3d(
                            x=float(row.acc_x),
                            y=float(row.acc_y),
                            z=float(row.acc_z),
                        ),
                        angular_velocity=Vector3d(
                            x=float(row.gyro_x),
                            y=float(row.gyro_y),
                            z=float(row.gyro_z),
                        )
                    )
                )
            except Exception:
                # Yield None only for parsing/type-related errors
                yield None


def stream_gps_from_csv(file_path: str, chunk_size: int = 1000):
    """Efficiently streams GPS data."""
    for chunk in pd.read_csv(file_path, chunksize=chunk_size):
        for row in chunk.itertuples(index=False):
            try:
                yield Message(
                    timestamp_ns=int(row.timestamp),
                    data=GPS(
                        position=Vector3d(
                            x=float(row.latitude),
                            y=float(row.longitude),
                            z=float(row.altitude),
                        ),
                        status=GPSStatus(
                            status=int(row.status), 
                            service=int(row.service),
                        )
                    )
                )

            except Exception:
                # Yield None only for parsing/type-related errors
                yield None

def stream_pressure_from_csv(file_path: str, chunk_size: int = 1000):
    """Efficiently streams Barometric Pressure data."""
    for chunk in pd.read_csv(file_path, chunksize=chunk_size):
        for row in chunk.itertuples(index=False):
            try:
                yield Message(
                    timestamp_ns=int(row.timestamp),
                    data=Pressure(value=row.pressure)
                )
            except Exception:
                # Yield None only for parsing/type-related errors
                yield None

"""
Main ingestion orchestration
"""
def main():
    with MosaicoClient.connect("localhost", 6726) as client:
        # Initialize the Orchestrator for the entire mission
        with client.sequence_create(
            sequence_name="multi_sensor_ingestion",
            metadata={"mission": "alpha_test", "environment": "laboratory"},
            on_error=OnErrorPolicy.Delete # Deletes the whole sequence if a fatal crash occurs
        ) as swriter:
            # Create dedicated Topic Writers for each sensor stream
            imu_twriter = swriter.topic_create(
                topic_name="sensors/imu",
                metadata={"sensor_id": "accel_01"},
                ontology_type=IMU,
            )
            
            gps_twriter = swriter.topic_create(
                topic_name="sensors/gps",
                metadata={"sensor_id": "gps_01"},
                ontology_type=GPS,
            )
            
            pressure_twriter = swriter.topic_create(
                topic_name="sensors/pressure",
                metadata={"sensor_id": "pressure_01"},
                ontology_type=Pressure,
            )

            # --- 1. Push IMU Data ---
            for msg in stream_imu_from_csv("imu.csv"):
                if msg is None:
                    # Log and skip, or raise if incomplete data is disallowed
                    print("Skipping row due to parsing error")
                    continue # Ignore malformed records
                try:
                    imu_twriter.push(message=msg)
                except Exception as e:
                    # Log and skip, or raise if incomplete data is disallowed
                    print(f"Error processing IMU at time: {msg.timestamp_ns}. Inner err: {e}") 

            # --- 2. Push GPS Data with Custom Processing ---
            for msg in stream_gps_from_csv("gps.csv"):
                if msg is None:
                    # Log and skip, or raise if incomplete data is disallowed
                    print("Skipping row due to parsing error")
                    continue # Ignore malformed records
                try:
                    # This custom processing might fail
                    process_gps_message(msg) 
                    gps_twriter.push(message=msg)
                except Exception as e:
                    # Log and skip, or raise if incomplete data is disallowed
                    print(f"Error processing GPS at time: {msg.timestamp_ns}. Inner err: {e}")

            # --- 3. Push Pressure Data ---
            for msg in stream_pressure_from_csv("pressure.csv"):
                if msg is None:
                    # Log and skip, or raise if incomplete data is disallowed
                    print("Skipping row due to parsing error")
                    continue # Ignore malformed records
                try:
                    pressure_twriter.push(message=msg)
                except Exception as e:
                    # Log and skip, or raise if incomplete data is disallowed
                    print(f"Error processing pressure at time: {msg.timestamp_ns}. Inner err: {e}")

        # All buffers are flushed and the sequence is committed when exiting the SequenceWriter 'with' block
        print("Multi-topic ingestion completed!")
```
