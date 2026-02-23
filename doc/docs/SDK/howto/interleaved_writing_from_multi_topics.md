# Writing Interleaved Topics

This guide demonstrates how to ingest data from multiple topics stored within a **single file container** (such as an MCAP or a specialized binary log) into the Mosaico Data Platform. Unlike serial ingestion where files are processed one by one, interleaved ingestion handles a stream of messages from different sensors—such as IMU, GPS, and Pressure—as they appear in the source file.

For this guide, we use the **[MCAP library](https://mcap.dev/)** as an example to briefly show how to parse a high-performance robotics container and stream its contents into Mosaico.

You will learn how to:

* **Orchestrate a single sequence** for a multi-sensor stream.
* **Dynamically resolve Topic Writers** using the local SDK cache.
* **Implement a Custom Translator** to map external schemas to the Mosaico Ontology.
* **Isolate failures** to a single sensor stream using Defensive Ingestion patterns.


### The Multi-Topic Streaming Architecture

In a mixed ingestion scenario, the source file provides a serialized stream of records. Each record contains a **topic name**, a **timestamp**, and a **data payload** associated with a specific **schema**.

| Topic | Schema Example (MCAP) | Mosaico Target Model |
| --- | --- | --- |
| `/robot/imu` | `sensor_msgs/msg/Imu` | `IMU` |
| `/robot/gps` | `sensor_msgs/msg/NavSatFix` | `GPS` |
| `/env/pressure` | `sensor_msgs/msg/FluidPressure` | `Pressure` |

As the reader iterates through the file, Mosaico dynamically assigns each record to its corresponding "lane" (Topic Writer).


### Step 1: Implementing the Custom Translator and Adapters

Because source files often use external data formats (like ROS, Protobuf, or JSON), you need a translation layer to map these raw payloads into strongly-typed Mosaico objects.

```python title="Map incoming data schemas to Mosaico Ontology models."
from mosaicolabs.models import (IMU, 
                                GPS, 
                                Pressure, 
                                Vector3d, 
                                GPSStatus, 
                                Time, 
                                Serializable)

def custom_translator(schema_name: str, payload: dict):
    if schema_name == "sensor_msgs/msg/Imu":
        header = payload['header']
        timestamp_ns = Time(
            sec=header['stamp']['sec'], 
            nanosec=header['stamp']['nanosec']
        ).to_nanoseconds()
        return Message(
            timestamp_ns=timestamp_ns,
            data=IMU(
                acceleration=Vector3d(**payload['linear_acceleration']),
                angular_velocity=Vector3d(**payload['angular_velocity'])
            )
        )
    
    if schema_name == "sensor_msgs/msg/NavSatFix":
        header = payload['header']
        timestamp_ns = Time(
            sec=header['stamp']['sec'], 
            nanosec=header['stamp']['nanosec']
        ).to_nanoseconds()
        return Message(
            timestamp_ns=timestamp_ns,
            data=GPS(
                position=Vector3d(
                    x=payload['latitude'], 
                    y=payload['longitude'], 
                    z=payload['altitude']
                ),
                status=GPSStatus(
                    status=payload['status']['status'], 
                    service=payload['status']['service']
                )
            )
        )
        
    if schema_name == "sensor_msgs/msg/FluidPressure":
        header = payload['header']
        timestamp_ns = Time(
            sec=header['stamp']['sec'], 
            nanosec=header['stamp']['nanosec']
        ).to_nanoseconds()
        return Message(
            timestamp_ns=timestamp_ns,
            data=Pressure(value=payload['fluid_pressure'])
        )
        
    return None


def determine_mosaico_type(schema_name: str) -> Optional[Type["Serializable"]]:
    """Determine the Mosaico type of the topic based on the schema name."""
    if schema_name == "sensor_msgs/msg/Imu":
        return IMU
    elif schema_name == "sensor_msgs/msg/NavSatFix":
        return GPS
    elif schema_name == "sensor_msgs/msg/FluidPressure":
        return Pressure
    
    return None

```

#### Understanding the Output

The Mosaico [`Message`][mosaicolabs.models.Message] object is an in-memory object wrapping the sensor data with necessary metadata (e.g. timestamp), and ensuring it is ready for serialization and network transmission.

In this specific case, the data are instances of the [`IMU`][mosaicolabs.models.sensors.IMU], [`GPS`][mosaicolabs.models.sensors.GPS] and [`Pressure`][mosaicolabs.models.sensors.Pressure] models. These are built-in parts of the Mosaico default ontology, meaning the platform already understands their schema and how to optimize their storage.

For a more in-depth explanation:

* **[Documentation: Data Models & Ontology](../ontology.md)**
* **[API Reference: Sensor Models](../API_reference/models/sensors.md)**

### Step 2: Orchestrating the Multi-Topic Interleaved Ingestion

To write data, we first establish a connection to the Mosaico server via the [`MosaicoClient.connect()`][mosaicolabs.comm.MosaicoClient.connect] method and create a [`SequenceWriter`][mosaicolabs.handlers.SequenceWriter].
A sequence writer acts as a logical container for related sensor data streams (topics).

When initializing your data handling pipeline, it is highly recommended to wrap the **Mosaico Client** within a `with` statement. This context manager pattern ensures that underlying network connections and shared resource pools are correctly shut down and released when your operations conclude.

```python title="Connect to the Mosaico server and create a sequence writer"
from mcap.reader import make_reader
from mosaicolabs import MosaicoClient, OnErrorPolicy, Message

with open("mission_data.mcap", "rb") as f:
    reader = make_reader(f)
    with MosaicoClient.connect("localhost", 6726) as client:
        with client.sequence_create(
            sequence_name="multi_sensor_ingestion",
            metadata={"mission": "alpha_test", "environment": "laboratory"},
            on_error=OnErrorPolicy.Delete # (1)!
        ) as swriter:
            # Steps 3 and 4 (Topic Creation & Pushing) happen here...

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

Inside the sequence, we can stream interleaved data without loading the entire file into memory. We automatically create individual **Topic Writers** per each channel in the MCAP file to manage data streams. Each writer is an independent "lane" assigned its own internal buffer and background thread for serialization. The [`swriter.get_topic_writer`][mosaicolabs.handlers.SequenceWriter.get_topic_writer] pattern removes the need to pre-scan the file. **Topics are created only when they are first encountered**.

```python
with client.sequence_create(...) as swriter:
    # Iterate through all interleaved messages
    for schema, channel, message in reader.iter_messages():
        # 1. Resolve Topic Writer using the SDK cache
        twriter = swriter.get_topic_writer(channel.topic) # (1)!
        
        if twriter is None:
            ontology_type = determine_mosaico_type(schema.name)
            if ontology_type is None:
                print(f"Skipping message on {channel.topic} due to unknown ontology type")
                # Skip the topic if no ontology type is found
                continue
                
            # Dynamically register the topic writer upon discovery
            twriter = swriter.topic_create( # (2)!
                topic_name=channel.topic,
                metadata={},
                ontology_type=ontology_type
            )
```

1. Here we are checking if the a `TopicWriter` for the current topic already exists.
2. Here we are creating the topic writer for the current topic, if it doesn't exist yet.


### Step 4: Pushing Data into the Pipeline

The final stage of the ingestion process involves iterating through your data generators and transmitting records to the Mosaico platform by calling the [`TopicWriter.push()`][mosaicolabs.handlers.TopicWriter.push] method for each record. The `push()` method optimizes the throughput by accumulating messages into internal batches.

```python
        try:
            # In a real scenario, use a deserializer like mcap_ros2.decoder
            raw_data = deserialize_payload(message.data, schema.name) # (1)!
            mosaico_msg = custom_translator(schema.name, raw_data)

            if mosaico_msg is None:
                # Log and skip, or raise if incomplete data is disallowed
                print("Skipping row due to parsing error")
                continue # Ignore malformed records
            
            twriter.push(message=mosaico_msg)
        except Exception as e:
            print(f"Skip error on {channel.topic} at {message.log_time}: {e}")

```

1. This is an example of a custom function that deserializes the payload of the current message.

#### Topic-Level Error Management

In the code snippet above, we implemented a **Controlled Ingestion** by wrapping the topic-specific processing and pushing logic within a local `try-except` block.
Because the `SequenceWriter` cannot natively distinguish which specific topic failed within your custom processing code (such as a coordinate transformation), an unhandled exception will bubble up and trigger the global sequence-level error policy. To avoid this, you should catch errors locally for each topic.

Upcoming versions of the SDK will introduce native **Topic-Level Error Policies**. This feature will allow you to define the error behavior directly when creating the topic, removing the need for boilerplate `try-except` blocks around every sensor stream.


## The full example code

```python
"""
Import the necessary classes from the Mosaico SDK.
"""
from mcap.reader import make_reader

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
For each schema, we define a function that translates the payload
of the current message into a `Message` object.
"""
def custom_translator(schema_name: str, payload: dict):
    if schema_name == "sensor_msgs/msg/Imu":
        header = payload['header']
        timestamp_ns = Time(
            sec=header['stamp']['sec'], 
            nanosec=header['stamp']['nanosec']
        ).to_nanoseconds()
        return Message(
            timestamp_ns=timestamp_ns,
            data=IMU(
                acceleration=Vector3d(**payload['linear_acceleration']),
                angular_velocity=Vector3d(**payload['angular_velocity'])
            )
        )
    
    if schema_name == "sensor_msgs/msg/NavSatFix":
        header = payload['header']
        timestamp_ns = Time(
            sec=header['stamp']['sec'], 
            nanosec=header['stamp']['nanosec']
        ).to_nanoseconds()
        return Message(
            timestamp_ns=timestamp_ns,
            data=GPS(
                position=Vector3d(
                    x=payload['latitude'], 
                    y=payload['longitude'], 
                    z=payload['altitude']
                ),
                status=GPSStatus(
                    status=payload['status']['status'], 
                    service=payload['status']['service']
                )
            )
        )
        
    if schema_name == "sensor_msgs/msg/FluidPressure":
        header = payload['header']
        timestamp_ns = Time(
            sec=header['stamp']['sec'], 
            nanosec=header['stamp']['nanosec']
        ).to_nanoseconds()
        return Message(
            timestamp_ns=timestamp_ns,
            data=Pressure(value=payload['fluid_pressure'])
        )
        
    return None


def determine_mosaico_type(schema_name: str) -> Optional[Type["Serializable"]]:
    """Determine the Mosaico type of the topic based on the schema name."""
    if schema_name == "sensor_msgs/msg/Imu":
        return IMU
    elif schema_name == "sensor_msgs/msg/NavSatFix":
        return GPS
    elif schema_name == "sensor_msgs/msg/FluidPressure":
        return Pressure
    return None

"""
Main ingestion orchestration
"""
def main():
    with open("mission_data.mcap", "rb") as f:
        reader = make_reader(f)
        with MosaicoClient.connect("localhost", 6726) as client:
            with client.sequence_create(
                sequence_name="multi_sensor_ingestion",
                metadata={"mission": "alpha_test", "environment": "laboratory"},
                on_error=OnErrorPolicy.Delete
            ) as swriter:
                # Iterate through all interleaved messages
                for schema, channel, message in reader.iter_messages():
                    # 1. Resolve Topic Writer using the SDK cache
                    twriter = swriter.get_topic_writer(channel.topic)
                    
                    if twriter is None:
                        ontology_type = determine_mosaico_type(schema.name)
                        if ontology_type is None:
                            print(f"Skipping message on {channel.topic} due to unknown ontology type")
                            # Skip the topic if no ontology type is found
                            continue
                            
                        # Dynamically register the topic writer upon discovery
                        twriter = swriter.topic_create(
                            topic_name=channel.topic,
                            metadata={},
                            ontology_type=ontology_type
                        )

                    # 2. Defensive Ingestion: Isolate errors to this specific record
                    try:
                        # In a real scenario, use a deserializer like mcap_ros2.decoder
                        raw_data = deserialize_payload(message.data, schema.name) # Example helper function
                        mosaico_msg = custom_translator(schema.name, raw_data)

                        if mosaico_msg is None:
                            # Log and skip, or raise if incomplete data is disallowed
                            print("Skipping row due to parsing error")
                            continue # Ignore malformed records
                        
                        twriter.push(message=mosaico_msg)
                    except Exception as e:
                        print(f"Skip error on {channel.topic} at {message.log_time}: {e}")

        # All buffers are flushed and the sequence is committed when exiting the SequenceWriter 'with' block
        print("Multi-topic ingestion completed!")
```