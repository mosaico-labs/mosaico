---
title: The Writing Workflow
description: Data Writers.
---

The **Writing Workflow** in Mosaico is designed for high-throughput data ingestion, ensuring that your application remains responsive even when streaming high-bandwidth sensor data like 4K video or high-frequency IMU telemetry.

The architecture is built around a **"Multi-Lane"** approach, where each sensor stream operates in its own isolated lane with dedicated system resources.

### The Orchestrator: `SequenceWriter`
API Reference: [`mosaicolabs.handlers.SequenceWriter`][mosaicolabs.handlers.SequenceWriter].

The `SequenceWriter` acts as the central controller for a recording session. It manages the high-level lifecycle of the data on the server and serves as the factory for individual sensor streams.

**Key Roles:**

* **Lifecycle Management**: It handles the lifecycle of a new sequence and ensures that it is either successfully committed as immutable data or, in the event of a failure, cleaned up according to your configured [`OnErrorPolicy`][mosaicolabs.enum.OnErrorPolicy].
* **Resource Distribution**: The writer pulls network connections from the **Connection Pool** and background threads from the **Executor Pool**, assigning them to individual topics. This isolation prevents a slow network connection on one topic from bottlenecking others.
* **Context Safety**: To ensure data integrity, the `SequenceWriter` must be used within a Python `with` block. This guarantees that all buffers are flushed and the sequence is closed properly, even if your application crashes.

```python
from mosaicolabs import MosaicoClient, OnErrorPolicy

# Open the connection with the Mosaico Client
with MosaicoClient.connect("localhost", 6726) as client:
    # Start the Sequence Orchestrator
    with client.sequence_create(
        sequence_name="mission_log_042", 
        # Custom metadata for this data sequence.
        metadata={ # (1)!
            "vehicle": {
                "vehicle_id": "veh_sim_042",
                "powertrain": "EV",
                "sensor_rig_version": "v3.2.1",
                "software_stack": {
                    "perception": "perception-5.14.0",
                    "localization": "loc-2.9.3",
                    "planning": "plan-4.1.7",
                },
            },
            "driver": {
                "driver_id": "drv_sim_017",
                "role": "validation",
                "experience_level": "senior",
            },
        }
        on_error = OnErrorPolicy.Delete # Default
        ) as seq_writer:

        # `seq_writer` is the writing handler of the new 'mission_log_042' sequence
        # Data will be uploaded by spawning topic writers that will manage the actual data stream 
        # remote push... See below.

```

1. The metadata fields will be queryable via the [`Query` mechanism](../query.md). The mechanism allows creating queries like: `Sequence.Q.user_metadata["vehicle.software_stack.planning"].match("plan-4.")`

### The Data Engine: `TopicWriter`
API Reference: [`mosaicolabs.handlers.TopicWriter`][mosaicolabs.handlers.TopicWriter].

Once a topic is created, a `TopicWriter` is spawned to handle the actual transmission of data for that specific stream. It abstracts the underlying networking protocols, allowing you to simply "push" Python objects while it handles the heavy lifting.

**Key Roles:**

* **Smart Buffering**: Instead of sending every single message over the network—which would be highly inefficient—the `TopicWriter` accumulates records in a memory buffer.
* **Automated Flushing**: The writer automatically triggers a "flush" to the server whenever the internal buffer exceeds your configured limits, such as a maximum byte size or a specific number of records.
* **Asynchronous Serialization**: For CPU-intensive data (like encoding images), the writer can offload the serialization process to background threads, ensuring your main application loop stays fast.

```python
# Continues from the code above...

    # 👉 with client.sequence_create(...) as seq_writer:
        # Create individual Topic Writers
        # Each writer gets its own assigned resources from the pools
        imu_writer = seq_writer.topic_create(
            topic_name="sensors/imu", # The univocal topic name
            metadata={ # The topic/sensor custom metadata
                "vendor": "inertix-dynamics",
                "model": "ixd-f100",
                "firmware_version": "1.2.0",
                "serial_number": "IMUF-9A31D72X",
                "calibrated":"false",
            },
            ontology_type=IMU, # The ontology type stored in this topic
        )

        # Another individual topic writer for the GPS device
        gps_writer = seq_writer.topic_create(
            topic_name="sensors/gps", # The univocal topic name
            metadata={ # The topic/sensor custom metadata
                "role": "primary_gps",
                "vendor": "satnavics",
                "model": "snx-g500",
                "firmware_version": "3.2.0",
                "serial_number": "GPS-7C1F4A9B",            
                "interface": { # (1)!
                    "type": "UART",
                    "baudrate": 115200,
                    "protocol": "NMEA",
                },
            }, # The topic/sensor custom metadata
            ontology_type=GPS, # The ontology type stored in this topic
        )

        # Push data - The SDK handles batching and background I/O
        imu_writer.push(
            message=Message(
                timestamp_ns=1700000000000, 
                data=IMU(acceleration=Vector3d(x=0, y=0, z=9.81), ...),
            )
        )

        gps_writer.push(
            message=Message(
                timestamp_ns=1700000000100, 
                data=GPS(position=Vector3d(x=44.0123,y=10.12345,z=0), ...),
            )
        )

# Exiting the block automatically flushes all topic buffers, finalizes the sequence on the server 
# and closes all connections and pools
```

1. The metadata fields will be queryable via the [`Query` mechanism](../query.md). The mechanism allows creating query expressions like: `Topic.Q.user_metadata["interface.type"].eq("UART")`.
    API Reference:
    * [`mosaicolabs.models.platform.Topic`][mosaicolabs.models.platform.Topic]
    * [`mosaicolabs.models.query.builders.QueryTopic`][mosaicolabs.models.query.builders.QueryTopic].

### Resilient Data Ingestion & Error Management

Recording high-bandwidth sensor data in dynamic environments requires a tiered approach to error handling. While the Mosaico SDK provides automated recovery through **Error Policies**, these act as a "last line of defense". For robust production pipelines, you must implement **Defensive Ingestion Patterns** to prevent isolated failures from compromising your entire recording session.

### Sequence-Level Error Handling
API Reference: [`mosaicolabs.enum.OnErrorPolicy`][mosaicolabs.enum.OnErrorPolicy].

Configured when instantiating a new [`SequenceWriter`][mosaicolabs.handlers.SequenceWriter] via [`MosaicoClient.connect()`][mosaicolabs.comm.MosaicoClient.connect] factory, these policies dictate how the server handles a sequence if an unhandled exception bubbles up to the `SequenceWriter` context manager.

#### 1. [`OnErrorPolicy.Delete`][mosaicolabs.enum.OnErrorPolicy.Delete] (The "Clean Slate" Policy)

* **Behavior**: If an error occurs, the SDK sends an `ABORT` signal to the server.
* **Result**: The server immediately deletes the entire sequence and all associated topic data.
* **Best For**: CI/CD pipelines, unit testing, or "Gold Dataset" generation where partial or corrupted logs are unacceptable.

#### 2. [`OnErrorPolicy.Report`][mosaicolabs.enum.OnErrorPolicy.Report] (The "Recovery" Policy)

* **Behavior**: The SDK finalizes data that successfully reached the server and sends a `NOTIFY_CREATE` signal with error details.
* **Result**: The sequence is preserved but remains in an **unlocked (pending) state**, allowing for forensic analysis.
* **Best For**: Field tests and mission-critical logs where lead-up data is essential for debugging.

An example schematic rationale for deciding between the two policies can be:

| Scenario | Recommended Policy | Rationale |
| --- | --- | --- |
| **Edge/Field Tests** | `OnErrorPolicy.Report` | Forensic value: "Partial data is better than no data" for crash analysis. |
| **Automated CI/CD** | `OnErrorPolicy.Delete` | Platform hygiene: Prevents cluttering the catalog with junk data from failed runs. |
| **Ground Truth Generation** | `OnErrorPolicy.Delete` | Integrity: Ensures only 100% verified, complete sequences enter the database. |

### Topic-Level Error Handling

Because the `SequenceWriter` cannot natively distinguish which specific topic failed within your injection script or custom processing code (such as a coordinate transformations), an unhandled exception will bubble up and trigger the global sequence-level error policy. To avoid this, you should catch errors locally for each topic. It is highly recommended to wrap the topic-specific processing and pushing logic within a local `try-except` block, if a single failure is accepted and the entire sequence can still be accepted with partial data on failing topics. As an example, see the [How-Tos](../howto/serialized_writing_from_csv.md#topic-level-error-management)

Upcoming versions of the SDK will introduce native **Topic-Level Error Policies**, which will allow the user to define the error behavior directly when creating the topic, removing the need for boilerplate `try-except` blocks around every sensor stream.
