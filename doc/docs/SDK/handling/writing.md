---
title: The Writing Workflow
description: Data Writers.
---

The **Writing Workflow** in Mosaico is designed for high-throughput data ingestion, ensuring that your application remains responsive even when streaming high-bandwidth sensor data like 4K video or high-frequency IMU telemetry.

The architecture is built around a **"Multi-Lane"** approach, where each sensor stream operates in its own isolated lane with dedicated system resources.

### The Orchestrator: `SequenceWriter`

The `SequenceWriter` acts as the central controller for a recording session. It manages the high-level lifecycle of the data on the server and serves as the factory for individual sensor streams.

**Key Roles:**

* **Lifecycle Management**: It handles the lifecycle of a new sequence and ensures that it is either successfully committed as immutable data or, in the event of a failure, cleaned up according to your configured `OnErrorPolicy`.
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
            "location": {
                "city": "Milan",
                "country": "IT",
                "facility": "Downtown",
                "gps": {
                    "lat": 45.46481,
                    "lon": 9.19201,
                },
            },
        }
        on_error = OnErrorPolicy.Delete # Default
        ) as seq_writer:

        # `seq_writer` is the writing handler of the new 'mission_log_042' sequence
        # Data will be uploaded by spawning topic writers that will manage the actual data stream 
        # remote push... See below.

```

1. The metadata fields will be queryable via the [`Query` mechanism](../query.md). The mechanism allows creating queries like: `Sequence.Q.user_metadata["vehicle.software_stack.planning"].match("plan-4.")`

API Reference: [`mosaicolabs.handlers.SequenceWriter`][mosaicolabs.handlers.SequenceWriter].

### The Data Engine: `TopicWriter`

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
        # Usage Mode A: Component-based
        imu_writer.push(
            ontology_obj=IMU(acceleration=Vector3d(x=0, y=0, z=9.81), ...),
            message_timestamp_ns=1700000000000
        )

        # Usage Mode B: Full Message-based
        gps_msg = Message(timestamp_ns=1700000000100, data=GPS(...))
        gps_writer.push(message=gps_msg)

# Exiting the block automatically flushes all topic buffers, finalizes the sequence on the server 
# and closes all connections and pools
```

1. The metadata fields will be queryable via the [`Query` mechanism](../query.md). The mechanism allows creating query expressions like: `Topic.Q.user_metadata["interface.type"].eq("UART")`.
    API Reference:
    * [`mosaicolabs.models.platform.Topic`][mosaicolabs.models.platform.Topic]
    * [`mosaicolabs.models.query.builders.QueryTopic`][mosaicolabs.models.query.builders.QueryTopic].

API Reference: [`mosaicolabs.handlers.TopicWriter`][mosaicolabs.handlers.TopicWriter].

### Resilient Data Ingestion & Error Management

Recording high-bandwidth sensor data in dynamic environments requires a tiered approach to error handling. While the Mosaico SDK provides automated recovery through **Error Policies**, these act as a "last line of defense". For robust production pipelines, you must implement **Defensive Ingestion Patterns** to prevent isolated failures from compromising your entire recording session.

### Tier 1: *Last-Resort* Automated Error Policies

Configured via `WriterConfig`, these policies dictate how the server handles a sequence if an unhandled exception bubbles up to the `SequenceWriter` context manager.

#### 1. `OnErrorPolicy.Delete` (The "Clean Slate" Policy)

* **Behavior**: If an error occurs, the SDK sends an `ABORT` signal to the server.
* **Result**: The server immediately deletes the entire sequence and all associated topic data.
* **Best For**: CI/CD pipelines, unit testing, or "Gold Dataset" generation where partial or corrupted logs are unacceptable.

#### 2. `OnErrorPolicy.Report` (The "Recovery" Policy)

* **Behavior**: The SDK finalizes data that successfully reached the server and sends a `NOTIFY_CREATE` signal with error details.
* **Result**: The sequence is preserved but remains in an **unlocked (pending) state**, allowing for forensic analysis.
* **Best For**: Field tests and mission-critical logs where lead-up data is essential for debugging.

### Tier 2: Defensive Ingestion Patterns (The "Golden Rule")

**The Golden Rule**: Always wrap data transformation and custom processing logic in local `try-except` blocks before calling `.push()`.

The SDK cannot distinguish which specific topic failed within your custom code; it only knows that an exception occurred within the `SequenceWriter` block and will trigger the global Error Policy accordingly. Without local protection, a single failing data point will terminate the entire injection process.

#### 1. Surgical Topic Finalization

If a specific sensor fails or a transformation function bugs out, you can catch the error locally. This allows you to surgically close the failing topic while letting other healthy streams complete their job.

* **Approach**: Catch the exception, call `finalize(error=e)` on the failing `TopicWriter`, and continue the loop.

```python
with client.sequence_create(name="resilient_mission") as seq_writer:
    cam_writer = seq_writer.topic_create(name="camera", ontology_type=CompressedImage)

    for data in sensor_source:

        # Protect Risky Processing
        try:
            # Always protect conversion/processing before the push happens
            processed_img = risky_image_transformation(data.raw_frame)
            cam_writer.push(ontology_obj=processed_img)
        except Exception as topic_e:
            # Topic failure: Report and continue
            print(f"Camera failure: {topic_e}. Continuing other topics.")
            # Optionally finalize the topic to prevent further pushes
            # e.g. if the topic is not critical for the mission
            if not cam_writer.finalized():
                cam_writer.finalize(error=topic_e) # Reports error to server

```

#### 2. Strategic Policy Selection

Align your `OnErrorPolicy` with your operational environment to balance data integrity against recovery needs:

| Scenario | Recommended Policy | Rationale |
| --- | --- | --- |
| **Edge/Field Tests** | `OnErrorPolicy.Report` | Forensic value: "Partial data is better than no data" for crash analysis. |
| **Automated CI/CD** | `OnErrorPolicy.Delete` | Platform hygiene: Prevents cluttering the catalog with junk data from failed runs. |
| **Ground Truth Generation** | `OnErrorPolicy.Delete` | Integrity: Ensures only 100% verified, complete sequences enter the database. |

---

### Summary of Resilience

By implementing local error handling, you shift the `SequenceWriter` error policies from being the "default" failure mode to being a **last-resort recovery** for systemic issues (like total network failure). This ensures that minor software bugs do not result in the total loss of valuable multi-sensor datasets.
