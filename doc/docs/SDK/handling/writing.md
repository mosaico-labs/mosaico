---
title: The Writing Workflow
description: Data Writers.
---

The **Writing Workflow** in Mosaico is designed for high-throughput data ingestion, ensuring that your application remains responsive even when streaming high-bandwidth sensor data like 4K video or high-frequency IMU telemetry.

The architecture is built around a **"Multi-Lane"** approach, where each sensor stream operates in its own isolated lane with dedicated system resources.

## `SequenceWriter`
API Reference: [`mosaicolabs.handlers.SequenceWriter`][mosaicolabs.handlers.SequenceWriter].

The `SequenceWriter` acts as the central controller for a recording session. It manages the high-level lifecycle of the data on the server and serves as the factory for individual sensor streams. 

Spawning a new sequence writer is done via the [`MosaicoClient.connect()`][mosaicolabs.comm.MosaicoClient.connect] factory method.

**Key Roles:**

* **Lifecycle Management**: It handles the lifecycle of a new sequence resource and related writing Session, ensuring that it is either successfully committed as immutable data. In the event of a failure, the sequence and the written data are handled according to the configured [`OnErrorPolicy`][mosaicolabs.enum.OnErrorPolicy].
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
        on_error = OnErrorPolicy.Delete
        ) as seq_writer:

        # `seq_writer` is the writing handler of the new 'mission_log_042' sequence
        # Data will be uploaded by spawning topic writers that will manage
        # the actual data stream push... See below.

```

1. The metadata fields will be queryable via the [`Query` mechanism](../query.md). The mechanism allows creating queries like: `Sequence.Q.user_metadata["vehicle.software_stack.planning"].match("plan-4.")`

### Sequence-Level Error Handling
API Reference: [`mosaicolabs.enum.OnErrorPolicy`][mosaicolabs.enum.OnErrorPolicy].

Configured when instantiating a new [`SequenceWriter`][mosaicolabs.handlers.SequenceWriter] via the `on_error` parameter, these policies dictate how the server handles a sequence if an unhandled exception bubbles up to the `SequenceWriter` context manager. By default, this policy is set to [`OnErrorPolicy.Report`][mosaicolabs.enum.OnErrorPolicy.Report], which means an error notification is sent to the server, allowing the platform to flag the sequence as failed while retaining whatever records were successfully transmitted before the error occurred. Alternatively, the [`OnErrorPolicy.Delete`][mosaicolabs.enum.OnErrorPolicy.Delete] policy will signal the server to physically remove the incomplete sequence and its associated topic directories, if any errors occurred.

An example schematic rationale for deciding between the two policies can be:

| Scenario | Recommended Policy | Rationale |
| --- | --- | --- |
| **Edge/Field Tests** | `OnErrorPolicy.Report` | Forensic value: "Partial data is better than no data" for crash analysis. |
| **Automated CI/CD** | `OnErrorPolicy.Delete` | Platform hygiene: Prevents cluttering the catalog with junk data from failed runs. |
| **Ground Truth Generation** | `OnErrorPolicy.Delete` | Integrity: Ensures only 100% verified, complete sequences enter the database. |

## `TopicWriter`
API Reference: [`mosaicolabs.handlers.TopicWriter`][mosaicolabs.handlers.TopicWriter].

Once a topic is created via [`mosaicolabs.handlers.SequenceWriter.topic_create`][mosaicolabs.handlers.SequenceWriter.topic_create], a `TopicWriter` is spawned to handle the actual transmission of data for that specific stream. It abstracts the underlying networking protocols, allowing you to simply "push" Python objects while it handles the heavy lifting.

**Key Roles:**

* **Smart Buffering**: Instead of sending every single message over the network—which would be highly inefficient—the `TopicWriter` accumulates records in a memory buffer.
* **Automated Flushing**: The writer automatically triggers a "flush" to the server whenever the internal buffer exceeds your configured limits, such as a maximum byte size or a specific number of records.
* **Asynchronous Serialization**: For CPU-intensive data (like encoding images), the writer can offload the serialization process to background threads, ensuring your main application loop stays fast.

```python
# Continues from the code above...

    # with client.sequence_create(...) as seq_writer:
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

### Topic-Level Error Handling

Because the `SequenceWriter` cannot natively distinguish which specific topic failed within your injection script or custom processing code (such as a coordinate transformations), an unhandled exception will bubble up and trigger the global sequence-level error policy. To avoid this, you should catch errors locally for each topic. It is highly recommended to wrap the topic-specific processing and pushing logic within a local `try-except` block, if a single failure is accepted and the entire sequence can still be accepted with partial data on failing topics. As an example, see the [How-Tos](../howto/serialized_writing_from_csv.md#topic-level-error-management)

Upcoming versions of the SDK will introduce native **Topic-Level Error Policies**, which will allow the user to define the error behavior directly when creating the topic, removing the need for boilerplate `try-except` blocks around every sensor stream.


## `SequenceUpdater`
API Reference: [`mosaicolabs.handlers.SequenceUpdater`][mosaicolabs.handlers.SequenceUpdater].

The `SequenceUpdater` is used to update an existing sequence on the server. Updating a sequence means adding new topics only, by opening a new writing Session. The `SequenceUpdater` cannot be used to update the metadata of a sequence or its existing topics.

Spawning a new sequence updater is done via the [`SequenceHandler.update()`][mosaicolabs.handlers.SequenceHandler.update] factory method.

**Key Roles:**

* **Lifecycle Management**: It handles the lifecycle of a new writing Session on an existing sequence and ensures that it is either successfully committed as immutable data or, in the event of a failure, cleaned up according to the configured [`OnErrorPolicy`][mosaicolabs.enum.OnErrorPolicy].
* **Resource Distribution**: The writer pulls network connections from the **Connection Pool** and background threads from the **Executor Pool**, assigning them to individual topics. This isolation prevents a slow network connection on one topic from bottlenecking others.
* **Context Safety**: To ensure data integrity, the `SequenceUpdater` must be used within a Python `with` block. This guarantees that all buffers are flushed and the writing Session is closed properly, even if your application crashes.

```python
from mosaicolabs import MosaicoClient, OnErrorPolicy

# Open the connection with the Mosaico Client
with MosaicoClient.connect("localhost", 6726) as client:
    # Get the handler for the sequence
    seq_handler = client.sequence_handler("mission_log_042")
    # Update the sequence
    with seq_handler.update(
        on_error = OnErrorPolicy.Delete # Relative to this session only
        ) as seq_updater:
            # Start creating topics and pushing data
            
```

!!! note "Session-level Error Handling"
    Configured when instantiating a new [`SequenceUpdater`][mosaicolabs.handlers.SequenceUpdater] via the `on_error` parameter, the `OnErrorPolicy` policy dictates how the server handles the new writing Session if an unhandled exception bubbles up to the `SequenceUpdater` context manager. The [very same semantics](#sequence-level-error-handling) as the `SequenceWriter` apply. These policies are relative to the **current writing Session only**: the data already stored in the sequence with previous sessions is not affected and are kept as immutable data.

Once obtained, the `SequenceUpdater` can be used to create new topics and push data to them, in the very same way as a explained in the [`TopicWriter` section](#topicwriter).

```python
# Continues from the code above...

    # seq_handler.update(...) as seq_updater:
        # Create individual Topic Writers
        # Each writer gets its own assigned resources from the pools
        imu_writer = seq_updater.topic_create(...)

        # Push data - The SDK handles batching and background I/O
        imu_writer.push(...)

# Exiting the block automatically flushes all topic buffers, finalizes the sequence on the server 
# and closes all connections and pools
```
