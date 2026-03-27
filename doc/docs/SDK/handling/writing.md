---
title: The Writing Workflow
description: Data Writers.
---

The **Writing Workflow** in Mosaico is designed for high-throughput data ingestion, ensuring that your application remains responsive even when streaming high-bandwidth sensor data like 4K video or high-frequency IMU telemetry.

The architecture is built around a **"Multi-Lane"** approach, where each sensor stream operates in its own isolated lane with dedicated system resources.

!!! info "API-Keys"
    When the connection is established via the authorization middleware (i.e. using an [API-Key](../client.md#2-authentication-api-key)), the writing workflow is allowed only if the key has at least [`APIKeyPermissionEnum.Write`][mosaicolabs.enum.APIKeyPermissionEnum.Write] permission.


## `SequenceWriter`
API Reference: [`mosaicolabs.handlers.SequenceWriter`][mosaicolabs.handlers.SequenceWriter].

The `SequenceWriter` acts as the central controller for a recording session. It manages the high-level lifecycle of the data on the server and serves as the factory for individual sensor streams. 

Spawning a new sequence writer is done via the [`MosaicoClient.connect()`][mosaicolabs.comm.MosaicoClient.connect] factory method.

**Key Roles:**

* **Lifecycle Management**: It handles the lifecycle of a new sequence resource and related writing Session, ensuring that it is either successfully committed as immutable data. In the event of a failure, the sequence and the written data are handled according to the configured [`SessionLevelErrorPolicy`][mosaicolabs.enum.SessionLevelErrorPolicy].
* **Resource Distribution**: The writer pulls network connections from the **Connection Pool** and background threads from the **Executor Pool**, assigning them to individual topics. This isolation prevents a slow network connection on one topic from bottlenecking others.
* **Context Safety**: To ensure data integrity, the `SequenceWriter` must be used within a Python `with` block. This guarantees that all buffers are flushed and the sequence is closed properly, even if your application crashes.

```python
from mosaicolabs import MosaicoClient, SessionLevelErrorPolicy, TopicLevelErrorPolicy

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
        on_error = SessionLevelErrorPolicy.Delete
        ) as seq_writer:

        # `seq_writer` is the writing handler of the new 'mission_log_042' sequence
        # Data will be uploaded by spawning topic writers that will manage
        # the actual data stream push... See below.

```

1. The metadata fields will be queryable via the [`Query` mechanism](../query.md). The mechanism allows creating queries like: `QuerySequence().with_user_metadata("vehicle.software_stack.planning", eq="plan-4.1.7")`

### Sequence-Level Error Handling
API Reference: [`mosaicolabs.enum.SessionLevelErrorPolicy`][mosaicolabs.enum.SessionLevelErrorPolicy].

??? warning "Deprecated OnErrorPolicy"
    In release 0.3.0, the [`OnErrorPolicy`][mosaicolabs.enum.OnErrorPolicy] is declared **`deprecated`** in favor of the [`SessionLevelErrorPolicy`][mosaicolabs.enum.SessionLevelErrorPolicy]. The support for the class will be removed in the release 0.4.0. No changes are made to the enum values of the new class `SessionLevelErrorPolicy`, which are identical to the ones of the deprecated class.    

Configured when instantiating a new [`SequenceWriter`][mosaicolabs.handlers.SequenceWriter] via the `on_error` parameter, these policies dictate how the server handles a sequence if an unhandled exception bubbles up to the `SequenceWriter` context manager. By default, this policy is set to [`SessionLevelErrorPolicy.Report`][mosaicolabs.enum.SessionLevelErrorPolicy.Report], which means an error notification is sent to the server, allowing the platform to flag the sequence as failed while retaining whatever records were successfully transmitted before the error occurred. Alternatively, the [`SessionLevelErrorPolicy.Delete`][mosaicolabs.enum.SessionLevelErrorPolicy.Delete] policy will signal the server to physically remove the incomplete sequence and its associated topic directories, if any errors occurred.

!!! info "Error Handling and API-Key"
    When the connection is established via the authorization middleware (i.e. using an [API-Key](../client.md#2-authentication-api-key)), the [`SessionLevelErrorPolicy.Delete`][mosaicolabs.enum.SessionLevelErrorPolicy.Delete] policy is successfully executed by the server only if the API-Key has [`APIKeyPermissionEnum.Delete`][mosaicolabs.enum.APIKeyPermissionEnum.Delete] permission. If this is not the case, the server will raise an error and the current writing Session will remain in an unlocked state.


An example schematic rationale for deciding between the two policies can be:

| Scenario | Recommended Policy | Rationale |
| --- | --- | --- |
| **Edge/Field Tests** | `SessionLevelErrorPolicy.Report` | Forensic value: "Partial data is better than no data" for crash analysis. |
| **Automated CI/CD** | `SessionLevelErrorPolicy.Delete` | Platform hygiene: Prevents cluttering the catalog with junk data from failed runs. |
| **Ground Truth Generation** | `SessionLevelErrorPolicy.Delete` | Integrity: Ensures only 100% verified, complete sequences enter the database. |

## `TopicWriter`
API Reference: [`mosaicolabs.handlers.TopicWriter`][mosaicolabs.handlers.TopicWriter].

Once a topic is created via [`SequenceWriter.topic_create`][mosaicolabs.handlers.SequenceWriter.topic_create], a `TopicWriter` is spawned to handle the actual transmission of data for that specific stream. It abstracts the underlying networking protocols, allowing you to simply "push" Python objects while it handles the heavy lifting.

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
            error_policy=TopicLevelErrorPolicy.Raise, # Raises an exception if an error occurs
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
            error_policy=TopicLevelErrorPolicy.Ignore, # Ignore errors in this topic
            ontology_type=GPS, # The ontology type stored in this topic
        )

        # Suppose we have a stream of IMU data, e.g. from a file
        # Push data in a controlled context - The SDK handles batching and background I/O
        for imu_data in imu_data_stream:
            with imu_writer: # Protect the execution
                imu_data = process_imu(imu_data) # This code may raise an exception: will re-raise
                imu_writer.push(
                    message=Message(
                        timestamp_ns=imu_data.timestamp_ns, 
                        data=imu_data,
                    )
                )

        for gps_data in gps_data_stream:
            with gps_writer: # Protect the execution
                gps_data = process_gps(gps_data) # This code may raise an exception: will be ignored
                gps_writer.push(
                    message=Message(
                        timestamp_ns=gps_data.timestamp_ns, 
                        data=gps_data,
                    )
                )

# Exiting the block automatically flushes all topic buffers, finalizes the sequence on the server 
# and closes all connections and pools
```

1. The metadata fields will be queryable via the [`Query` mechanism](../query.md). The mechanism allows creating query expressions like: `QueryTopic().with_user_metadata("interface.type", eq="UART")`.
    API Reference:
    * [`mosaicolabs.models.platform.Topic`][mosaicolabs.models.platform.Topic]
    * [`mosaicolabs.models.query.builders.QueryTopic`][mosaicolabs.models.query.builders.QueryTopic].


### Topic-Level Error Handling

By default, the `SequenceWriter` context manager cannot natively distinguish which specific topic failed during custom processing or data pushing. An unhandled exception in one stream will bubble up and trigger the global **Sequence-Level Error Policy**, potentially aborting the entire upload. To prevent this, the SDK introduces native **Topic-Level Error Policies**, which automate the "Defensive Ingestion" pattern directly within the `TopicWriter`. This pattern is highly recommended, in paerticular for complex ingestion pipelines (see for example the [interleaved ingestion how-to](../howto/interleaved_writing_from_multi_topics.md)).

!!! note:
    The error handling is only possible inside the `TopicWriter` context manager, i.e. by wrapping the processing and pushing code inside a `with topic_writer:` block.

When creating a topic via the `SequenceWriter.topic_create` function, users can specify a [`TopicLevelErrorPolicy`][mosaicolabs.enum.TopicLevelErrorPolicy] that isolates failures to that specific data "lane". This ensures that a single malformed message or transformation error does not compromise the high-level sequence. By defining these behaviors at the configuration level, the user can eliminate the need for boilerplate error-handling code around every topic writer context. The `TopicLevelErrorPolicy` can be set to:

* `TopicLevelErrorPolicy.Raise`: (Default) Raises an exception if an error occurs; this returns the error handling to the `SequenceWriter.on_error` policy.
* `TopicLevelErrorPolicy.Ignore`: Reports the error to the server and continues the ingestion process.
* `TopicLevelErrorPolicy.Finalize`: Reports the error to the server and finalizes the topic, but does not interrupt the ingestion process.

## `SequenceUpdater`
API Reference: [`mosaicolabs.handlers.SequenceUpdater`][mosaicolabs.handlers.SequenceUpdater].

The `SequenceUpdater` is used to update an existing sequence on the server. Updating a sequence means adding new topics only, by opening a new writing Session. The `SequenceUpdater` cannot be used to update the metadata of a sequence or its existing topics.

Spawning a new sequence updater is done via the [`SequenceHandler.update()`][mosaicolabs.handlers.SequenceHandler.update] factory method.

**Key Roles:**

* **Lifecycle Management**: It handles the lifecycle of a new writing Session on an existing sequence and ensures that it is either successfully committed as immutable data or, in the event of a failure, cleaned up according to the configured [`SessionLevelErrorPolicy`][mosaicolabs.enum.SessionLevelErrorPolicy].
* **Resource Distribution**: The writer pulls network connections from the **Connection Pool** and background threads from the **Executor Pool**, assigning them to individual topics. This isolation prevents a slow network connection on one topic from bottlenecking others.
* **Context Safety**: To ensure data integrity, the `SequenceUpdater` must be used within a Python `with` block. This guarantees that all buffers are flushed and the writing Session is closed properly, even if your application crashes.

```python
from mosaicolabs import MosaicoClient, SessionLevelErrorPolicy

# Open the connection with the Mosaico Client
with MosaicoClient.connect("localhost", 6726) as client:
    # Get the handler for the sequence
    seq_handler = client.sequence_handler("mission_log_042")
    # Update the sequence
    with seq_handler.update(
        on_error = SessionLevelErrorPolicy.Delete # Relative to this session only
        ) as seq_updater:
            # Start creating topics and pushing data
            
```

!!! note "Session-level Error Handling"
    Configured when instantiating a new [`SequenceUpdater`][mosaicolabs.handlers.SequenceUpdater] via the `on_error` parameter, the `SessionLevelErrorPolicy` policy dictates how the server handles the new writing Session if an unhandled exception bubbles up to the `SequenceUpdater` context manager. The [very same semantics](#sequence-level-error-handling) as the `SequenceWriter` apply. These policies are relative to the **current writing Session only**: the data already stored in the sequence with previous sessions is not affected and are kept as immutable data.

!!! info "Error Handling and API-Key"
    When the connection is established via the authorization middleware (i.e. using an [API-Key](../client.md#2-authentication-api-key)), the [`SessionLevelErrorPolicy.Delete`][mosaicolabs.enum.SessionLevelErrorPolicy.Delete] policy is successfully executed by the server only if the API-Key has [`APIKeyPermissionEnum.Delete`][mosaicolabs.enum.APIKeyPermissionEnum.Delete] permission. If this is not the case, the server will raise an error and the current writing Session will remain in an unlocked state.

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
