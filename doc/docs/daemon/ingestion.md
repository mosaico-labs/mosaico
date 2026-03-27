# Ingestion 

Data ingestion in Mosaico is explicitly engineered to handle write-heavy workloads, enabling the system to absorb high-bandwidth sensor data, such as 4K video streams or high-frequency Lidar point clouds, without contending with administrative traffic.

## Sessions

We have intentionally moved away from traditional versioning. While versioning is a common pattern, it often introduces significant architectural complexity and degrades the developer experience when querying historical data. 
For instance, if multiple versions of a single sequence exist, the query engine faces an ambiguous choice: should it return the latest state, the most stable state, or a specific point-in-time snapshot? 

Consider a scenario where you are tracking a temperature sensor. If you have versioned sequences, a query for a given temperature becomes problematic: do you search across Version 1.2 or Version 2.0? Has the data changed across versions?

Mosaico eliminates this ambiguity by focusing on **Sessions** rather than versions.

A session represents an immutable **data layer** within Mosaico. 
It acts as a logical container for topics that are uploaded together as a single unit. 

To visualize the hierarchy, think of a sequence as the primary data container. 
Sessions then represent specific, immutable layers of data within that sequence, while topics serve as a specific instance of an ontology model.

This design allows for high-concurrency environments; you can upload multiple sessions simultaneously without the risk of data races or state corruption. 

Sessions are the primary mechanism used to update and evolve sequences. 
This model is particularly powerful for parallel workloads that need to contribute data to the same base layer. 
Because sessions are independent and immutable, separate workloads can operate in isolation. 
For example, one process might be cleaning a dataset while another is appending real-time logs; both can work based on the same base data layer and commit their results as independent sessions without interfering with one another.

### An example

To illustrate the session workflow in practice, imagine you are managing a dataset for autonomous driving. 

When you first initialize a sequence, you perform a bulk upload of raw sensor data collected during a drive. 
This initial upload constitutes your first session, containing the base telemetry and camera streams:

```text
[ SESSION: BASE DATA ] 
topolino_amaranto_25032026/camera/front
topolino_amaranto_25032026/camera/back
topolino_amaranto_25032026/car/gps
topolino_amaranto_25032026/car/imu
```

Once this base layer is established, you can trigger two independent processing workloads. 
The first workload focuses on computer vision, analyzing the front and back camera streams to generate object detection labels. 
Simultaneously, a second workload runs a visual odometry algorithm, fusing data from the cameras, IMU, and GPS to calculate the precise trajectory of the vehicle.

Because Mosaico supports concurrent sessions, these two tasks do not need to wait for one another or risk overwriting the base data. 
The vision task commits its results as one session, and the odometry task commits its results as another. 

The final state of your sequence is now an enriched, multi-layered data container. 
When you query this sequence, the engine provides a unified view that includes the original raw sensors plus the newly generated analytical topics:

```text
[ SESSION: BASE DATA ]
topolino_amaranto_25032026/camera/front
topolino_amaranto_25032026/camera/back
topolino_amaranto_25032026/car/gps
topolino_amaranto_25032026/car/imu

[ SESSION: LABELING DATA ]
topolino_amaranto_25032026/labels/object_detection

[ SESSION: VISUAL ODOMETRY ]
topolino_amaranto_25032026/tracks/visual_odometry
```

This approach allows your data to grow *horizontally* with new topics while maintaining the absolute immutability of the original sensor readings.

## Ingestion Protocol

The data ingestion protocol in Mosaico follows a structured, multi-step flow designed to ensure type safety and prevent race conditions in high-concurrency environments. 

The process begins with `sequence_create`, which establishes the primary data container. 
Because Mosaico uses a session-based update model, you must then initialize a specific *data layer* using `session_create`, returning a session UUID that serves as the active context for all subsequent uploads within that specific batch.
Within this active session, you define individual data streams via `topic_create`, where each topic is assigned a unique path (e.g., `my_sequence/topic/1`) and returns its own topic UUID. 
Data is then transmitted using the Arrow Flight `do_put` operation, starting with an Arrow schema for structural validation and followed by a stream of `RecordBatch` payloads. 
By passing the topic UUID to `do_put`, the system ensures the incoming binary stream is mapped correctly to the intended topic and session layer. 

Once all topics and their respective data streams are uploaded, the session must be formally committed with `session_finalize`. 
This action triggers server-side validation against registered ontologies, chunks the data for efficient storage, and locks the session to make the data permanent and available for downstream queries.

Here is a simplified example of the ingestion flow using a pyhton-like pseudocode:

```py title="Ingestion protocol"
# Initialize the Sequence and the Session layer
sequence_create("my_sequence", metadata)
ss_uuid = session_create("my_sequence")

# Create topics within the session and stream data
t1_uuid = topic_create(ss_uuid, "my_sequence/topic/1", metadata) # (1)!
do_put(t1_uuid, data_stream) 

t2_uuid = topic_create(ss_uuid, "my_sequence/topic/2", metadata)
do_put(t2_uuid, data_stream)

# Commit the session to make data immutable
session_finalize(ss_uuid) # (2)!
```

1.  The `topic_create` action returns a UUID that must be passed to the `do_put` call to route the data stream correctly.
2.  During finalization, all resources are consolidated and locked. Alternatively, you can call [`session_delete(ss_uuid)`](actions.md#session-management) to discard the upload. Or call [`sequence_delete(sq_uuid)`](actions.md#sequence-management) to discard the entire sequence if you want to start over.

??? note "Why UUIDs?"
    UUIDs are employed throughout the protocol to prevent contentious uploads. For instance, if two users attempt to create a resource with the same name simultaneously, the system ensures only one successfully receives a UUID. This identifier acts as a "token of authority," ensuring that subsequent `do_put` or `finalize` operations are performed only by the user who initiated the resource.

    UUID are available to clients only during the `*_create` actions. Possessing a UUID is a prerequisite for performing any further operations on that resource, such as uploading data or finalizing the session. This design choice ensures that only the creator of a resource can modify it.

### Updating a Sequence

To extend a sequence with new data, you follow a nearly identical flow to the initial ingestion, but you omit the `sequence_create`. 

This process leverages Mosaico's session model to layer new information onto the established sequence without modifying the original data. 

Here is a simplified example of the update flow using a pyhton-like pseudocode:

```py title="Extending a sequence with new data"
# Initialize a NEW Session layer for the existing sequence
ss_uuid = session_create("my_sequence")

# Create new topics (e.g., processed labels) within this session
t3_uuid = topic_create(ss_uuid, "my_sequence/labels/object_detection", metadata) # (1)!
do_put(t3_uuid, processed_data_stream) 

# Commit the new session to merge the layer into the sequence
session_finalize(ss_uuid)
```

1.  Even though the sequence already exists, the `topic_create` call within a new session ensures this specific data stream is tracked as a new, immutable contribution.

??? tip "Concurrency"
    Because each upload is encapsulated in its own session, multiple workers can extend the same sequence simultaneously. 
    Mosaico handles the isolation of these sessions, ensuring that `session_finalize` only commits the specific topics and data associated with that worker's session UUID.

### Aborting an Upload

If you encounter an error *during the ingestion process* or simply want to discard all the data just uploaded, there is a straightforward way to abort the sequence.

Here is a simplified example of the abort flow using a pyhton-like pseudocode:

```py title="Aborting an upload", hl_lines="13 17"
# Initialize the Sequence and the Session layer
sequence_create("my_sequence", metadata)
ss_uuid = session_create("my_sequence")

# Create topics within the session and stream data
t1_uuid = topic_create(ss_uuid, "my_sequence/topic/1", metadata) # (1)!
do_put(t1_uuid, data_stream) 

t2_uuid = topic_create(ss_uuid, "my_sequence/topic/2", metadata)
do_put(t2_uuid, data_stream)

# Delete the session to discard all uploaded topics data
session_delete(ss_uuid)

# You can delete the dangling sequence since there are no data associated
# or avoid deletion and start a new session to upload new data with a `session_create`
sequence_delete()
```

???warning "Permissions" 
    If **API key management** is enabled, the `sequence_delete` and `session_delete` actions require a key with at least `delete` privileges.

## Chunking & Indexing Strategy

The backend automatically manages *chunking* to efficiently handle intra-sequence queries and prevent memory overload from ingesting large data streams. 
As data streams in, the server buffers the incoming data until a full chunk is accumulated, then writes it to disk as an optimal storage unit called a *chunk*. 

???tip "Configuring Chunk Size"
    The chunk size is configurable via the `MOSAICOD_MAX_CHUNK_SIZE_IN_BYTES` environment variable. Setting this value to `0` disables automatic chunking, allowing for unlimited chunk sizes. However, it is generally recommended to set a reasonable chunk size to balance memory usage and query performance. See the [environment variables](env.md#general) section for more details.

For each chunk written to disk, the server calculates and stores *skip indices* in the metadata database. These indices include ontology-specific statistics, such as type-specific metadata (e.g., coordinate bounding boxes for GPS data or value ranges for sensors). This allows the query engine to perform content-based filtering without needing to read the entire bulk data.
