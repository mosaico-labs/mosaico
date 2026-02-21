# Ingestion 

Data ingestion in Mosaico is handled by the Flight `DoPut` streaming endpoint. 
This channel is explicitly engineered to handle write-heavy workloads, enabling the system to absorb high-bandwidth sensor data, such as 4K video streams or high-frequency Lidar point clouds—without contending with administrative traffic.

## The Ingestion Protocol

Data ingestion follows a structured protocol to ensure type safety and proper sequencing. The process begins with creating a new sequence using `sequence_create`, which takes a sequence name and optional user metadata, returning a unique sequence UUID.

Within this sequence, you create topics for each data stream via `topic_create`, associating them with the sequence UUID and assigning unique paths like `my_sequence/topic/1`. Each topic can also include its own metadata. For each topic, data is uploaded using the Flight `do_put` operation, starting with an Arrow schema for validation, followed by streaming `RecordBatch` payloads.

Once all topics are uploaded, the sequence is finalized with `sequence_finalize`, committing it to make the data immutable and queryable. During this process, the server validates schemas against registered ontologies, chunks data for efficient storage, and computes indices for fast querying.

```py title="Ingestion protocol in pseudo-code"
sq_uuid = do_action<sequence_create>("my_sequence", metadata)

# Create topic and upload data
t1_uuid = do_action<topic_create>(sq_uuid, "my_sequence/topic/1", metadata) # (1)!
do_put(t1_uuid, data_stream) 
    
do_action<sequence_finalize>(sq_uuid) # (2)!
```

1. The `topic_create` action returns a UUID that must be passed to the `do_put` call.
2. During finalization, all resources are consolidated and locked. Alternatively, you can call [`sequence_abort(sq_uuid)`](actions.md#sequence-management).

!!! note "Why UUIDs?"
    UUIDs are employed in the ingestion protocol to prevent contentious uploads of the same resources. For instance, if two users attempt to create a new resource (such as a sequence or topic) with the same name, only one will succeed and receive a UUID. This UUID is then used in subsequent calls to ensure that operations are performed by the user who successfully created the resource.

## Chunking & Indexing Strategy

The backend automatically manages *chunking* to efficiently handle intra-sequence queries and prevent memory overload from streaming data. As data streams in, the server buffers the incoming data until a full chunk is accumulated, then writes it to disk as an optimal storage unit called a *chunk*.

For each chunk written to disk, the server calculates and stores *skip indices* in the metadata database. These indices include ontology-specific statistics, such as type-specific metadata (e.g., coordinate bounding boxes for GPS data or value ranges for sensors). This allows the query engine to perform content-based filtering without needing to read the entire bulk data.