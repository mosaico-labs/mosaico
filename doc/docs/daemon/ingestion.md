# Ingestion 

Data ingestion in Mosaico is handled by the Flight `DoPut` streaming endpoint. 
This channel is explicitly engineered to handle write-heavy workloads, enabling the system to absorb high-bandwidth sensor data—such as 4K video streams or high-frequency Lidar point clouds—without contending with administrative traffic.

## The Ingestion Protocol

Data ingestion follows a structured protocol to ensure type safety and proper sequencing. The process begins with creating a new sequence using `sequence_create`, which takes a sequence name and optional user metadata, returning a unique sequence UUID.

Within this sequence, you create topics for each data stream via `topic_create`, associating them with the sequence UUID and assigning unique paths like `my_sequence/topic/1`. Each topic can also include its own metadata. For each topic, data is uploaded using the Flight `do_put` operation, starting with an Arrow schema for validation, followed by streaming RecordBatch payloads.

Once all topics are uploaded, the sequence is finalized with `sequence_finalize`, committing it to make the data immutable and queryable. During this process, the server validates schemas against registered ontologies, chunks data for efficient storage, and computes indices for fast querying.

```py
sq_uuid = sequence_create("my_sequence", metadata)
    
t1_uuid = topic_create(sq_uuid, "my_sequence/topic/1", metadata) # (1)!
do_put(t1_uuid, stream) # (2)!
    

sequence_finalize(sq_uuid) # (3)!
```

1. In this case we are 
2. ASasdasda
3. asdasd

## Chunking & Indexing Strategy

To manage massive datasets efficiently, the backend automatically handles *chunking*. As data flows in, `mosaicod` splits the continuous stream into optimal storage units called *chunks*.

For every chunk written, the server computes and stores *skip indices* in the metadata database containing ontology statistics i.e. type-specific metadata (e.g., coordinate bounding boxes for GPS, value ranges for sensors) that enables the query engine to perform content-based filtering without reading the bulk data.