# Client

API Reference: [`mosaicolabs.comm.MosaicoClient`][mosaicolabs.comm.MosaicoClient].

The `MosaicoClient` is a resource manager designed to orchestrate three distinct **Layers** of communication and processing. 
This layered architecture ensures that high-throughput sensor data does not block critical control operations or application logic.


## Control Layer

A single, dedicated connection is maintained for metadata operations. 
This layer handles lightweight tasks such as creating sequences, querying the catalog, and managing schema definitions. 
By isolating control traffic, the client ensures that critical commands (like `sequence_finalize`) are never queued behind heavy data transfers.

## Data Layer

For high-bandwidth data ingestion (e.g., uploading 4x 1080p cameras simultaneously), the client maintains a **Connection Pool** of multiple Flight clients. 
The SDK automatically stripes writes across these connections in a round-robin fashion, allowing the application to saturate the available network bandwidth.

## Processing Layer

Serialization of complex sensor data (like compressing images or encoding LIDAR point clouds) is CPU-intensive. 
The SDK uses an **Executor Pool** of background threads to offload these tasks. 
This ensures that while one thread is serializing the *next* batch of data, another thread is already transmitting the *previous* batch over the network.

**Best Practice:** It is recomended to always use the client inside a `with` context to ensure resources in all layers are cleanly released.

```python
from mosaicolabs import MosaicoClient

with MosaicoClient.connect("localhost", 6726) as client:
    # Logic goes here
    pass
# Pools and connections are closed automatically

```

