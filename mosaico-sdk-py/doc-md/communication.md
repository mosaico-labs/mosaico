<div align="right">
<picture>
<source media="(prefers-color-scheme: light)" srcset="../../logo/mono_black.svg">
<source media="(prefers-color-scheme: dark)" srcset="../../logo/mono_white.svg">
<img alt="Mosaico logo" src="../../logo/color_white.svg" height="30">
</picture>
</div>

# Client Architecture

The Mosaico SDK is built on a high-performance, asynchronous architecture designed to handle high-throughput sensor data.  The `MosaicoClient` is the primary entry point for the SDK. It acts as a resource manager that orchestrates three distinct layers of communication and processing:

1.  **Control Plane (Synchronous):** A single, dedicated connection used for metadata operations like creating sequences, querying the catalog, and managing schema definitions.
2.  **Data Plane (Parallelized):** A `ConnectionPool` of multiple Flight clients. When uploading data, the SDK automatically stripes writes across these connections. This allows high-bandwidth streams (e.g., 4x 1080p cameras) to saturate the network bandwidth without being bottlenecked by a single TCP/gRPC window.
3.  **Processing Plane (Asynchronous):** An `ExecutorPool` of background threads. Heavy CPU tasks—specifically the serialization of Python objects into Apache Arrow buffers—are offloaded here. This ensures that while one thread is serializing the next batch of data, another thread is transmitting the previous batch over the network.

It is recommended to use the client in a `with` context to ensure resources are cleanly released.


### Connection Pooling
To maximize throughput, the client automatically initializes a pool of connections based on the host system's capabilities (typically matching `os.cpu_count()`).
* **Round-Robin Distribution:** Data batches are assigned to connections in a cycle.
* **Non-Blocking:** Writing to the network does not block the serialization of the next message.

### Executor Pooling
Serialization of complex sensor data (like images) can be CPU-intensive. The SDK maintains a pool of `ThreadPoolExecutor` instances.
Each executor acts as a dedicated "lane," ensuring that heavy serialization tasks do not block the main application thread or the control plane.


## API Reference

**Factory Method**
* **`connect(cls, host: str, port: int, timeout: int = 5) -> MosaicoClient`**
    Establishes the connection to the server and initializes all resource pools.
    * `host`: Server address.
    * `port`: Server port (default 6726).
    * `timeout`: Connection timeout in seconds.

**Context Manager**
* **`__enter__ / __exit__`**: Automatically closes connections and thread pools when leaving the block.

**Resource Factories**
* **`sequence_create(sequence_name: str, metadata: dict, ...) -> SequenceWriter`**
    Creates a [new writer](handlers.md#writing-data) for uploading data.
    * `sequence_name`: Unique identifier for the new recording.
    * `metadata`: Dictionary of tags (e.g., `{"robot": "spot", "location": "lab"}`).
    * `on_error`: Policy for handling write failures (`Delete` or `Report`).
* **`sequence_handler(sequence_name: str) -> SequenceHandler`**
    Retrieves a [handler](handlers.md#reading--handling-data) for an existing sequence (for reading metadata or streaming data). Caches the result to prevent redundant lookups.
* **`topic_handler(sequence_name: str, topic_name: str) -> TopicHandler`**
    Retrieves a [handler](handlers.md#reading--handling-data) for a specific topic within a sequence.

**Data Operations**
* **`query(*queries: QueryableProtocol) -> List[QueryResponseItem]`**
    Executes [queries](queries.md) against the data catalogs - Platform entities (i.e. Sequence or Topic) or Ontology catalog. Accepts `Query` builder objects.
* **`sequence_delete(sequence_name: str)`**
    Permanently removes a sequence and all its associated data from the server.

    > [!NOTE]
    > **Note on Immutability** 
    > 
    > Mosaico sequences are designed to be immutable once successfully committed (see also [Core Concepts](../../CORE_CONCEPTS.md#data-lifetime-and-integrity)). Therefore, deletion is strictly limited to **incomplete or malformed sequences** resulting from:
    > 1.  A write operation where the error policy was set to `Report` (leaving partial data).
    > 2.  An unexpected connection drop or system failure during the writing process, that instantly closes the communication with the server.
    >     
    > In these specific "unlocked" states, the sequence can be cleaned up. Attempting to delete a successfully committed (locked) sequence is forbidden and will result in an error.

**Lifecycle**
* **`close()`**
    Manually shuts down all pools and connections. Called automatically by the context manager (if the instance was created in a `with` block).
