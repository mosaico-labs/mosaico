# Multi-Domain Querying

This guide demonstrates how to orchestrate a **Unified Query** across three distinct layers of the Mosaico Data Platform: the **Sequence** (session metadata), the **Topic** (channel configuration), and the **Ontology Catalog** (actual sensor data). By combining these builders in a single request, you can perform highly targeted searches that correlate mission-level context with specific sensor events.

### The Objective

We want to isolate data segments from a large fleet recording by searching for:

1. **Sequence**: Sessions belonging to the `"Apollo"` project.
2. **Topic**: Specifically the front-facing camera imu topic named `"/front/camera/imu"`.
3. **Ontology**: Time segments where such an IMU recorded a longitudinal acceleration (x-axis) exceeding 5.0 m/s².

For a more in-depth explanation:

* **[Documentation: Querying Catalogs](../query.md)**
* **[API Reference: Query Builders](../API_reference/query/builders.md)**
* **[API Reference: Query Response](../API_reference/query/response.md)**

### Implementation

When you pass multiple builders to the [`MosaicoClient.query()`][mosaicolabs.comm.MosaicoClient.query] method, the platform joins them with a logical **AND** condition. The server will return only the sequences that match the [`QuerySequence`][mosaicolabs.models.query.builders.QuerySequence] criteria, and within those sequences, only the topics that match both the [`QueryTopic`][mosaicolabs.models.query.builders.QueryTopic] and [`QueryOntologyCatalog`][mosaicolabs.models.query.builders.QueryOntologyCatalog] criteria. The multi-domain query allows you to execute a search across metadata and raw sensor data in a single, atomic request.

```python
from mosaicolabs import MosaicoClient, QuerySequence, QueryTopic, QueryOntologyCatalog, IMU, Sequence

# 1. Establish a connection
with MosaicoClient.connect("localhost", 6726) as client:
    
    # 2. Execute a unified multi-domain query
    results = client.query(
        # Filter 1: Sequence Layer (Project Metadata)
        QuerySequence()
            .with_expression(Sequence.Q.user_metadata["project.name"].eq("Apollo")), # (1)!
        
        # Filter 2: Topic Layer (Specific Channel Name)
        QueryTopic()
            .with_name("/front/camera/imu"), # Precise name match
            
        # Filter 3: Ontology Layer (Deep Data Event Detection)
        QueryOntologyCatalog(include_timestamp_range=True)
            .with_expression(IMU.Q.acceleration.x.gt(5.0))
    )

    # 3. Process the Structured Response
    if results:
        for item in results:
            # item.sequence contains the matched Sequence metadata
            print(f"Sequence: {item.sequence.name}") 
            
            # item.topics contains only the topics and time-segments 
            # that satisfied ALL criteria simultaneously
            for topic in item.topics:
                # Access the high-precision timestamp for the detected event
                print(f"  - Match in Topic: {topic.name}") # (2)!
                start, end = topic.timestamp_range.start, topic.timestamp_range.end # (3)!
                print(f"    Event Window: {start} to {end} ns")

```

1. Use dot notation to access nested fields in the `user_metadata` dictionary.
2. The `item.topics` list contains all the topics that matched the query. In this case, it will contain all the topics that are of type IMU, with a name matching that specific topic name and for which the data-related filter is met.
3. The `topic.timestamp_range` provides the first and last occurrence of the queried condition within a topic, allowing you to slice data accurately for further analysis.

The `query` method returns `None` if an error occurs, or a [`QueryResponse`][mosaicolabs.models.query.response.QueryResponse] object. This response acts as a list of [`QueryResponseItem`][mosaicolabs.models.query.response.QueryResponseItem] objects, each providing:

*   **`item.sequence`**: A [`QueryResponseItemSequence`][mosaicolabs.models.query.response.QueryResponseItemSequence] containing the sequence metadata.
*   **`item.topics`**: A list of [`QueryResponseItemTopic`][mosaicolabs.models.query.response.QueryResponseItemTopic] objects that matched the query.

!!! info "Result Normalization"
    The `topic.name` returns the relative topic path (e.g., `/front/camera/image`), which is immediately compatible with other SDK methods like [`MosaicoClient.topic_handler()`][mosaicolabs.comm.MosaicoClient.topic_handler], [`SequenceHandler.get_topic_handler()`][mosaicolabs.handlers.SequenceHandler.get_topic_handler] or [streamers](../API_reference/handlers/reading.md).

### Key Concepts

* [**Convenience Methods**](../query.md#convenience-methods): High-level helpers like `with_name_match()` provide a quick way to filter common fields.
* [**Generic Methods**](../query.md#generic-expression-method): The `with_expression()` method accepts raw **Query Expressions** generated through the [`.Q` proxy](../query.md#the-q-proxy-mechanism). This provides full access to every supported operator (`.gt()`, `.lt()`, `.between()`, etc.) for specific fields.
* **The `.Q` Proxy**: Every ontology model features a [static `.Q` attribute](../ontology.md#querying-data-ontology-with-the-query-q-proxy) that dynamically builds type-safe field paths for your expressions.
* **Temporal Windows**: By setting `include_timestamp_range=True` in the `QueryOntologyCatalog`, the platform identifies the exact start and end of the matching event within the stream.
* **Dictionary Access**: Use bracket notation (e.g., [`Sequence.Q.user_metadata["key.subkey"]`][mosaicolabs.models.platform.Sequence]) to query custom tags that are not part of a fixed schema.
