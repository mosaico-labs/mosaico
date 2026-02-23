# Detecting Events in Time-Series Data

Beyond metadata, the Mosaico platform allows for deep inspection of actual sensor payloads. This guide shows how to search for specific physical events, such as high-impact accelerations, across your entire data catalog.

### The Objective

Identify specific time segments where an IMU sensor recorded:

1. Lateral acceleration (-axis) greater than .
2. Longitudinal acceleration (-axis) greater than .

For a more in-depth explanation:

* **[Documentation: Querying Catalogs](../query.md)**
* **[API Reference: Query Builders](../API_reference/query/builders.md)**
* **[API Reference: Query Response](../API_reference/query/response.md)**

### Implementation

When you call multiple `with_*` methods of the [`QueryOntologyCatalog`][mosaicolabs.models.query.builders.QueryOntologyCatalog] builder, the platform joins them with a logical **AND** condition. The server will return only the sequences that match the  criteria alltogether.

```python
from mosaicolabs import MosaicoClient, QueryOntologyCatalog, IMU

# 1. Establish a connection
with MosaicoClient.connect("localhost", 6726) as client:
    # 2. Execute the query across all available data
    # include_timestamp_range is essential for pinpointing the exact time of the event
    results = client.query(
        QueryOntologyCatalog(include_timestamp_range=True)
        .with_expression(IMU.Q.acceleration.x.gt(5.0))
        .with_expression(IMU.Q.acceleration.y.gt(4.0))
    )

    # 3. Process the Response
    if results:
        for item in results:
            print(f"Impact detected in Sequence: {item.sequence.name}")
            for topic in item.topics:
                # topic.timestamp_range provides the start and end of the match
                print(f"  - Match in Topic: {topic.name}") # (1)!
                start, end = topic.timestamp_range.start, topic.timestamp_range.end # (2)!
                print(f"    Event Window: {start} to {end} ns")

```

1. The `item.topics` list contains all the topics that matched the query. In this case, it will contain all the topics that are of type IMU and for which the data-related filter is met. 
2. The `topic.timestamp_range` provides the first and last occurrence of the queried condition within a topic, allowing you to slice data accurately for further analysis.

The `query` method returns `None` if an error occurs, or a [`QueryResponse`][mosaicolabs.models.query.response.QueryResponse] object. This response acts as a list of [`QueryResponseItem`][mosaicolabs.models.query.response.QueryResponseItem] objects, each providing:

*   **`item.sequence`**: A [`QueryResponseItemSequence`][mosaicolabs.models.query.response.QueryResponseItemSequence] containing the sequence metadata.
*   **`item.topics`**: A list of [`QueryResponseItemTopic`][mosaicolabs.models.query.response.QueryResponseItemTopic] objects that matched the query.

!!! info "Result Normalization"
    The `topic.name` returns the relative topic path (e.g., `/front/camera/image`), which is immediately compatible with other SDK methods like [`MosaicoClient.topic_handler()`][mosaicolabs.comm.MosaicoClient.topic_handler], [`SequenceHandler.topic_handler()`][mosaicolabs.handlers.SequenceHandler.get_topic_handler] or [streamers](../API_reference/handlers/reading.md).

### Key Concepts

* [**Generic Methods**](../query.md#generic-expression-method): The `with_expression()` method accepts raw **Query Expressions** generated through the [`.Q` proxy](../query.md#the-q-proxy-mechanism). This provides full access to every supported operator (`.gt()`, `.lt()`, `.between()`, etc.) for specific fields.
* **The `.Q` Proxy**: Every ontology model features a [static `.Q` attribute](../ontology.md#querying-data-ontology-with-the-query-q-proxy) that dynamically builds type-safe field paths for your expressions.
* **Temporal Windows**: Setting `include_timestamp_range=True` enables the platform to return the precise "occurrence" of the event, which is vital for later playback or slicing.
* **Type-Safe Operators**: The `.Q` proxy ensures that only valid operators (like `.gt()`) are available for numeric fields like `acceleration.x`.

