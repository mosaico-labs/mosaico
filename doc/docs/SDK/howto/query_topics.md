# Querying Topics by Name and Metadata

This guide demonstrates how to locate specific recording sessions based on their naming conventions and custom user metadata tags. This is the most common entry point for data discovery, allowing you to isolate sessions that match specific environmental or project conditions.

### The Objective

We want to find all topics where:

1. The topic refers to an IMU sensor.
2. The user metadata indicates a specific sensor interface (e.g., `"serial"`).

For a more in-depth explanation:

* **[Documentation: Querying Catalogs](../query.md)**
* **[API Reference: Query Builders](../API_reference/query/builders.md)**
* **[API Reference: Query Response](../API_reference/query/response.md)**

### Implementation

When you call multiple `with_*` methods of the [`QueryTopic`][mosaicolabs.models.query.builders.QueryTopic] builder, the platform joins them with a logical **AND** condition. The server will return only the sequences that match the  criteria alltogether.

```python
from mosaicolabs import MosaicoClient, QueryTopic, Topic, IMU

# 1. Establish a connection
with MosaicoClient.connect("localhost", 6726) as client:
    # 2. Execute the query
    results = client.query(
        QueryTopic()
        # Use a convenience method for fuzzy name matching
        .with_ontology_tag(IMU.ontology_tag())
        # Use the .Q proxy to filter fixed and dynamic metadata fields
        .with_expression(Topic.Q.user_metadata["interface"].eq("serial")))

    # 3. Process the Response
    if results:
        for item in results:
            # item.sequence contains the metadata for the matched sequence
            print(f"Matched Sequence: {item.sequence.name}")
            print(f"  Topics: {[topic.name for topic in item.topics]}") # (1)!

```

1. The `item.topics` list contains all the topics that matched the query. In this case, it will contain all the topics that are of type IMU and have the user metadata field `interface` set to `"serial"`.

The `query` method returns `None` if an error occurs, or a [`QueryResponse`][mosaicolabs.models.query.response.QueryResponse] object. This response acts as a list of [`QueryResponseItem`][mosaicolabs.models.query.response.QueryResponseItem] objects, each providing:

*   **`item.sequence`**: A [`QueryResponseItemSequence`][mosaicolabs.models.query.response.QueryResponseItemSequence] containing the sequence metadata.
*   **`item.topics`**: A list of [`QueryResponseItemTopic`][mosaicolabs.models.query.response.QueryResponseItemTopic] objects that matched the query.

!!! info "Result Normalization"
    The `topic.name` returns the relative topic path (e.g., `/front/camera/image`), which is immediately compatible with other SDK methods like [`MosaicoClient.topic_handler()`][mosaicolabs.comm.MosaicoClient.topic_handler], [`SequenceHandler.get_topic_handler()`][mosaicolabs.handlers.SequenceHandler.get_topic_handler] or [streamers](../API_reference/handlers/reading.md).

### Key Concepts

* [**Convenience Methods**](../query.md#convenience-methods): High-level helpers like `with_ontology_tag()` provide a quick way to filter by ontology tags.
* [**Generic Methods**](../query.md#generic-expression-method): The `with_expression()` method accepts raw **Query Expressions** generated through the [`.Q` proxy](../query.md#the-q-proxy-mechanism). This provides full access to every supported operator (`.gt()`, `.lt()`, `.between()`, etc.) for specific fields.
* **Dynamic Metadata Access**: Using the bracket notation [`Topic.Q.user_metadata["key"]`][mosaicolabs.models.platform.Topic] allows you to query any custom tag you attached during the ingestion phase.
