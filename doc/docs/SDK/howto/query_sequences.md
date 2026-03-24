---
title: Querying Sequences by Name and Metadata
description: Example how-to for Querying Sequences by Name and Metadata
---

This guide demonstrates how to locate specific recording sessions based on their naming conventions and custom user metadata tags. This is the most common entry point for data discovery, allowing you to isolate sessions that match specific environmental or project conditions.


!!! example "Related experiment"
    To fully grasp the following How-To, we recommend you to read (and reproduce) the **[Querying Catalogs](../examples/query_catalogs.md) Example**.

??? question "In Depth Explanation"
    * **[Documentation: Querying Catalogs](../query.md)**
    * **[API Reference: Query Builders](../API_reference/query/builders.md)**
    * **[API Reference: Query Response](../API_reference/query/response.md)**

### The Objective

We want to find all sequences where:

1. The sequence name contains the string `"test_drive"`.
2. The user metadata indicates a specific project name (e.g., `"Apollo"`).
3. The environmental visibility was recorded as less than 50m.

### Implementation

When you call multiple `with_*` methods of the [`QuerySequence`][mosaicolabs.models.query.builders.QuerySequence] builder, the platform joins them with a logical **AND** condition. The server will return only the sequences that match the  criteria alltogether.

```python
from mosaicolabs import MosaicoClient, QuerySequence, Sequence

# 1. Establish a connection
with MosaicoClient.connect("localhost", 6726) as client:
    # 2. Execute the query
    results = client.query(
        QuerySequence()
        # Use a convenience method for fuzzy name matching
        .with_name_match("test_drive")
        # Use convenience method for filtering fixed and dynamic metadata fields
        .with_user_metadata("project.name", eq="Apollo")
        .with_user_metadata("environment.visibility", lt=50) # (1)!
    )

    # 3. Process the Response
    if results:
        for item in results:
            # item.sequence contains the information for the matched sequence
            print(f"Matched Sequence: {item.sequence.name}")
            print(f"  Topics: {[topic.name for topic in item.topics]}") # (2)!

```

1. Use dot notation to access nested fields stored in the user metadata.
2. The `item.topics` list contains all the topics that matched the query. In this case, all the available topics are returned because no topic-specific filters were applied.

The `query` method returns `None` if an error occurs, or a [`QueryResponse`][mosaicolabs.models.query.response.QueryResponse] object. This response acts as a list of [`QueryResponseItem`][mosaicolabs.models.query.response.QueryResponseItem] objects, each providing:

*   **`item.sequence`**: A [`QueryResponseItemSequence`][mosaicolabs.models.query.response.QueryResponseItemSequence] containing the sequence name matching the query.
*   **`item.topics`**: A list of [`QueryResponseItemTopic`][mosaicolabs.models.query.response.QueryResponseItemTopic] objects that matched the query.

!!! info "Result Normalization"
    The `topic.name` returns the relative topic path (e.g., `/front/camera/image`), which is immediately compatible with other SDK methods like [`MosaicoClient.topic_handler()`][mosaicolabs.comm.MosaicoClient.topic_handler], [`SequenceHandler.get_topic_handler()`][mosaicolabs.handlers.SequenceHandler.get_topic_handler] or [streamers](../API_reference/handlers/reading.md).

### Key Concepts

* [**Convenience Methods**](../query.md#convenience-methods): High-level helpers like `with_name_match()` provide a quick way to filter common fields.
* [**Generic Methods**](../query.md#generic-expression-method): The `with_expression()` method accepts raw **Query Expressions** generated through the [`.Q` proxy](../query.md#the-q-proxy-mechanism). This provides full access to every supported operator (`.gt()`, `.lt()`, `.between()`, etc.) for specific fields.
* **Dynamic Metadata Access**: Using the nested notation in `with_user_metadata("key.subkey", ...)` allows you to query any custom tag you attached during the ingestion phase.
