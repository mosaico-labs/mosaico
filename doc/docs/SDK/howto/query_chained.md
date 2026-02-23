# Complex Chained Queries

Sometimes a single query is insufficient because you need to correlate data across different topics. This guide demonstrates **Query Chaining**, a technique where the results of one search are used to "lock" the domain for a second, more specific search.

### The Objective

Find sequences where a high-precision GPS state was achieved, and **within those same sequences**, locate any log messages containing the string `"[ERR]"`.

For a more in-depth explanation:

* **[Documentation: Querying Catalogs](../query.md)**
* **[API Reference: Query Builders](../API_reference/query/builders.md)**
* **[API Reference: Query Response](../API_reference/query/response.md)**

### Implementation

```python
from mosaicolabs import MosaicoClient, QueryTopic, QueryOntologyCatalog, GPS, String

with MosaicoClient.connect("localhost", 6726) as client:
    # Step 1: Initial Broad Search
    # Find all sequences with high-precision GPS (e.g. status code 2)
    initial_response = client.query(
        QueryOntologyCatalog(GPS.Q.status.status.eq(2))
    )

    if initial_response:
        # Step 2: Domain Locking
        # .to_query_sequence() creates a new builder pre-filtered to ONLY these sequences
        refined_domain = initial_response.to_query_sequence() # (1)!

        # Step 3: Targeted Refinement
        # Search for error strings only within the validated sequences
        final_results = client.query(
            refined_domain, # Restrict to this search domain
            QueryTopic().with_name("/localization/log_string"),
            QueryOntologyCatalog(String.Q.data.match("[ERR]"))
        )

        for item in final_results:
            print(f"Error found in precise sequence: {item.sequence.name}")

```

1. [`to_query_sequence()`][mosaicolabs.models.query.response.QueryResponse.to_query_sequence] returns a [`QuerySequence`][mosaicolabs.models.query.builders.QuerySequence] builder pre-filtered to include only the **sequences** present in the response. See also [`to_query_topic()`][mosaicolabs.models.query.response.QueryResponse.to_query_topic]


The `query` method returns `None` if an error occurs, or a [`QueryResponse`][mosaicolabs.models.query.response.QueryResponse] object. This response acts as a list of [`QueryResponseItem`][mosaicolabs.models.query.response.QueryResponseItem] objects, each providing:

*   **`item.sequence`**: A [`QueryResponseItemSequence`][mosaicolabs.models.query.response.QueryResponseItemSequence] containing the sequence metadata.
*   **`item.topics`**: A list of [`QueryResponseItemTopic`][mosaicolabs.models.query.response.QueryResponseItemTopic] objects that matched the query.

!!! info "Result Normalization"
    The `topic.name` returns the relative topic path (e.g., `/front/camera/image`), which is immediately compatible with other SDK methods like [`MosaicoClient.topic_handler()`][mosaicolabs.comm.MosaicoClient.topic_handler], [`SequenceHandler.get_topic_handler()`][mosaicolabs.handlers.SequenceHandler.get_topic_handler] or [streamers](../API_reference/handlers/reading.md).


### Key Concepts

* [**Convenience Methods**](../query.md#convenience-methods): High-level helpers like `QueryTopic().with_name()` provide a quick way to filter by ontology tags.
* [**Generic Methods**](../query.md#generic-expression-method): The `with_expression()` method accepts raw **Query Expressions** generated through the [`.Q` proxy](../query.md#the-q-proxy-mechanism). This provides full access to every supported operator (`.gt()`, `.lt()`, `.between()`, etc.) for specific fields.
* **The `.Q` Proxy**: Every ontology model features a [static `.Q` attribute](../ontology.md#querying-data-ontology-with-the-query-q-proxy) that dynamically builds type-safe field paths for your expressions.
* **Why is Chaining Necessary?** A single `client.query()` call applies a logical **AND** to all conditions to find a single **topic** that satisfies everything. Since a topic cannot be both a `GPS` stream and a `String` log simultaneously, you must use chaining to link two different topics within the same **Sequence** context.
