---
title: Querying Catalogs
description: The Query mechanism
---

The **Query Module** provides a high-performance, **fluent** interface for discovering and filtering data within the Mosaico Data Platform. It is designed to move beyond simple keyword searches, allowing you to perform deep, semantic queries across metadata, system catalogs, and the physical content of sensor streams.

A typical query workflow involves chaining methods within specialized builders to create a unified request that the server executes atomically. In the example below, the code orchestrates a multi-domain search to isolate high-interest data segments. Specifically, it queries for:

* **Sequence Discovery**: Finds any recording session whose name contains the string `"test_drive"` **AND** where the custom user metadata indicates an `"environment.visibility"` value strictly less than 50.
* **Topic Filtering**: Restricts the search specifically to the data channel named `"/front/camera/image"`.
* **Ontology Analysis**: Performs a deep inspection of IMU sensor payloads to identify specific time segments where the **X-axis acceleration exceeds a certain threshold** while simultaneously the **Y-axis acceleration exceeds a certain threshold**.

```python
from mosaicolabs import QueryOntologyCatalog, QuerySequence, QueryTopic, IMU, MosaicoClient

# Establish a connection to the Mosaico Data Platform
with MosaicoClient.connect("localhost", 6726) as client:
    # Perform a unified server-side query across multiple domains:
    qresponse = client.query(
        # Filter Sequence-level metadata
        QuerySequence()
        .with_name_match("test_drive")  # Use convenience method for fuzzy name matching
        .with_expression(               # Use the .Q proxy to filter the `user_metadata` field
            Sequence.Q.user_metadata["environment.visibility"].lt(50)
        ),
        # Search on topics with specific names
        QueryTopic()
        .with_name("/front/camera/image"),
        # Perform deep time-series discovery within sensor payloads
        QueryOntologyCatalog(include_timestamp_range=True) # Request temporal bounds for matches
        .with_expression(IMU.Q.acceleration.x.gt(5.0))     # Use the .Q proxy to filter the `acceleration` field
        .with_expression(IMU.Q.acceleration.y.gt(4.0)),
    )

    # The server returns a QueryResponse grouped by Sequence for structured data management
    if qresponse is not None:
        for item in qresponse:
            # 'item.sequence' contains the name for the matched sequence
            print(f"Sequence: {item.sequence.name}") 
            
            # 'item.topics' contains only the topics and time-segments 
            # that satisfied the QueryOntologyCatalog criteria
            for topic in item.topics:
                # Access high-precision timestamps for the data segments found
                start, end = topic.timestamp_range.start, topic.timestamp_range.end
                print(f"  Topic: {topic.name} | Match Window: {start} to {end}")

```

The provided example illustrates the core architecture of the Mosaico Query DSL. To effectively use this module, it is important to understand the two primary mechanisms that drive data discovery:

* **Query Builders (Fluent Logic Collectors)**: Specialized builders like [`QuerySequence`][mosaicolabs.models.query.builders.QuerySequence], [`QueryTopic`][mosaicolabs.models.query.builders.QueryTopic], and [`QueryOntologyCatalog`][mosaicolabs.models.query.builders.QueryOntologyCatalog] serve as containers for your search criteria. They provide a **Fluent Interface** where you can chain two types of methods:
    * **Convenience Methods**: High-level helpers for common fields, such as `with_name()`, `with_name_match()`, or `with_created_timestamp()`.
    * **Generic `with_expression()`**: A versatile method that accepts any expression obtained via the **`.Q` proxy**, allowing you to define complex filters for nested user metadata or deep sensor payloads.
* **The `.Q` Proxy (Dynamic Model Inspection)**: Every [`Serializable`][mosaicolabs.models.serializable.Serializable] model in the Mosaico ontology features a static `.Q` attribute. This proxy dynamically inspects the model's underlying schema to build dot-notated field paths and intercepts attribute access (e.g., `IMU.Q.acceleration.x`). When a terminal method is called—such as `.gt()`, `.lt()`, or `.between()`—it generates a type-safe **Atomic Expression** used by the platform to filter physical sensor data or metadata fields.

By combining these mechanisms, the Query Module delivers a robust filtering experience:

* **Multi-Domain Orchestration**: Execute searches across Sequence metadata, Topic configurations, and raw Ontology sensor data in a single, atomic request.
* **Structured Response Management**: Results are returned in a [`QueryResponse`][mosaicolabs.models.query.response.QueryResponse] that is automatically grouped by `Sequence`, making it easier to manage multi-sensor datasets.

## Query Execution & The Response Model

Queries are executed via the [`query()`][mosaicolabs.comm.MosaicoClient.query] method exposed by the [`MosaicoClient`][mosaicolabs.comm.MosaicoClient] class. When multiple builders are provided, they are combined with a logical **AND**.

| Method | Return | Description |
| :--- | :--- | :--- |
| [`query(*queries, query)`][mosaicolabs.comm.MosaicoClient.query] | [`Optional[QueryResponse]`][mosaicolabs.models.query.response.QueryResponse] | Executes one or more queries against the platform catalogs. The provided queries are joined in AND condition. The method accepts a variable arguments of query builder objects or a pre-constructed [`Query`][mosaicolabs.models.query.builders.Query] object.|


The query execution returns a [`QueryResponse`][mosaicolabs.models.query.response.QueryResponse] object, which behaves like a standard Python list containing [`QueryResponseItem`][mosaicolabs.models.query.response.QueryResponseItem] objects.

| Class | Description |
| --- | --- |
| [`QueryResponseItem`][mosaicolabs.models.query.response.QueryResponseItem] | Groups all matches belonging to the same **Sequence**. Contains a `QueryResponseItemSequence` and a list of related `QueryResponseItemTopic`.|
| [`QueryResponseItemSequence`][mosaicolabs.models.query.response.QueryResponseItemSequence] | Represents a specific **Sequence** where matches were found. It includes the sequence name. |
| [`QueryResponseItemTopic`][mosaicolabs.models.query.response.QueryResponseItemTopic] | Represents a specific **Topic** where matches were found. It includes the normalized topic path and the optional `timestamp_range` (the first and last occurrence of the condition). |

```python
import sys
from mosaicolabs import MosaicoClient, QueryOntologyCatalog
from mosaicolabs.models.sensors import IMU

# Establish a connection to the Mosaico Data Platform
with MosaicoClient.connect("localhost", 6726) as client:
    
    # Define a Deep Data Filter using the .Q Query Proxy
    # We are searching for vertical impact events where acceleration.z > 15.0 m/s^2
    impact_qbuilder = QueryOntologyCatalog(
        IMU.Q.acceleration.z.gt(15.0),
        # include_timestamp_range returns the precise start/end of the matching event
        include_timestamp_range=True
    )

    # Execute the query via the client
    results = client.query(impact_qbuilder)
    # The same can be obtained by using the Query object
    # results = client.query(
    #     query = Query(
    #         impact_qbuilder
    #     )
    # )
    
    if results is not None:
        # Parse the structured QueryResponse object
        # Results are automatically grouped by Sequence for easier data management
        for item in results:
            print(f"Sequence: {item.sequence.name}")
            
            # Iterate through matching topics within the sequence
            for topic in item.topics:
                # Topic names are normalized (sequence prefix is stripped) for direct use
                print(f"  - Match in: {topic.name}")
                
                # Extract the temporal bounds of the event
                if topic.timestamp_range:
                    start = topic.timestamp_range.start
                    end = topic.timestamp_range.end
                    print(f"    Occurrence: {start} ns to {end} ns")

```

* **Temporal Windows**: The `timestamp_range` provides the first and last occurrence of the queried condition within a topic, allowing you to slice data accurately for further analysis.
* **Result Normalization**: `topic.name` returns the relative topic path (e.g., `/sensors/imu`), making it immediately compatible with other SDK methods like [`topic_handler()`][mosaicolabs.comm.MosaicoClient.topic_handler].


### Restricted Queries (Chaining)
The `QueryResponse` class enables a powerful mechanism for **iterative search refinement** by allowing you to convert your current results back into a new query builder.
This approach is essential for resolving complex, multi-modal dependencies where a single monolithic query would be logically ambiguous, inefficient or technically impossible.

| Method | Return Type | Description |
| --- | --- | --- |
| [`to_query_sequence()`][mosaicolabs.models.query.response.QueryResponse.to_query_sequence] | [`QuerySequence`][mosaicolabs.models.query.builders.QuerySequence] | Returns a query builder pre-filtered to include only the **sequences** present in the response. |
| [`to_query_topic()`][mosaicolabs.models.query.response.QueryResponse.to_query_topic] | [`QueryTopic`][mosaicolabs.models.query.builders.QueryTopic] | Returns a query builder pre-filtered to include only the specific **topics** identified in the response. |

When you invoke these factory methods, the SDK generates a new query expression containing an explicit `$in` filter populated with the identifiers held in the current response. This effectively **"locks" the search domain**, allowing you to apply new criteria to a restricted subset of your data without re-scanning the entire platform catalog.

```python
from mosaicolabs import MosaicoClient, QueryTopic, QueryOntologyCatalog, GPS, String

with MosaicoClient.connect("localhost", 6726) as client:
    # Broad Search: Find all sequences where a GPS sensor reached a high-precision state (status=2)
    initial_response = client.query(
        QueryOntologyCatalog(GPS.Q.status.status.eq(2))
    )
    # 'initial_response' now acts as a filtered container of matching sequences.

    # Domain Locking: Restrict the search scope to the results of the initial query
    if not initial_response.is_empty():
        # .to_query_sequence() generates a QuerySequence pre-filled with the matching sequence names.
        refined_query_builder = initial_response.to_query_sequence()

        # Targeted Refinement: Search for error patterns ONLY within the restricted domain
        # This ensures the platform only scans for '[ERR]' strings within sequences already validated for GPS precision.
        final_response = client.query(
            refined_query_builder,                                         # The "locked" sequence domain
            QueryTopic().with_name("/localization/log_string"),    # Target a specific log topic
            QueryOntologyCatalog(String.Q.data.match("[ERR]"))     # Filter by exact data content pattern
        )

```

When a specific set of topics has been identified through a data-driven query (e.g., finding every camera topic that recorded a specific event), you can use `to_query_topic()` to "lock" your next search to those specific data channels. This is particularly useful when you need to verify a condition on a very specific subset of sensors across many sequences, bypassing the need to re-identify those topics in the next step.

In the next example, we first find all topics of a specific channel from a specific sequence name pattern, and then search specifically within *those* topics for any instances where the data content matches a specific pattern.

```python
from mosaicolabs import MosaicoClient, QueryTopic

with MosaicoClient.connect("localhost", 6726) as client:
    # Broad Search: Find sequences with high-precision GPS
    initial_response = client.query(
            QueryTopic().with_name("/localization/log_string"), # Target a specific log topic
            QuerySequence().with_name_match("test_winter_2025_")  # Filter by sequence name pattern
        )

    # Chaining: Use results to "lock" the domain and find specific log-patterns in those sequences
    if not initial_response.is_empty():
        final_response = client.query(
            initial_response.to_query_topic(),              # The "locked" topic domain
            QueryOntologyCatalog(String.Q.data.match("[ERR]"))  # Filter by content
        )
```

#### When Chaining is Necessary

The previous example of the `GPS.status` query and the subsequent `/localization/log_string` topic search highlight exactly when *query chaining* becomes a technical necessity rather than just a recommendation. In the Mosaico Data Platform, a single `client.query()` call applies a logical **AND** across all provided builders to locate individual **data streams (topics)** that satisfy every condition simultaneously.
Because a single topic cannot physically represent two different sensor types at once, such as being both a `GPS` sensor and a `String` log, a monolithic query attempting to filter for both on the same stream will inherently return zero results. Chaining resolves this by allowing you to find the correct **Sequence** context in step one, then "locking" that domain to find a different **Topic** within that same context in step two.

```python
# AMBIGUOUS: This looks for ONE topic that is BOTH GPS and String
response = client.query(
    QueryOntologyCatalog(GPS.Q.status.status.eq(DGPS_FIX)),
    QueryOntologyCatalog(String.Q.data.match("[ERR]")),
    QueryTopic().with_name("/localization/log_string")
)

```

## Architecture

### Query Layers

Mosaico organizes data into three distinct architectural layers, each with its own specialized Query Builder:

#### [`QuerySequence`][mosaicolabs.models.query.builders.QuerySequence] (Sequence Layer)
API Reference: [`mosaicolabs.models.query.builders.QuerySequence`][mosaicolabs.models.query.builders.QuerySequence].

Filters recordings based on high-level session metadata, such as the sequence name or the time it was created.

**Example** Querying for sequences by name and creation date

```python
from mosaicolabs import MosaicoClient, Topic, QuerySequence

with MosaicoClient.connect("localhost", 6726) as client:
    # Search for sequences by project name and creation date
    qresponse = client.query(
        QuerySequence()
        .with_name_match("test_drive")
        .with_expression(Sequence.Q.user_metadata["project"].eq("Apollo"))
        .with_created_timestamp(time_start=Time.from_float(1690000000.0))
    )

    # Inspect the response
    for item in qresponse:
        print(f"Sequence: {item.sequence.name}")
        print(f"Topics: {[topic.name for topic in item.topics]}")
```


#### [`QueryTopic`][mosaicolabs.models.query.builders.QueryTopic] (Topic Layer)
API Reference: [`mosaicolabs.models.query.builders.QueryTopic`][mosaicolabs.models.query.builders.QueryTopic].

Targets specific data channels within a sequence. You can search for topics by name pattern or by their specific Ontology type (e.g., "Find all GPS topics").

**Example** Querying for image topics by ontology tag, metadata key and topic creation timestamp

```python
from mosaicolabs import MosaicoClient, Image, Topic, QueryTopic

with MosaicoClient.connect("localhost", 6726) as client:
    # Query for all 'image' topics created in a specific timeframe, matching some metadata (key, value) pair
    qresponse = client.query(
        QueryTopic()
        .with_ontology_tag(Image.ontology_tag())
        .with_created_timestamp(time_start=Time.from_float(1700000000))
        .with_expression(Topic.Q.user_metadata["camera_id.serial_number"].eq("ABC123_XYZ"))
    )

    # Inspect the response
    if qresponse is not None:
        # Results are automatically grouped by Sequence for easier data management
        for item in qresponse:
            print(f"Sequence: {item.sequence.name}")
            print(f"Topics: {[topic.name for topic in item.topics]}")
```


#### [`QueryOntologyCatalog`][mosaicolabs.models.query.builders.QueryOntologyCatalog] (Ontology Catalog Layer)
API Reference: [`mosaicolabs.models.query.builders.QueryOntologyCatalog`][mosaicolabs.models.query.builders.QueryOntologyCatalog].

Filters based on the **actual time-series content** of the sensors (e.g., "Find events where `acceleration.z` exceeded a specific value").

**Example** Querying for mixed sensor data

```python
from mosaicolabs import MosaicoClient, QueryOntologyCatalog, GPS, IMU

    with MosaicoClient.connect("localhost", 6726) as client:
        # Chain multiple sensor filters together
        qresponse = client.query(
            QueryOntologyCatalog()
            .with_expression(GPS.Q.status.satellites.geq(8))
            .with_expression(Temperature.Q.value.between([273.15, 373.15]))
            .with_expression(Pressure.Q.value.geq(100000))
        )

        # Inspect the response
        if qresponse is not None:
            # Results are automatically grouped by Sequence for easier data management
            for item in qresponse:
                print(f"Sequence: {item.sequence.name}")
                print(f"Topics: {[topic.name for topic in item.topics]}")

        # Filter for a specific component value and extract the first and last occurrence times
        qresponse = client.query(
            QueryOntologyCatalog(include_timestamp_range=True)
            .with_expression(IMU.Q.acceleration.x.lt(-4.0))
            .with_expression(IMU.Q.acceleration.y.gt(5.0))
            .with_expression(Pose.Q.rotation.z.geq(0.707))
        )

        # Inspect the response
        if qresponse is not None:
            # Results are automatically grouped by Sequence for easier data management
            for item in qresponse:
                print(f"Sequence: {item.sequence.name}")
                print(f"Topics: {{topic.name:
                            [topic.timestamp_range.start, topic.timestamp_range.end]
                            for topic in item.topics}}")
```



The Mosaico Query Module offers two distinct paths for defining filters,  **Convenience Methods** and **Generic Expression Method**, both of which support **method chaining** to compose multiple criteria into a single query using a logical **AND**.

#### Convenience Methods

The query layers provide high-level fluent helpers (`with_<attribute>`), built directly into the query builder classes and designed for ease of use.
They allow you to filter data without deep knowledge of the internal model schema. 
The builder automatically selects the appropriate field and operator (such as exact match vs. substring pattern) based on the method used.

```python
from mosaicolabs import QuerySequence, QueryTopic, RobotJoint

# Build a filter with name pattern
qbuilder = QuerySequence()
    .with_name_match("test_drive")
# Execute the query
qresponse = client.query(qbuilder)

# Inspect the response
if qresponse is not None:
    # Results are automatically grouped by Sequence for easier data management
    for item in qresponse:
        print(f"Sequence: {item.sequence.name}")
        print(f"Topics: {[topic.name for topic in item.topics]}")

# Build a filter with ontology tag AND a specific creation time window
qbuilder = QueryTopic()
    .with_ontology_tag(RobotJoint.ontology_tag())
    .with_created_timestamp(start=t1, end=t2)
# Execute the query
qresponse = client.query(qbuilder)

# Inspect the response
if qresponse is not None:
    # Results are automatically grouped by Sequence for easier data management
    for item in qresponse:
        print(f"Sequence: {item.sequence.name}")
        print(f"Topics: {[topic.name for topic in item.topics]}")
```

* **Best For**: Standard system-level fields like Names and Timestamps.

#### Generic Expression Method

The `with_expression()` method accepts raw **Query Expressions** generated through the `.Q` proxy. 
This provides full access to every supported operator (`.gt()`, `.lt()`, `.between()`, etc.) for specific fields.

```python
from mosaicolabs import QueryOntologyCatalog, QuerySequence, IMU

# Build a filter with name pattern and metadata-related expression
qbuilder = QuerySequence()
    .with_expression(
        # Use query proxy for generating a QueryExpression
        Sequence.Q.user_metadata['environment.visibility'].lt(50)
    )
    # Can be AND-chained with convenience methods
    .with_name_match("test_drive")
# Execute the query
qresponse = client.query(qbuilder)

# Inspect the response
if qresponse is not None:
    # Results are automatically grouped by Sequence for easier data management
    for item in qresponse:
        print(f"Sequence: {item.sequence.name}")
        print(f"Topics: {[topic.name for topic in item.topics]}")

# Build a filter with deep time-series data discovery and measurement time windowing
qbuilder = QueryOntologyCatalog()
    .with_expression(IMU.Q.acceleration.x.gt(5.0))
    .with_expression(IMU.Q.header.stamp.sec.gt(1700134567))
    .with_expression(IMU.Q.header.stamp.nanosec.between([123456, 789123]))
# Execute the query
qresponse = client.query(qbuilder)

# Inspect the response
if qresponse is not None:
    # Results are automatically grouped by Sequence for easier data management
    for item in qresponse:
        print(f"Sequence: {item.sequence.name}")
        print(f"Topics: {[topic.name for topic in item.topics]}")
```

* **Best For**: Accessing specific Ontology data fields (e.g., acceleration, position, etc.) and custom `user_metadata` in `Sequence` and `Topic` data models.

### The `.Q` Proxy Mechanism

The Query Proxy is the cornerstone of Mosaico's type-safe data discovery. Every data model in the Mosaico Ontology (e.g., `IMU`, `GPS`, `Image`) is automatically injected with a static `.Q` attribute during class initialization. This mechanism transforms static data structures into dynamic, fluent interfaces for constructing complex filters.

The proxy follows a three-step lifecycle to ensure that your queries are both semantically correct and high-performance:

1. **Intelligent Mapping**: During system initialization, the proxy inspects the sensor's schema recursively. It maps every nested field path (e.g., `"acceleration.x"`) to a dedicated *queryable* object, i.e. an object providing comparison operators and expression generation methods.
2. **Type-Aware Operators**: The proxy identifies the data type of each field (numeric, string, dictionary, or boolean) and exposes only the operators valid for that type. This prevents logical errors, such as attempting a substring `.match()` on a numeric acceleration value.
3. **Intent Generation**: When you invoke an operator (e.g., `.gt(15.0)`), the proxy generates a `QueryExpression`. This object encapsulates your search intent and is serialized into an optimized JSON format for the platform to execute.

To understand how the proxy handles nested structures, inherited attributes, and data types, consider the `IMU` ontology class:

```python
class IMU(Serializable, HeaderMixin):
    acceleration: Vector3d      # Composed type: contains x, y, z
    angular_velocity: Vector3d  # Composed type: contains x, y, z
    orientation: Optional[Quaternion] = None # Composed type: contains x, y, z, w
```

The `.Q` proxy enables you to navigate the data exactly as it is defined in the model. By following the `IMU.Q` instruction, you can drill down through nested fields and inherited mixins using standard dot notation until you reach a base queryable type.

The proxy automatically flattens the hierarchy, including fields inherited from `HeaderMixin` (like `frame_id` and `stamp`), assigning the correct queryable type and operators to each leaf node:
(API Reference: [`mosaicolabs.models.sensors.IMU`][mosaicolabs.models.sensors.IMU--querying-with-the-q-proxy])

| Proxy Field Path | Queryable Type | Supported Operators (Examples) |
| --- | --- | --- |
| **[`IMU.Q.acceleration.x/y/z`][mosaicolabs.models.sensors.IMU.acceleration--querying-with-the-q-proxy]** | **Numeric** | `.gt()`, `.lt()`, `.geq()`, `.leq()`, `.eq()`, `.between()`, `.in_()` |
| **[`IMU.Q.angular_velocity.x/y/z`][mosaicolabs.models.sensors.IMU.angular_velocity--querying-with-the-q-proxy]** | **Numeric** | `.gt()`, `.lt()`, `.geq()`, `.leq()`, `.eq()`, `.between()`, `.in_()` |
| **[`IMU.Q.orientation.x/y/z/w`][mosaicolabs.models.sensors.IMU.orientation--querying-with-the-q-proxy]** | **Numeric** | `.gt()`, `.lt()`, `.geq()`, `.leq()`, `.eq()`, `.between()`, `.in_()` |
| **[`IMU.Q.header.frame_id`][mosaicolabs.models.sensors.IMU.header--querying-with-the-q-proxy]** | **String** | `.eq()`, `.match()` |
| **[`IMU.Q.header.stamp.sec`][mosaicolabs.models.sensors.IMU.header--querying-with-the-q-proxy]** | **Numeric** | `.gt()`, `.lt()`, `.geq()`, `.leq()`, `.eq()`, `.between()`, `.in_()` |
| **[`IMU.Q.header.stamp.nanosec`][mosaicolabs.models.sensors.IMU.header--querying-with-the-q-proxy]** | **Numeric** | `.gt()`, `.lt()`, `.geq()`, `.leq()`, `.eq()`, `.between()`, `.in_()` |


The following table lists the supported operators for each data type:

| Data Type | Operators |
| --- | --- |
| **Numeric** | `.eq()`, `.neq()`, `.lt()`, `.leq()`, `.gt()`, `.geq()`, `.between()`, `.in_()` |
| **String** | `.eq()`, `.neq()`, `.match()` (i.e. substring), `.in_()` |
| **Boolean** | `.eq(True/False)` |
| **Dictionary** | `.eq()`, `.neq()`, `.lt()`, `.leq()`, `.gt()`, `.geq()`, `.between()`, `.in_()`, `.match()`|

#### Supported vs. Unsupported Types

While the `.Q` proxy is highly versatile, it enforces specific rules on which data structures can be queried:

* **Supported Types**: The proxy resolves all simple (int, float, str, bool) or composed types (like `Vector3d` or `Quaternion`). It will continue to expose nested fields as long as they lead to a primitive base type.
* **Dictionaries**: Dynamic fields, such as the [`user_metadata`][mosaicolabs.models.platform.platform_base.PlatformBase.user_metadata] found in the **[`Topic`][mosaicolabs.models.platform.Topic]** and **[`Sequence`][mosaicolabs.models.platform.Sequence]** platform models, are fully queryable through the proxy using bracket notation (e.g., `Topic.Q.user_metadata["key"]` or `Topic.Q.user_metadata["key.subkey.subsubkey"]`). This approach provides the flexibility to search across custom tags and dynamic properties that aren't part of a fixed schema. This dictionary-based querying logic is not restricted to platform models; it applies to any **custom ontology model** created by the user that contains a `dict` field.
    * **Syntax**: Instead of the standard dot notation used for fixed fields, you must use square brackets `["key"]` to target specific dictionary entries.
    * **Nested Access**: For dictionaries containing nested structures, you can use **dot notation within the key string** (e.g., `["environment.visibility"]`) to traverse sub-fields.
    * **Operator Support**: Because dictionary values are dynamic, these fields are "promiscuous," meaning they support all available numeric, string, and boolean operators without strict SDK-level type checking.

* **Unsupported Types (Lists and Tuples)**: Any field defined as a container, such as a **List** or **Tuple** (e.g., `covariance: List[float]`), is currently skipped by the proxy generator. These fields will not appear in autocomplete and cannot be used in a query expression.

## Constraints & Limitations

While fully functional, the current implementation (v0.x) has a **Single Occurrence Constraint**.

* **Constraint**: A specific data field path may appear **only once** within a single query builder instance. You cannot chain two separate conditions on the same field (e.g., `.gt(0.5)` and `.lt(1.0)`).
    ```python
    # INVALID: The same field (acceleration.x) is used twice in the constructor
    QueryOntologyCatalog() \
        .with_expression(IMU.Q.acceleration.x.gt(0.5))
        .with_expression(IMU.Q.acceleration.x.lt(1.0)) # <- Error! Duplicate field path

    ```
* **Solution**: Use the built-in **`.between([min, max])`** operator to perform range filtering on a single field path.
* **Note**: You can still query multiple *different* fields from the same sensor model (e.g., `acceleration.x` and `acceleration.y`) in one builder.
    ```python
    # VALID: Each expression targets a unique field path
    QueryOntologyCatalog(
        IMU.Q.acceleration.x.gt(0.5),              # Unique field
        IMU.Q.acceleration.y.lt(1.0),              # Unique field
        IMU.Q.angular_velocity.x.between([0, 1]),   # Correct way to do ranges
        include_timestamp_range=True
    )

    ```
