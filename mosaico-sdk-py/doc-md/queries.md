<div align="right">
<picture>
<source media="(prefers-color-scheme: light)" srcset="../../logo/mono_black.svg">
<source media="(prefers-color-scheme: dark)" srcset="../../logo/mono_white.svg">
<img alt="Mosaico logo" src="../../logo/color_white.svg" height="30">
</picture>
</div>

# Query Module Documentation

> [!NOTE]
> **Preliminary Design**
> 
> This query API is currently in a development state and can be subject to significant changes. While the current interface (Proxies + Builders) is fully functional for the supported use cases, the design is subject to evolution. Future versions may introduce more streamlined patterns for complex nested queries or additional operator support.

## Architecture & Rationale

The Query Module addresses the "stringly-typed" problem common in data filtering APIs. Rather than relying on error-prone, manually constructed dictionary keys (e.g., `"gps.position.x": ...`), the module implements a **Fluent Interface** pattern powered by a **Query Proxy**.

### The `_QueryProxy` (`.Q`) Mechanism

Every data model inheriting from `Serializable` (such as `IMU`, `GPS`, `Image`) is automatically injected with a static `.Q` attribute during class initialization. This attribute is an instance of `_QueryProxy`.

1.  **Schema Mapping & Initialization**: During initialization, the system inspects the model's schema and generates a comprehensive dictionary structure. This structure maps specific field paths (e.g., `"imu.acceleration.x"`, `"gps.header.stamp.sec"`) to composed *queryable* objects, determined by the field's data type (e.g., `int` or `float` maps to `_QueryableNumeric`; `bool` maps to `_QueryableBool`). This composition ensures that only operators valid for that specific data type (such as `.gt()` for numbers or `.match()` for strings) are exposed.
2.  **Attribute Resolution**: Resolution occurs within the `__getattr__` method of the `.Q` instance. When an expression such as `IMU.Q.acceleration.x` is evaluated, the proxy resolves the attribute access to the corresponding string path (e.g., `"imu.acceleration.x"`). This path is used to retrieve the pre-composed field instance from the internal dictionary. The returned object then exposes the appropriate operator methods (e.g., `.eq()`, `.match()`) for query construction.
3.  **Expression Generation**: Invoking an operator method (e.g., `.gt(5)`) generates a `QueryExpression` object. This object encapsulates the *intent* of the query, which is subsequently serialized into the JSON format expected by the Data Platform.

## Query Construction Approaches

The Query Module provides two distinct approaches to defining filters. Both approaches support **method chaining**, allowing multiple criteria to be composed into a single query (logical AND).

### A. Convenience Methods (`with_<attribute>`)

These high-level helper methods are built directly into the builder classes. Usage does not require knowledge of the internal model schema. The value to be searched is provided directly, and the builder automatically selects the correct field and operator (e.g., exact match vs. substring match).

  * **Best for:** Standard system fields (Names, Timestamps).
  * **Composition:** Multiple `with_*` methods can be chained to refine the search results.
  * **Usage:**
    ```python
    # Example: Filter by name pattern AND creation time
    QuerySequence()
        .with_name_match("test_drive")
        .with_created_timestamp(start=t1, end=t2)
    ```

### B. The Generic Expression Method (`with_expression` & Constructor)

This interface provides full control over query construction. It accepts raw **Query Expressions** generated via the `.Q` proxy, enabling the application of any supported operator (`>`, `<`, `!=`, `in`, etc.) to specific fields.

  * **Best for:** specific Ontology Data fields and `user_metadata`.
  * **Composition:** Multiple expressions are combined using a logical **AND**.
  * **Initialization:** Expressions can be passed directly to the builder's constructor or added iteratively via method chaining.

**Example 1: Method Chaining**

```python
QueryOntologyCatalog()
  .with_expression(IMU.Q.acceleration.x.gt(5.0))
  .with_expression(IMU.Q.header.stamp.sec.gt(1700134567))
  .with_expression(IMU.Q.header.stamp.nanosec.between([123456, 789123]))
```

**Example 2: Constructor Initialization**

```python
# Pass expressions directly to the constructor for a more concise syntax
QueryOntologyCatalog(
    IMU.Q.acceleration.x.gt(5.0),
    IMU.Q.header.stamp.sec.gt(1700134567),
    IMU.Q.header.stamp.nanosec.between([123456, 789123])
)
```


## Query Builders API Reference

### `QuerySequence`

Filters sequences based on high-level metadata.

  * **Attributes:** Must be queried via **Convenience Methods** `with_<attribute>`.
  * **User Metadata:** Must be queried via **`with_expression`** and the `Sequence.Q` proxy.

| Method | Argument | Description |
| :--- | :--- | :--- |
| **`__init__(*exprs)`** | `Expression` | Initializes the query with variable number of expressions (valid **Only** for `Sequence.Q.user_metadata`) |
| **`with_name(name)`** | `str` | Exact match for the sequence name. |
| **`with_name_match(pattern)`** | `str` | Substring/Pattern match on the name (e.g., "drive\_2023"). |
| **`with_created_timestamp(start, end)`** | `Time` | Filters sequences created within the given time range. If only `start` is provided, acts as **greater-than**; if only `end` is provided, acts as **less-than**; if both are provided, acts as **between**.|
| **`with_expression(expr)`** | `Expression` | **Only** for `Sequence.Q.user_metadata`|

### `QueryTopic`

Filters specific topics within a sequence.

  * **Attributes:** Must be queried via **Convenience Methods** `with_<attribute>`.
  * **User Metadata:** Must be queried via **`with_expression`** and the `Topic.Q` proxy.

| Method | Argument | Description |
| :--- | :--- | :--- |
| **`__init__(*exprs)`** | `Expression` | Initializes the query with variable number of expressions (valid **Only** for `Topic.Q.user_metadata`) |
| **`with_name_match(pattern)`** | `str` | Substring match on the topic name (e.g., "camera/front"). |
| **`with_ontology_tag(tag)`** | `str` | Performs an exact match on the data type tag. It is strongly recommended to programmatically retrieve the tag from the model class (e.g., `with_ontology_tag(GPS.ontology_tag())`) rather than using hardcoded strings. |
| **`with_created_timestamp(start, end)`** | `Time` | Filters topics created within the given time range. If only `start` is provided, acts as **greater-than**; if only `end` is provided, acts as **less-than**; if both are provided, acts as **between**. |
| **`with_expression(expr)`/Constructor** | `Expression` | **Only** for `Topic.Q.user_metadata`. |


> [\!NOTE]
> **Querying `user_metadata` in Topic/Sequence**
>
> The `user_metadata` field supports all [available operators](#supported-operators). To query a value, access the specific metadata key using bracket notation (`[]`) and chain the desired comparison method.
>
> For nested dictionaries, use **dot notation** (`.`) within the key string to traverse sub-fields.
>
> **Important:** You must use the exact key name defined in the metadata.
>
> **Examples:**
>
> ```python
> Sequence.Q.user_metadata['driver'].match('Mark')
> Sequence.Q.user_metadata['environment.visibility'].lt(50)  # Dot notation for nested fields
> ```

### `QueryOntologyCatalog`

Filters the actual time-series data content inside the topics.

* **All Data Fields:** Can be queried by passing expressions directly to the **constructor** or via **`with_expression`** using the specific class proxy (e.g., `IMU.Q`, `GPS.Q`).
* **Timestamps:** Special helper methods exist for the standard header timestamps.
* **Range Metadata:** The query can be configured to return the temporal bounds (start/end) of the matching data.

| Method | Argument | Description |
| :--- | :--- | :--- |
| **`__init__(*exprs, include_timestamp_range)`** | `Expression`, `bool` | Initializes the query with a variable number of expressions. If **`include_timestamp_range`** is `True`, the server returns the start and end timestamps corresponding to the first and last time the queried conditions occurred. |
| **`with_message_timestamp(type, start, end)`** | `Type`, `Time` | Filters by message reception timestamp (middleware/platform time). If only `start` is provided, acts as **greater-than**; if only `end` is provided, acts as **less-than**; if both are provided, acts as **between**. |
| **`with_data_timestamp(type, start, end)`** | `Type`, `Time` | Filters by the data internal `header.stamp` (measurement generation time). Follows the same logic as `with_message_timestamp`. |
| **`with_expression(expr)`** | `Expression` | Applies complex filters to **any** ontology field (e.g., `acceleration`, `position`). |

## Current Limitations

The current implementation imposes specific constraints on query structure. These limitations are expected to be removed in future releases.

**Single Occurrence per Field:** 

A specific field may appear only once within a single query builder instance. It is currently not possible to chain multiple conditions on the *exact same* field path (e.g., manually constructing a range filter), even if passed as separate arguments to the constructor. The following code is **NOT allowed**:
```python
# Invalid: Same field used twice
QueryOntologyCatalog(
    IMU.Q.acceleration.x.gt(0.5),
    IMU.Q.acceleration.x.lt(1.0)
)
# Also Invalid
QueryOntologyCatalog()
    .with_expression(IMU.Q.acceleration.x.gt(0.5))
    .with_expression(IMU.Q.acceleration.x.lt(1.0)) # <- same field
```


* **Workaround**: Use the `.between()` operator where applicable.
* **Clarification**: This restriction applies only to the specific field path. It is fully supported to query **multiple different fields** from the same model within a single query.


The following code is **Valid**:

```python
# Valid: Different fields
QueryOntologyCatalog(
    IMU.Q.acceleration.x.gt(0.5),
    IMU.Q.acceleration.y.lt(1.0), # <- different field
    IMU.Q.angular_velocity.x.between([0, 1]),
    include_timestamp_range=True
)
```


## Query Execution & Examples

Queries are executed via the `MosaicoClient` object. The `.query()` method accepts one or more Query Builder objects. When multiple builders are provided, they are combined with a logical **AND**.

1. *Example: Retrieve data where the GPS service is equal to a certain vaue (generally an enum-based) (`==1`), restricted to topics tagged with the "UART" interface*

  ```python
  from mosaicolabs.models.query import QueryOntologyCatalog, QueryTopic
  from mosaicolabs.models.platform import Topic
  from mosaicolabs.models.sensors import GPS

  # Execute the query
  query_resp = client.query(
      # Filter 1: Data Content (Ontology) -> Use .Q proxy
      QueryOntologyCatalog().with_expression(
          GPS.Q.status.service.eq(1)
      ),
      # Filter 2: Topic Metadata (Platform) -> Use .Q proxy for user_metadata
      QueryTopic().with_expression(
          Topic.Q.user_metadata["interface.type"].eq("UART")
      ),
  )

  # Process results
  for item in query_resp:
      print(f"Found sequence: {item.sequence.name}")
      print(f"Matching topics: {[t.name for t in item.topics]}")
  ```

2. *Example: Retrieves specific high-brake events that occurred during all the test campaigns which name matches a specific substring.*

  ```python
  from mosaicolabs.models.query import QueryOntologyCatalog, QuerySequence
  from mosaicolabs.models.sensors import IMU

  results = client.query(
      # Filter 1: Sequence Name -> Use Convenience Method
      QuerySequence().with_name_match("winter_test_2023"), # all names that match `*winter_test_2023*`

      # Filter 2: IMU Data Threshold -> Use .Q proxy
      # We assume that the IMU x axis is aligned to the vehicle's longitudinal axis
      QueryOntologyCatalog().with_expression(
          IMU.Q.acceleration.x.lt(-6.0)
      )
  )
  ```

### Query Response

The query returns a `QueryResponse` object. This class acts as a container for `QueryResponseItem` objects via the `items` field. Each item groups results by **Sequence**, providing the sequence identifier and the list of specific **Topics** within that sequence that matched the query criteria.

| Attribute | Type | Description |
| --- | --- | --- |
| **`items`** | `List[QueryResponseItem]` | A list of items containing the sequence and related topic objects that satisfied the filter conditions. |

The class behaves like a standard Python list. You can:

* **Iterate** over items (`for item in results`).
* **Check length** (`len(results)`).
* **Access by index** (`results[0]`).
* **Check emptiness** via the helper method `results.is_empty()`.


**Class: `QueryResponseItem`**

| Attribute | Type | Description |
| --- | --- | --- |
| **`sequence`** | `QueryResponseItemSequence` | Object containing the unique identifier (name) of the sequence. |
| **`topics`** | `List[QueryResponseItemTopic]` | A list of topic objects belonging to this sequence that satisfied the filter conditions. |

**Class: `QueryResponseItemSequence`**

| Attribute | Type | Description |
| --- | --- | --- |
| **`name`** | `str` | The unique identifier (name) of the sequence. |

**Class: `QueryResponseItemTopic`**

| Attribute | Type | Description |
| --- | --- | --- |
| **`name`** | `str` | The normalized relative topic path (e.g., `"/topic/path"`). The prefix is automatically stripped. |
| **`timestamp_range`** | `TimestampRange\|None` | The start and end timestamps corresponding to the first and last time the queried conditions occurred.

> [!NOTE]
> **Topic Name Normalization**
>
> The raw response from the backend returns fully qualified resource names (e.g., `"sequence_name/topic/path"`). The `QueryResponseItemTopic` automatically processes these strings during initialization. Therefore, the **`name`** attribute exposes only the normalized relative topic path (e.g., `"/topic/path"`), stripping the sequence prefix for easier usage.

**Example Usage:**

```python
results = client.query(...) # The return is a QueryResponse

if results and not results.is_empty():
      print(f"Total sequences found: {len(results)}")
    
    # Access by index
    first_item = results[0]
    for item in results: # Iterate like a list
        print(f"Sequence: {item.sequence.name}")
        
        for it_topic in item.topics:
            print(f" - Found matching topic: {it_topic.name}")
            
            # Access timestamp range if requested
            if it_topic.timestamp_range:
                start = it_topic.timestamp_range.start
                end = it_topic.timestamp_range.end
                print(f"   Matches found between: {start} and {end}")

```

## Restricted Queries (Chaining)

The `QueryResponse` class enables **search refinement** through factory methods that convert current results back into a new query builder.

| Method | Return type | Description |
| --- | --- | --- |
| **`to_query_sequence()`** | `QuerySequence` | Returns a query builder pre-filtered to include only the **sequences** present in this response. |
| **`to_query_topic()`** | `QueryTopic` | Returns a query builder pre-filtered to include only the **topics** present in this response. |

These methods effectively "lock" the domain of the search. When you call `to_query_sequence()`, it generates a new query expression containing an explicit `$in` filter populated with the list of sequence names currently held in the response. Similarly, `to_query_topic()` restricts future queries to the specific topic paths identified in the result set.

This mechanism allows you to perform **iterative refinement**: taking a broad result set and applying *new* conditions specifically to that subset without re-scanning the entire catalog. This is particularly powerful for **resolving ambiguous or multi-modal dependencies** by breaking a complex question into logical steps and normalizing situations where a single monolithic query would be confusing or inefficient.

**Example Scenario**
*Retrieve all sequences where a high-precision GPS fix (`status==2`) is present, AND where the Localization filter specifically reported an error.*

By chaining queries, we ensure we only look for the error string within the context of valid GPS sequences.

```python
# Initial Query: Find all sequences where a valid GPS fix (status==2) was received
initial_response = client.query(
    QueryOntologyCatalog(GPS.Q.status.status.eq(2))
)

# Refinement: Create a new query restricted ONLY to the sequences found above.
# This effectively says: "Focus ONLY on the sequences from 'initial_response'..."
refined_query = initial_response.to_query_sequence()

# Final Query: Apply the specific logic (Error string) to the restricted scope
final_response = client.query(
    refined_query,                                         # The restricted scope
    QueryTopic().with_name("/localization/log_string"),    # Filter by specific topic
    QueryOntologyCatalog(String.Q.data.match("[ERR]"))     # Search for the error pattern
)

```

### Technical Note: The Necessity of Chaining

It is important to understand that the **Restricted Query (Chaining)** pattern described above is not just a workaround for current software limitations; it is the correct architectural pattern for multi-modal correlation. Even when future releases enable querying [multiple data models simultaneously](#current-limitations), a single monolithic query will often yield incorrect results due to **logical ambiguity**. By providing multiple criteria in a single `client.query()` call, the server attempts to find the *data streams* that satisfy **all** conditions simultaneously (AND logic). Consider the following query:

```python
# Ambiguous Query: Will return 0 results
response = client.query(
    # Criteria 1: Must be GPS data
    QueryOntologyCatalog(GPS.Q.status.status.eq(2)),
    # Criteria 2: Must be String data
    QueryOntologyCatalog(String.Q.data.match("[ERR]")),
    # Criteria 3: Topic name must be specific
    QueryTopic().with_name("/localization/log_string")
)

```

**This query will fail and return zero results.**
The server interprets this request as: *"Find a topic that is a **GPS** sensor AND is a **String** message AND is named `/localization/log_string`."*

This intersection is logically impossible because:

1. A single topic cannot be both type `GPS` and type `String` at the same time.
2. The topic `/localization/log_string` (which contains Strings) will fail the check for `GPS` criteria immediately.

By using **Chaining**, you decouple the criteria into logical steps:

1. **Step 1:** Find the *Sequences* that contain valid GPS data.
2. **Step 2:** Within *those specific sequences*, search for the Error string in the right topic.


## Supported Operators

The operators available depend on the data type of the field being queried.

### Numeric Fields (`int`, `float`)

  * **Applies to (e.g.):** `GPS.Q.latitude`, `IMU.Q.acceleration.x`, `Header.Q.stamp.sec`, etc.
  * **Operators:**
      * `.eq(value)`: Equal to.
      * `.neq(value)`: Not equal to.
      * `.lt(value)`: Less than.
      * `.leq(value)`: Less than or equal to.
      * `.gt(value)`: Greater than.
      * `.geq(value)`: Greater than or equal to.
      * `.between([min, max])`: Value is within the inclusive range.
      * `.in_([v1, v2, ...])`: Value matches one of the options.

### String Fields (`str`)

  * **Applies to (e.g.):** `Image.Q.encoding`, `Header.Q.frame_id`, etc.
  * **Operators:**
      * `.eq("value")`: Exact match.
      * `.neq("value")`: Not equal.
      * `.match("pattern")`: Substring match.
      * `.in_(["a", "b"])`: Match one of the list.

### Boolean Fields (`bool`)

  * **Applies to (e.g.):** `Image.Q.is_bigendian`, `ROI.Q.do_rectify`.
  * **Operators:**
      * `.eq(True/False)`

### Dynamic Metadata (`dict` / `user_metadata`)

  * **Applies to:** `Sequence.Q.user_metadata["key"]`, `Topic.Q.user_metadata["key"]`.
  * **Behavior:** Since metadata values are dynamic, these fields are **promiscuous**. All operators (Numeric, String, and Bool) are allowed without strict type checking at the SDK level.
  * **Syntax:** Square brackets `["key"]` must be used instead of dot notation.

### Unsupported / Skipped Types

When querying **Ontology Catalog Data** (e.g., `IMU`, `GPS`), the `QueryProxy` enforces specific limitations on queryable types:

1.  **Supported:**
      * Base primitive types (`int`, `float`, `str`, `bool`).
      * Nested Models: Drill-down into nested objects (e.g., `Vector3d` inside `IMU`) is supported.
      * Dictionaries: Keys can be queried (`["key"]`) similar to metadata.
2.  **Unsupported / Skipped:**
      * **Container Types (Lists/Tuples):** Querying elements inside a list or tuple is **not supported**. Fields defined as containers (e.g., `covariance: List[float]`) are skipped by the proxy generator and will not appear in autocomplete. This features may be made available in future releases.