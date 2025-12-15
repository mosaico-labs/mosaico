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
| **`with_name(name)`** | `str` | Exact match for the sequence name. |
| **`with_name_match(pattern)`** | `str` | Substring/Pattern match on the name (e.g., "drive\_2023"). |
| **`with_created_timestamp(start, end)`** | `Time` | Filters sequences created within the given time range. If only `start` is provided, acts as **greater-than**; if only `end` is provided, acts as **less-than**; if both are provided, acts as **between**.|
| **`with_expression(expr)`/Constructor** | `Expression` | **Only** for `Sequence.Q.user_metadata`|

### `QueryTopic`

Filters specific topics within a sequence.

  * **Attributes:** Must be queried via **Convenience Methods** `with_<attribute>`.
  * **User Metadata:** Must be queried via **`with_expression`** and the `Topic.Q` proxy.

| Method | Argument | Description |
| :--- | :--- | :--- |
| **`with_name_match(pattern)`** | `str` | Substring match on the topic name (e.g., "camera/front"). |
| **`with_ontology_tag(tag)`** | `str` | Performs an exact match on the data type tag. It is strongly recommended to programmatically retrieve the tag from the model class (e.g., `with_ontology_tag(GPS.ontology_tag())`) rather than using hardcoded strings. |
| **`with_created_timestamp(start, end)`** | `Time` | Filters topics created within the given time range. If only `start` is provided, acts as **greater-than**; if only `end` is provided, acts as **less-than**; if both are provided, acts as **between**. |
| **`with_expression(expr)`/Constructor** | `Expression` | **Only** for `Topic.Q.user_metadata`. |


> [\!NOTE]
> **Querying `user_metadata` in Topic/Sequence**
>
> The `user_metadata` field supports all [available operators](https://www.google.com/search?q=%23supported-operators). To query a value, access the specific metadata key using bracket notation (`[]`) and chain the desired comparison method.
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

  * **All Data Fields:** Must be queried via **`with_expression`** using the specific class proxy (e.g., `IMU.Q`, `GPS.Q`).
  * **Timestamps:** Special helper methods exist for the standard header timestamps.

| Method | Argument | Description |
| :--- | :--- | :--- |
| **`with_message_timestamp(type, start, end)`** | `Type`, `Time` | Filters by message reception timestamp (middleware/platform time). If only `start` is provided, acts as **greater-than**; if only `end` is provided, acts as **less-than**; if both are provided, acts as **between**. |
| **`with_data_timestamp(type, start, end)`** | `Type`, `Time` | Filters by the sensor's internal `header.stamp` (measurement generation time). Follows the same logic as `with_message_timestamp`. |
| **`with_expression(expr)` / Constructor** | `Expression` | Applies complex filters to **any** ontology field (e.g., `acceleration`, `position`). |

## Current Limitations

The current implementation imposes specific constraints on query structure. These limitations are expected to be removed in future releases.

1.  **Single Occurrence per Field:** A specific field may appear only once within a single query builder instance. It is currently not possible to chain multiple conditions on the *exact same* field path (e.g., manually constructing a range filter).

    The following code is **NOT allowed**:

    ```python
    # Invalid: Same field used twice
    QueryOntologyCatalog()
        .with_expression(IMU.Q.acceleration.x.gt(0.5))
        .with_expression(IMU.Q.acceleration.x.lt(1.0))
    ```

      * **Workaround**: Use the `.between()` operator where applicable.
      * **Clarification**: This restriction applies only to the specific field path. It is fully supported to query **multiple different fields** from the same model within a single query. 
      
    The following code is **Valid**:

    ```python
    # Valid: Different fields
    QueryOntologyCatalog()
        .with_expression(IMU.Q.acceleration.x.gt(0.5))
        .with_expression(IMU.Q.acceleration.y.lt(1.0))
        .with_expression(IMU.Q.angular_velocity.x.between([0, 1]))
    ```

2.  **Single Sensor Model per Query:** A `QueryOntologyCatalog` instance supports expressions from only one ontology type at a time. Mixing different sensor models in the same catalog query is not permitted in the current version of the library.

    The following code is **NOT allowed**:

    ```python
    # Invalid: Mixing IMU and GPS in the same builder
    QueryOntologyCatalog()
        .with_expression(IMU.Q.acceleration.x.gt(0.5))
        .with_expression(GPS.Q.status.service.eq(2))
    ```

      * **Workaround**: To filter by multiple sensor criteria, you must construct separate queries for each sensor type, execute them independently via the client, and perform an intersection of the resulting sequences and topics on the application side.


## Query Execution & Examples

Queries are executed via the `MosaicoClient` object. The `.query()` method accepts one or more Query Builder objects. When multiple builders are provided, they are combined with a logical **AND**.

### Complex Multi-Level Query

*Example: Retrieve data where the GPS service is equal to a certain vaue (generally an enum-based) (`==1`), restricted to topics tagged with the "UART" interface*

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
    print(f"Found sequence: {item.sequence}")
    print(f"Matching topics: {item.topics}")
```

### Filtering by Sequence and Data

*Example: Retrieves specific high-brake events that occurred during all the test campaigns which name matches a specific substring.*

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
The query return is a `List[QueryResponseItem] | None`. Each object groups results by **Sequence**, providing the sequence identifier and the list of specific **Topics** within that sequence that matched the query criteria.

**Class: `QueryResponseItem`**

| Attribute | Type | Description |
| --- | --- | --- |
| **`sequence`** | `str` | The unique identifier (name) of the sequence. |
| **`topics`** | `List[str]` | A list of topic names belonging to this sequence that satisfied the filter conditions. |

> [!NOTE]
> **Topic Name Normalization**
>
> The raw response from the backend returns fully qualified resource names (e.g., `"sequence_name/topic/path"`).
> The `QueryResponseItem` automatically processes these strings during initialization. Therefore, the **`topics`** attribute exposes only the relative topic path (e.g., `"/topic/path"`), stripping the sequence prefix for easier usage.

**Example Usage:**

```python
results = client.query(...)

for item in results:
    print(f"Sequence: {item.sequence}")
    # topics list contains relative paths, e.g., '/sensors/imu'
    for topic_name in item.topics:
        print(f" - Found matching topic: {topic_name}")

```

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