# Queries

Mosaico distinguishes itself from simple file stores with a powerful **Query System** capable of filtering data based on both high-level metadata and content values. The query engine operates through the [`query`](actions.md#query) action, accepting structured JSON-based filter expressions that can span the entire data hierarchy.

## Query Architecture

The query engine is designed around a three-tier filtering model that allows you to construct complex, multi-dimensional searches:

**Sequence Filtering.** Target recordings by structural attributes like sequence name, creation timestamp, or user-defined metadata tags. This level allows you to narrow down which recording sessions are relevant to your search.

**Topic Filtering.** Refine your search to specific data streams within sequences. You can filter by topic name, ontology tag (the data type), serialization format, or topic-level user metadata.

**Ontology Filtering.** Query the actual physical values recorded inside the sensor data without scanning terabytes of files. The engine leverages statistical indices computed during ingestion, min/max bounds stored in the metadata cache for each chunk, to rapidly include or exclude entire segments of data.

## Filter Domains

### Sequence Filter

The sequence filter allows you to target specific recording sessions based on their metadata:

| Field                       | Description                                                  |
| --------------------------- | ------------------------------------------------------------ |
| `sequence.name`             | The sequence identifier (supports text operations)           |
| `sequence.creation`         | The creation timestamp in nanoseconds (supports timestamp operations) |
| `sequence.user_metadata.<key>` | Custom user-defined metadata attached to the sequence        |

### Topic Filter

The topic filter narrows the search to specific data streams within matching sequences:

| Field                          | Description                                                  |
| ------------------------------ | ------------------------------------------------------------ |
| `topic.name`                   | The topic path within the sequence (supports text operations) |
| `topic.creation`               | The topic creation timestamp in nanoseconds (supports timestamp operations) |
| `topic.ontology_tag`           | The data type identifier (e.g., `Lidar`, `Camera`, `IMU`)    |
| `topic.serialization_format`   | The binary layout format (`Default`, `Ragged`, or `Image`)   |
| `topic.user_metadata.<key>`    | Custom user-defined metadata attached to the topic           |

### Ontology Filter

The ontology filter queries the actual sensor data values. Fields are specified using dot notation: `<ontology_tag>.<field_path>`.

For example, to query IMU acceleration data: `imu.acceleration.x`, where `imu` is the ontology tag and `acceleration.x` is the field path within that data model.

#### Timestamp query support

If `include_timestamp_range` is set to `true` the response will also return [timestamps ranges](#timestamps) for each query.

## Supported Operators

The query engine supports a rich set of comparison operators. Each operator is prefixed with `$` in the JSON syntax:

| Operator | Description |
| --- | --- |
| `$eq` | Equal to (supports all types) |
| `$neq` | Not equal to (supports all types) |
| `$lt` | Less than (numeric and timestamp only) |
| `$gt` | Greater than (numeric and timestamp only) |
| `$leq` | Less than or equal to (numeric and timestamp only) |
| `$geq` | Greater than or equal to (numeric and timestamp only) |
| `$between` | Within a range `[min, max]` inclusive (numeric and timestamp only) |
| `$in` | Value is in a set of options (supports integers and text) |
| `$match` | Matches a pattern (text only, supports SQL LIKE patterns with `%` wildcards) |
| `$ex` | Field exists |
| `$nex` | Field does not exist |

## Query Syntax

Queries are submitted as JSON objects. Each field is mapped to an operator and value. Multiple conditions are combined with implicit AND logic.

```json hl_lines="15"
{
  "sequence": {
    "name": { "$match": "test_run_%" },
    "user_metadata": {
      "driver": { "$eq": "Alice" }
    }
  },
  "topic": {
    "ontology_tag": { "$eq": "imu" }
  },
  "ontology": {
    "imu.acceleration.x": { "$gt": 5.0 },
    "imu.acceleration.y": { "$between": [-2.0, 2.0] },

    "include_timestamp_range": true, // (1)!
  }
}
```

1. This filed is optional, if set to `true` the query returns the [timestamp ranges](#timestamps)

This query searches for:

- Sequences with names matching `test_run_%` pattern
- Where the user metadata field `driver` equals `"Alice"`
- Containing topics with ontology tag `imu`
- Where the IMU's x-axis acceleration exceeds 5.0
- And the y-axis acceleration is between -2.0 and 2.0

## Response Structure

The query response is hierarchically grouped by sequence. For each matching sequence, it provides the list of topics that satisfied the filter criteria, along with optional timestamp ranges indicating when the ontology conditions were met.

```json title="Query response example"
{
  "items": [
    {
      "sequence": "test_run_01",
      "topics": [
        { 
          "locator": "test_run_01/sensors/imu",
          "timestamp_range": [1000000000, 2000000000]
        },
        {
          "locator": "test_run_01/sensors/gps",
          "timestamp_range": [1000000000, 2000000000]
        }
      ]
    },
    {
      "sequence": "test_run_02",
      "topics": [
        {
          "locator": "test_run_02/camera/front",
          "timestamp_range": [1500000000, 2500000000]
        },
        {
          "locator": "test_run_02/lidar/point_cloud",
          "timestamp_range": [1500000000, 2500000000]
        }
      ]
    }
  ]
}
```

### Timestamps

It returns the time window `[min, max]` where the filter conditions were met for that topic, with `min` being the timestamp of the first matching event and max being the timestamp of the last matching event. This allows you to retrieve only the relevant data slices using the [retrieval protocol](retrieval.md#the-retrieval-protocol).

!!! note 
    The `timestamp_range` field is included only when ontology filters are applied and `include_timestamp_range` is set to `true` inside the `ontology` filter. 

## Performance Characteristics

The query engine is optimized for high performance by minimizing unnecessary data retrieval and I/O operations. 
During execution, the engine uses index-based pruning to evaluate precomputed min/max statistics and skip indices, allowing it to bypass irrelevant data chunks without reading the underlying files. 

Performance is further improved by executing metadata cache queries, such as sequence and topic filters, directly within the database, which ensures sub-second response times even across thousands of sequences.

The system employs **lazy evaluation** to keep network payloads lightweight; instead of returning raw data immediately, queries return locators and timestamp ranges. This architecture allows client applications to fetch only the required data slices via the retrieval protocol as needed.
