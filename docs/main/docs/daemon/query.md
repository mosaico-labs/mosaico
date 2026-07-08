---
title: Query
sidebar_position: 7
description: "How mosaicod's query engine works. Covers the JSON filter expression format, hierarchy traversal across Sequences, Topics, and Ontology field values, and the query action API used by SDK clients."
---

Mosaico distinguishes itself from simple file stores with a powerful **Query System** capable of filtering data based on both high-level metadata and content values. The query engine operates through the [`query`](actions.md#query) action, accepting structured JSON-based filter expressions that can span the entire data hierarchy.

## Architecture

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
| `sequence.created_at`       | The creation timestamp in nanoseconds (supports timestamp operations) |
| `sequence.user_metadata.<key>` | Custom user-defined metadata attached to the sequence        |

### Topic Filter

The topic filter narrows the search to specific data streams within matching sequences:

| Field                          | Description                                                  |
| ------------------------------ | ------------------------------------------------------------ |
| `topic.name`                   | The topic path within the sequence (supports text operations) |
| `topic.created_at`             | The topic creation timestamp in nanoseconds (supports timestamp operations) |
| `topic.ontology_tag`           | The data type identifier (e.g., `Lidar`, `Camera`, `IMU`)    |
| `topic.serialization_format`   | The binary layout format (`Default`, `Ragged`, or `Image`)   |
| `topic.user_metadata.<key>`    | Custom user-defined metadata attached to the topic           |

### Ontology Filter

The ontology filter queries the actual sensor data values. Fields are specified using dot notation: `<ontology_tag>.<field_path>`.

For example, to query IMU acceleration data: `imu.acceleration.x`, where `imu` is the ontology tag and `acceleration.x` is the field path within that data model.

#### Querying list and complex fields

Fields backed by an Arrow **list** column (including lists of structs) are addressed with an **index specifier** appended to the list segment of the path. Exactly one specifier is allowed per field path.

| Specifier | Meaning |
| --- | --- |
| `[i]` | The element at position `i` (0-based) must satisfy the predicate. |
| `[?]` | **At least one** element must satisfy the predicate. |
| `[!]` | **Every** element must satisfy the predicate. |

The specifier attaches to whichever segment is the list, and the path may continue into a struct after it:

```json
{
  "ontology": {
    "lidar.ranges[?]": { "$gt": 50.0 },
    "imu.samples[0].x": { "$eq": 1.0 },
    "robot.readings[!].active": { "$eq": true }
  }
}
```

- `lidar.ranges[?]`: the topic matches if any element of the `ranges` list exceeds `50.0`.
- `imu.samples[0].x`: targets the `x` field of the first struct in the `samples` list of structs.
- `robot.readings[!].active`: every struct in `readings` must have `active` equal to `true`.

A **plain list** column (no specifier) can be compared as a whole with `$eq` / `$neq` against a JSON array. The comparison is element-wise: it holds only when the arrays have the same length and every position matches. The literal is capped at `MOSAICOD_MAX_SIZE_PLAIN_LIST_EQ` elements (default `1024`).

```json
{
  "ontology": {
    "mock.list_test": { "$eq": [3, 4, 5] }
  }
}
```

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
| `$in` | Matches when the field equals **any** value in the list, e.g. `{ "$in": [1, 5, 9] }`. Supports numeric and text values; a single-element list behaves like `$eq`. Lists mixing different value types are rejected. |
| `$match` | Matches a POSIX regular expression (text only). Applies to `sequence.name`, `topic.name`, textual user metadata, and textual ontology fields. |
| `$ex` | Field exists (the column is present). |
| `$nex` | Field does not exist. |

:::note
`$ex` and `$nex` take no value: they are written as a bare string, e.g. `"imu.acceleration.x": "$ex"`. All other operators are written as an object, e.g. `"imu.acceleration.x": { "$gt": 5.0 }`.
:::

## Syntax

Queries are submitted as JSON objects. Each field is mapped to an operator and value. Multiple conditions are combined with implicit AND logic.

```json hl_lines="15" {15}
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
  }
}
```

This query searches for:

- Sequences with names matching `test_run_%` pattern
- Where the user metadata field `driver` equals `"Alice"`
- Containing topics with ontology tag `imu`
- Where the IMU's x-axis acceleration exceeds 5.0
- And the y-axis acceleration is between -2.0 and 2.0

## Response Structure

The query response is hierarchically grouped by sequence. For each matching sequence, it provides the list of topics that satisfied the filter criteria.

```json title="query_response_example"
{
  "items": [
    {
      "sequence": "test_run_01",
      "topics": [
        { 
          "locator": "test_run_01/sensors/imu"
        },
        {
          "locator": "test_run_01/sensors/gps"
        }
      ]
    },
    {
      "sequence": "test_run_02",
      "topics": [
        {
          "locator": "test_run_02/camera/front"
        },
        {
          "locator": "test_run_02/lidar/point_cloud"
        }
      ]
    }
  ]
}
```

### Performance Characteristics

The query engine is optimized for high performance by minimizing unnecessary data retrieval and I/O operations. 
During execution, the engine uses index-based pruning to evaluate precomputed min/max statistics and skip indices, allowing it to bypass irrelevant data chunks without reading the underlying files. 

Performance is further improved by executing metadata cache queries, such as sequence and topic filters, directly within the database, which ensures sub-second response times even across thousands of sequences.

The system employs **lazy evaluation** to keep network payloads light-weight; instead of returning raw data immediately, queries return just sequence and topic locators. This architecture allows client applications to fetch only the required data slices via the retrieval protocol as needed.
