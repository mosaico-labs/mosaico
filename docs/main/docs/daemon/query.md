---
title: Query
sidebar_position: 7
description: "How mosaicod's query engine works. Covers the protobuf filter message format, hierarchy traversal across Sequences, Topics, and Ontology field values, and the query action API used by SDK clients."
---

Mosaico distinguishes itself from simple file stores with a powerful **Query System** capable of filtering data based on both high-level metadata and content values. The query engine operates through the [`query`](actions.md#query) action, accepting a structured protobuf `Filter` message (see `proto/mosaico/v1/query.proto`) that can span the entire data hierarchy.

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

For example, to query IMU acceleration data: `IMU.acceleration.x`, where `IMU` is the ontology tag and `acceleration.x` is the field path within that data model.

#### Querying list and complex fields

Fields backed by an Arrow **list** column (including lists of structs) are addressed with an **index specifier** appended to the list segment of the path. Exactly one specifier is allowed per field path.

| Specifier | Meaning |
| --- | --- |
| `[i]` | The element at position `i` (0-based) must satisfy the predicate. |
| `[?]` | **At least one** element must satisfy the predicate. |
| `[!]` | **Every** element must satisfy the predicate. |

The specifier attaches to whichever segment is the list, and the path may continue into a struct after it:

```protobuf
ontology {
  predicates { field: "lidar.ranges[?]"          condition { op: OPERATOR_GT value { float: 50.0 } } }
  predicates { field: "imu.samples[0].x"         condition { op: OPERATOR_EQ value { float: 1.0 } } }
  predicates { field: "robot.readings[!].active" condition { op: OPERATOR_EQ value { boolean: true } } }
}
```

- `lidar.ranges[?]`: the topic matches if any element of the `ranges` list exceeds `50.0`.
- `imu.samples[0].x`: targets the `x` field of the first struct in the `samples` list of structs.
- `robot.readings[!].active`: every struct in `readings` must have `active` equal to `true`.

A **plain list** column (no specifier) can be compared as a whole with `OPERATOR_EQ` / `OPERATOR_NE` against an array value (e.g. `integer_array`). The comparison is element-wise: it holds only when the arrays have the same length and every position matches. The literal is capped at `MOSAICOD_MAX_SIZE_PLAIN_LIST_EQ` elements (default `1024`).

```protobuf
ontology {
  predicates {
    field: "mock.list_test"
    condition { op: OPERATOR_EQ value { integer_array { values: [3, 4, 5] } } }
  }
}
```

:::warning
**Nested lists (a list of lists) are not supported.** Querying a field whose column is a list whose elements are themselves lists returns an *unsupported operation* error. Only list columns of scalars or of structs can be queried, whether as a plain list or through an index specifier (`[i]`, `[?]`, `[!]`).
:::

#### Aggregators

Regular and aggregated predicates are evaluated at two different levels:

* A **regular predicate** works on the **single samples**: the condition is checked on each sample of the field, and a topic matches if at least one of its samples satisfies it.
* An **aggregated predicate** works on the **whole topic**: the aggregator first combines all the samples of the field in the topic into a single value (e.g. their maximum), and the condition is checked once on that value.

For example, with the condition `imu.acceleration.x < 20.0`:

* as a regular predicate, it matches the topics where **at least one** sample is below `20.0`;
* with `AGGREGATOR_MAX`, it matches the topics whose maximum is below `20.0`, i.e. where **every** sample is below `20.0`.

The aggregator is set in the optional `aggregator` field of each `OntologyPredicate` (see `proto/mosaico/v1/query.proto`): a predicate without the field keeps the regular behavior, evaluated on the single samples.

```protobuf
ontology {
  predicates {
    field: "imu.acceleration.x"
    aggregator: AGGREGATOR_MAX
    condition { op: OPERATOR_LT value { float: 20.0 } }
  }
}
```

| Aggregator | Description | Data types | Operators |
| --- | --- | --- | --- |
| `AGGREGATOR_MAX` | Maximum value of the field over the topic | Numeric, text | `OPERATOR_EQ`, `OPERATOR_NE`, `OPERATOR_LT`, `OPERATOR_LE`, `OPERATOR_GT`, `OPERATOR_GE`, `OPERATOR_IN`, `OPERATOR_BETWEEN`, `OPERATOR_OUTSIDE`, `OPERATOR_MATCH` (text only) |
| `AGGREGATOR_MIN` | Minimum value of the field over the topic | Numeric, text | `OPERATOR_EQ`, `OPERATOR_NE`, `OPERATOR_LT`, `OPERATOR_LE`, `OPERATOR_GT`, `OPERATOR_GE`, `OPERATOR_IN`, `OPERATOR_BETWEEN`, `OPERATOR_OUTSIDE`, `OPERATOR_MATCH` (text only) |
| `AGGREGATOR_RANGE` | Spread of the field over the topic, i.e. `max - min` | Numeric | `OPERATOR_EQ`, `OPERATOR_NE`, `OPERATOR_LT`, `OPERATOR_LE`, `OPERATOR_GT`, `OPERATOR_GE`, `OPERATOR_IN`, `OPERATOR_BETWEEN`, `OPERATOR_OUTSIDE` |
| `AGGREGATOR_COUNT` | Number of samples of the field over the topic, null and NaN included | Numeric, text | `OPERATOR_EQ`, `OPERATOR_NE`, `OPERATOR_LT`, `OPERATOR_LE`, `OPERATOR_GT`, `OPERATOR_GE`, `OPERATOR_IN`, `OPERATOR_BETWEEN`, `OPERATOR_OUTSIDE` |
| `AGGREGATOR_COUNT_VALID` | Number of valid samples of the field over the topic, i.e. neither null nor NaN | Numeric, text | `OPERATOR_EQ`, `OPERATOR_NE`, `OPERATOR_LT`, `OPERATOR_LE`, `OPERATOR_GT`, `OPERATOR_GE`, `OPERATOR_IN`, `OPERATOR_BETWEEN`, `OPERATOR_OUTSIDE` |
| `AGGREGATOR_MEAN` | Mean of the valid samples of the field over the topic | Numeric | `OPERATOR_EQ`, `OPERATOR_NE`, `OPERATOR_LT`, `OPERATOR_LE`, `OPERATOR_GT`, `OPERATOR_GE`, `OPERATOR_IN`, `OPERATOR_BETWEEN`, `OPERATOR_OUTSIDE` |
| `AGGREGATOR_VAR` | Sample variance (divided by `n - 1`) of the valid samples of the field over the topic | Numeric | `OPERATOR_EQ`, `OPERATOR_NE`, `OPERATOR_LT`, `OPERATOR_LE`, `OPERATOR_GT`, `OPERATOR_GE`, `OPERATOR_IN`, `OPERATOR_BETWEEN`, `OPERATOR_OUTSIDE` |
| `AGGREGATOR_STD` | Sample standard deviation of the valid samples of the field over the topic, i.e. the square root of `AGGREGATOR_VAR` | Numeric | `OPERATOR_EQ`, `OPERATOR_NE`, `OPERATOR_LT`, `OPERATOR_LE`, `OPERATOR_GT`, `OPERATOR_GE`, `OPERATOR_IN`, `OPERATOR_BETWEEN`, `OPERATOR_OUTSIDE` |
| `AGGREGATOR_FIRST` | Value of the field at the first timestamp of the topic | Numeric, text | `OPERATOR_EQ`, `OPERATOR_NE`, `OPERATOR_LT`, `OPERATOR_LE`, `OPERATOR_GT`, `OPERATOR_GE`, `OPERATOR_IN`, `OPERATOR_BETWEEN`, `OPERATOR_OUTSIDE`, `OPERATOR_MATCH` (text only) |
| `AGGREGATOR_LAST` | Value of the field at the last timestamp of the topic | Numeric, text | `OPERATOR_EQ`, `OPERATOR_NE`, `OPERATOR_LT`, `OPERATOR_LE`, `OPERATOR_GT`, `OPERATOR_GE`, `OPERATOR_IN`, `OPERATOR_BETWEEN`, `OPERATOR_OUTSIDE`, `OPERATOR_MATCH` (text only) |
| `AGGREGATOR_RATE` | Samples per second (Hz) of the field over the topic, null and NaN included, i.e. `(count - 1) / duration`, where `count` is `AGGREGATOR_COUNT` and the duration goes from the first to the last timestamp of the topic | Numeric, text | `OPERATOR_EQ`, `OPERATOR_NE`, `OPERATOR_LT`, `OPERATOR_LE`, `OPERATOR_GT`, `OPERATOR_GE`, `OPERATOR_IN`, `OPERATOR_BETWEEN`, `OPERATOR_OUTSIDE` |

On text fields, `AGGREGATOR_MAX` and `AGGREGATOR_MIN` compare the values lexicographically (bytewise), and `OPERATOR_MATCH` checks the pattern against the aggregated value, e.g. `AGGREGATOR_MAX` with `OPERATOR_MATCH` `"z*"` matches the topics whose greatest value starts with `z`. The same holds for `AGGREGATOR_FIRST` and `AGGREGATOR_LAST`.

`AGGREGATOR_COUNT`, `AGGREGATOR_COUNT_VALID` and `AGGREGATOR_RATE` always compare against a number, whatever the type of the field, e.g. `AGGREGATOR_COUNT` with `OPERATOR_GT` `100` on a text field.

Some aggregated values can't be computed for a topic, e.g. the mean of a field that is always null, or the standard deviation of a single sample. Such a topic is never returned by a predicate using that aggregator, whatever the condition. This happens when:
* `AGGREGATOR_FIRST` or `AGGREGATOR_LAST` find a null or NaN value at the first or last timestamp of the topic;
* `AGGREGATOR_MEAN` finds no valid sample;
* `AGGREGATOR_VAR` or `AGGREGATOR_STD` find less than two valid samples;
* `AGGREGATOR_MEAN`, `AGGREGATOR_VAR` or `AGGREGATOR_STD` find an infinite value (`+inf` or `-inf`) among the valid samples;
* `AGGREGATOR_RATE` finds less than two samples;
* `AGGREGATOR_RATE` finds the first and last timestamps of the topic equal.

`AGGREGATOR_RATE` counts the samples where the field is null or NaN too, so it measures the frequency of the samples of the topic, not of the valid values of the field, e.g. three samples `[null, 2, 5]` 5 ns apart have a rate of `(3 - 1) / 10 ns`.

Aggregated predicates are resolved on the statistics computed for each chunk at ingestion time (e.g. the same min/max bounds used to prune chunks), so they never scan the data stored in the object store.

On a list field, i.e. a field holding a list of values in each sample, only `AGGREGATOR_MAX`, `AGGREGATOR_MIN`, `AGGREGATOR_RANGE`, `AGGREGATOR_COUNT` and `AGGREGATOR_RATE` are supported, and the field path must not have an index specifier. They work at two different levels:
* `AGGREGATOR_MAX`, `AGGREGATOR_MIN` and `AGGREGATOR_RANGE` look at the **elements** of all the lists of the topic, as if they were a single flat sequence of values;
* `AGGREGATOR_COUNT` and `AGGREGATOR_RATE` look at the **samples**, whatever the length of their lists.

For example, a topic with three samples of the list field `scan.ranges`, `[1, 4]`, `[7]` and `[2, 3, 9]`, 1 second apart:

| Aggregator | Value | Why |
| --- | --- | --- |
| `AGGREGATOR_MAX` | `9` | greatest element of all the lists |
| `AGGREGATOR_MIN` | `1` | smallest element of all the lists |
| `AGGREGATOR_RANGE` | `8` | `9 - 1` |
| `AGGREGATOR_COUNT` | `3` | three samples, not six elements |
| `AGGREGATOR_RATE` | `1` Hz | `(3 - 1) / 2 s` |

The same rules hold for lists of lists and for the fields of a list of structs:
* on a **list of lists**, the elements are the values of the innermost lists, at any depth. E.g. if the samples of `scan.grid` are `[[1, 4], [7]]` and `[[2], [3, 9]]`, `AGGREGATOR_MAX` is `9` and `AGGREGATOR_COUNT` is `2`;
* on a **list of structs**, each field of the struct is a list field, holding the values of that field for all the structs of the sample. E.g. if `scan.points` holds a list of `{x, y}` structs, `AGGREGATOR_MAX` on `scan.points.x` is the greatest `x` of all the points of the topic. The same holds for the nested fields, e.g. `scan.points.pose.x`. A null struct in the list is ignored, like a null value.

:::warning
The following predicates are rejected with an *invalid argument* error:
* An aggregator on a list field with an index specifier (`[i]`, `[?]`, `[!]`).
* An aggregator other than `AGGREGATOR_MAX`, `AGGREGATOR_MIN`, `AGGREGATOR_RANGE`, `AGGREGATOR_COUNT` and `AGGREGATOR_RATE` on a list field (see above).
* An operator not listed for the aggregator in the table above, e.g. `OPERATOR_EX` or `OPERATOR_NEX`, or `OPERATOR_MATCH` with `AGGREGATOR_RANGE`.
* `AGGREGATOR_RANGE`, `AGGREGATOR_MEAN`, `AGGREGATOR_VAR` or `AGGREGATOR_STD` on a text field.
* A text operand with an aggregator other than `AGGREGATOR_MAX`, `AGGREGATOR_MIN`, `AGGREGATOR_FIRST` and `AGGREGATOR_LAST`.
* A boolean operand, e.g. `OPERATOR_EQ` `true`: a boolean has only two values, so comparing it with an aggregated value is meaningless.
* An array operand with `OPERATOR_EQ`, `OPERATOR_NE`, `OPERATOR_LT`, `OPERATOR_LE`, `OPERATOR_GT`, `OPERATOR_GE` or `OPERATOR_MATCH`, e.g. `AGGREGATOR_MAX` with `OPERATOR_EQ` `[1, 2]`. An aggregator always produces a single number or text, even on a list field (see above), so comparing it with a list is meaningless. The array operands of `OPERATOR_IN`, `OPERATOR_BETWEEN` and `OPERATOR_OUTSIDE` are allowed, since they are a set of values or the bounds of a range, e.g. `AGGREGATOR_MAX` with `OPERATOR_IN` `[1, 2]` matches the topics whose maximum is `1` or `2`.
* An `aggregator` field set to `AGGREGATOR_UNSPECIFIED`: a regular predicate must leave the field unset.
* Any aggregator defined in the protocol but not listed in the table above, which are not supported yet.
:::

:::note
Aggregators describe a topic as a whole, not single samples: they select **which topics** match, but not **when**. Aggregated predicates are not supported by the topic `clusterize` and `intersect` actions, which compute the time ranges where the regular predicates are satisfied.
:::

## Supported Operators

The query engine supports a rich set of comparison operators, defined by the `Operator` enum. Each operator is set in the `op` field of a `Condition`, together with the comparison `value`:

| Operator | Description |
| --- | --- |
| `OPERATOR_EQ` | Equal to (supports all types) |
| `OPERATOR_NE` | Not equal to (supports all types) |
| `OPERATOR_LT` | Less than. Supports numeric, text (lexicographic) and timestamp values |
| `OPERATOR_GT` | Greater than (same types as `OPERATOR_LT`) |
| `OPERATOR_LE` | Less than or equal to (same types as `OPERATOR_LT`) |
| `OPERATOR_GE` | Greater than or equal to (same types as `OPERATOR_LT`) |
| `OPERATOR_BETWEEN` | Within a range `[min, max]` inclusive (same types as `OPERATOR_LT`) |
| `OPERATOR_OUTSIDE` | Outside a range `[min, max]`, the strict complement of `OPERATOR_BETWEEN`: matches when `v < min` **or** `v > max` (same types as `OPERATOR_LT`) |
| `OPERATOR_IN` | Matches when the field equals **any** value in the list, e.g. `value { integer_array { values: [1, 5, 9] } }`. Supports numeric, text and boolean values; a single-element list behaves like `OPERATOR_EQ`. |
| `OPERATOR_MATCH` | Matches a pattern (wildcards allowed). Applies to text fields only: `sequence.name`, `topic.name`, `topic.ontology_tag`, `topic.serialization_format`, textual user metadata, and textual ontology fields. |
| `OPERATOR_EX` | Field exists (the column is present). |
| `OPERATOR_NEX` | Field does not exist. |

:::note
`OPERATOR_EX` and `OPERATOR_NEX` take no value, e.g. `condition { op: OPERATOR_EX }`. All other operators require a `value`, e.g. `condition { op: OPERATOR_GT value { float: 5.0 } }`. `OPERATOR_BETWEEN` and `OPERATOR_OUTSIDE` take a two-element array value holding `[min, max]`.
:::

:::note
The pattern syntax allowed by the `OPERATOR_MATCH` operator accepts the following wildcards:
*  `*` matches a multiple (zero or more) characters, including space.
*  `?` matches a single (exactly one) characters, including space.
*  `[]` matches a character set. Examples: [aeiou] to match any vocals, or [a-z] to match a range
*  `#` matches any single digit (0 — 9). Shortcut for [0-9]
:::

## Syntax

Queries are submitted as a `Filter` message, made of an optional `SequenceFilter`, `TopicFilter` and `OntologyFilter`. Each field is mapped to a `Condition`, i.e. an operator and a value, and all the conditions must be satisfied. The ontology predicates are checked **one by one** against each topic: a topic matches when every predicate is satisfied.

```protobuf
sequence {
  name { op: OPERATOR_MATCH value { text: "test_run_*" } }
  user_metadata {
    key: "driver"
    value { op: OPERATOR_EQ value { text: "Alice" } }
  }
}
topic {
  ontology_tag { op: OPERATOR_EQ value { text: "IMU" } }
}
ontology {
  predicates { field: "IMU.acceleration.x" condition { op: OPERATOR_GT value { float: 5.0 } } }
  predicates { field: "IMU.acceleration.y" condition { op: OPERATOR_BETWEEN value { float_array { values: [-2.0, 2.0] } } } }
}
```

This query searches for:

- Sequences with names matching `test_run_*` pattern
- Where the user metadata field `driver` equals `"Alice"`
- Containing topics with ontology tag `IMU`
- Where the IMU's x-axis acceleration exceeds 5.0
- And the y-axis acceleration is between -2.0 and 2.0

### User metadata keys

To prevent users from inserting weird user metadata keys, only the following symbols and chars are accepted at creation:
*  `0-9, a-z, A-Z` _alphanumeric_
* `-` _minus. No repetitions allowed (e.g. `robot--status`)_
* `_` _underscore_
* ` ` _white space_

Inside queries also the following ones are admitted:
* `*`, this enables glob pattern search on keys
* `**`, this enables recursive glob pattern search on keys

#### Examples

```json title="query_user_metadata_glob_pattern_example"
{
  "robot_id": "bot-v4-092",
  "tags": ["warehouse-alpha", "heavy-duty", "fleet-3"],
  "status": {
    "telemetry": {
      "battery": 84,
      "temperature": 38.5
    },
    "hardware": {
      "motor_left": "nominal",
      "motor_right": "nominal"
    }    
  },
  "logs": [
    {
      "id": 1,
      "message": "boot success"
    },
    {
      "id": 2,
      "message": "lidar calibrated"
    }
  ],
  "payload_manifest": {
    "item_id": "sku-99201",
    "destination": "bin-c4",
    "sensors": {
      "weight_sensor": "active",
      "laser_scanner": "active",
      "diagnostics": {
        "camera_feed": "online"
      }
    }
  }
}
```

| Glob Pattern                  | Rule Type                | Description                                                                           | Matches Found                                                                                                                                                                  |
|:------------------------------|:-------------------------|:--------------------------------------------------------------------------------------|:-------------------------------------------------------------------------------------------------------------------------------------------------------------------------------|
| `*`                           | Single Level (`*`)       | Matches top-level keys only.                                                          | `robot_id`, `status`, `payload_manifest`                                                                                                                                       |
| `status.*`                    | Single Level (`*`)       | Matches every single key directly inside the `status` object.                         | `status.telemetry`, `status.hardware`                                                                                                                                          |
| `payload_manifest.sensors.**` | Deep Recursive (`**`)    | Traverses deeply to capture all keys and nested objects underneath `sensors`.         | `payload_manifest.sensors.weight_sensor`, `payload_manifest.sensors.laser_scanner`, `payload_manifest.sensors.diagnostics`, `payload_manifest.sensors.diagnostics.camera_feed` |
| `payload_manifest.*.*`        | Positional Single (`*`)  | Matches any key that is exactly **two levels down** from the `payload_manifest` root. | `payload_manifest.sensors.weight_sensor`, `payload_manifest.sensors.laser_scanner`, `payload_manifest.sensors.diagnostics`                                                     |
| `tags[*]`                     | Array flattining (`[*]`) | Matches all elements inside the `tags` array.                                         | `"warehouse-alpha"`, `"heavy-duty"`, `"fleet-3"`                                                                                                                               |
| `logs[*].id`                  | Array flattining (`[*]`) | Matches all objects inside the `logs` array containing an `id` key.                   | `logs[0].id`, `logs[1].id`                                                                                                                                                     |


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
