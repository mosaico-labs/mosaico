---
title: Query Engine
description: Deep semantic search capabilities.
sidebar:
    order: 5
---

Mosaico distinguishes itself from simple file stores with a powerful **Query Engine** capable of filtering data based on both high-level metadata and deep content values. The query engine operates through the `query` action in the Control Channel, accepting structured JSON-based filter expressions that can span the entire data hierarchy.

## Query Architecture

The query engine is designed around a three-tier filtering model that allows you to construct complex, multi-dimensional searches:

**Sequence Filtering.** Target recordings by structural attributes like sequence name, creation timestamp, or user-defined metadata tags. This level allows you to narrow down which recording sessions are relevant to your search.

**Topic Filtering.** Refine your search to specific data streams within sequences. You can filter by topic name, ontology tag (the data type), serialization format, or topic-level user metadata.

**Ontology Filtering (Deep Content Search).** Query the actual physical values recorded inside the sensor data without scanning terabytes of files. The engine leverages statistical indices computed during ingestion—min/max bounds stored in the metadata cache for each chunk—to rapidly include or exclude entire segments of data.

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

### Ontology Filter (Deep Content)

The ontology filter queries the actual sensor data values. Fields are specified using dot notation: `<ontology_tag>.<field_path>`.

For example, to query IMU acceleration data: `imu.acceleration.x`, where `imu` is the ontology tag and `acceleration.x` is the field path within that data model.

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

```json
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
    "imu.acceleration.y": { "$between": [-2.0, 2.0] }
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

The query response is hierarchically grouped by sequence. For each matching sequence, it provides the list of topics that satisfied the filter criteria, along with optional timestamp ranges indicating when the ontology conditions were met.

```json
{
  "items": [
    {
      "sequence": "test_run_01",
      "topics": [
        { 
          "locator": "test_run_01/sensors/imu",
          "timestamp_range": [1600000000, 1600005000]
        }
      ]
    }
  ]
}
```

The `timestamp_range` field is included only when ontology filters are applied and indicates the precise time windows where the deep content conditions were satisfied. This allows you to retrieve only the relevant data slices using the Retrieval Channel.

## Performance Characteristics

The query engine is optimized for speed through several mechanisms:

**Index-Based Pruning.** Ontology queries leverage skip indices—precomputed min/max statistics for each chunk. The engine can exclude entire chunks without reading the underlying data files, dramatically reducing I/O.

**Metadata Cache Queries.** Sequence and topic filters execute entirely within the L1 metadata cache (PostgreSQL), providing sub-second response times even across thousands of sequences.

**Lazy Evaluation.** The engine returns locators and timestamp ranges, not the actual data. This keeps query responses lightweight. Clients then use the Retrieval Channel to fetch only the relevant data slices.