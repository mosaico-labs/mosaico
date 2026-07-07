---
title: Filtering
sidebar_position: 8
description: "How mosaicod's topic_filter_clusterize and topic_filter_intersect actions turn ontology filters into temporal windows. Covers the time-gap clustering rule, the cross-topic overlap rule, the request schemas, and the streamed JSONL responses."
---

The [`query`](query.md) action answers *which* topics contain data matching a filter. The **filtering** actions answer the complementary, temporal question, *when* does matching data occur, and return time windows that can be fed straight into the [retrieval protocol](retrieval.md#the-retrieval-protocol):

- [`topic_filter_clusterize`](#clustering) groups a **single** topic's matching timestamps into temporal clusters.
- [`topic_filter_intersect`](#intersection) clusters **several** topics of the same sequence and streams the windows where all of them match at once.

Both actions require the `read` [permission](api_key.md).

## Clustering

The `topic_filter_clusterize` action filters one topic by its ontology filter values and an optional time window, groups the matching timestamps into **clusters**, and streams them back.

### How clustering works

A cluster is a contiguous time window built by walking the matching timestamps in order: two consecutive matches belong to the same cluster as long as the gap between them does not exceed `clustering_dt_ns`. A gap larger than the threshold closes the current cluster and opens a new one. Each cluster is reported by its `[start_ns, end_ns]` bounds.

:::note
Setting `clustering_dt_ns` to `0` collapses the result into a single `[min, max]` cluster spanning all matching timestamps.
:::

### Request

```json title="topic_filter_clusterize_request"
{
  "locator": "test_run_01/sensors/imu",
  "clustering_dt_ns": 1000000000,
  "ontology": {
    "imu.acceleration.x": { "$gt": 5.0 },
    "imu.acceleration.z": { "$between": [-1.0, 1.0] }
  },
  "timestamp_range": { "start_ns": 1000000000, "end_ns": 5000000000 }
}
```

| Field | Description |
| --- | --- |
| `locator` | The topic to filter, as `<sequence>/<topic_path>`. |
| `clustering_dt_ns` | Maximum time gap, in nanoseconds, between two consecutive matching events for them to belong to the same cluster. A value of `0` yields a single `[min, max]` cluster. |
| `ontology` | Ontology filter, expressed with the same fields and [operators](query.md#supported-operators) as the `query` action. Multiple fields are combined with implicit AND, and must reference the topic's own ontology tag. |
| `timestamp_range` | Optional `{ start_ns, end_ns }` window; events outside it are ignored. |

### Response

The result is streamed as JSONL, **one cluster per line** in timestamp order. Each cluster carries a progressive `id` and its `[start_ns, end_ns]` bounds:

```json title="topic_filter_clusterize_response"
{ "ts": { "start_ns": 1000000000, "end_ns": 1200000000 }, "id": 0 }
{ "ts": { "start_ns": 3000000000, "end_ns": 3400000000 }, "id": 1 }
```

The stream is empty when the timestamp range does not overlap the topic data, or when no event matches the ontology filter.

Because clusters are streamed without buffering, the response starts flowing as soon as the first cluster is closed, keeping memory usage flat regardless of how much data the topic holds.

## Intersection

The `topic_filter_intersect` action lifts clustering to **multiple** topics of the same sequence: *when do all of them hold matching data at the same time?* It clusters each topic independently, then streams the time windows where the per-topic clusters overlap.

### How intersection works

Each entry in the request is clustered on its own, exactly as in [clustering](#clustering): the topic is filtered by its ontology values and optional time window, and the matching timestamps are grouped into clusters using that topic's `clustering_dt_ns` gap threshold.

The per-topic cluster streams are then merged with a k-way walk that always advances the stream whose current cluster ends first. Whenever the clusters currently active on **every** topic overlap, <u>allowing a tolerance of `intersect_dt_ns` between them</u>, an intersection window is emitted. Each intersection carries a progressive `id` and its `[start_ns, end_ns]` bounds.

:::note
At least **two** topics are required, and they must all belong to the **same sequence**. Every entry must carry a non-empty `ontology` filter.
:::

### Request

```json title="topic_filter_intersect_request"
{
  "topics": [
    {
      "locator": "test_run_01/sensors/imu",
      "clustering_dt_ns": 1000000000,
      "ontology": {
        "imu.acceleration.x": { "$gt": 5.0 },
        "imu.acceleration.y": { "$lt": 10.0}
      },
      "timestamp_range": { "start_ns": 1000000000, "end_ns": 5000000000 }
    },
    {
      "locator": "test_run_01/sensors/gps",
      "clustering_dt_ns": 1000000000,
      "ontology": {
        "gps.speed": { "$gt": 10.0 }
      }
    }
  ],
  "intersect_dt_ns": 200000000
}
```

| Field | Description |
| --- | --- |
| `topics` | Array of per-topic clustering configs (at least two, all in the same sequence). Each entry has the same shape as a [clustering](#request) request. |
| `topics[].locator` | The topic to cluster, as `<sequence>/<topic_path>`. |
| `topics[].clustering_dt_ns` | Maximum time gap, in nanoseconds, between two consecutive matching events for them to belong to the same cluster on that topic. A value of `0` yields a single cluster spanning the whole range. |
| `topics[].ontology` | Ontology filter for that topic, expressed with the same fields and [operators](query.md#supported-operators) as the `query` action. Must be non-empty and reference the topic's own ontology tag. |
| `topics[].timestamp_range` | Optional `{ start_ns, end_ns }` window; events outside it are ignored. |
| `intersect_dt_ns` | Tolerance, in nanoseconds, within which per-topic clusters are still considered overlapping. `0` requires strict overlap. |

### Response

The result is streamed as JSONL, **one intersection window per line** in timestamp order. Each window carries a progressive `id` and its `[start_ns, end_ns]` bounds:

```json title="topic_filter_intersect_response"
{ "ts": { "start_ns": 1200000000, "end_ns": 1400000000 }, "id": 0 }
{ "ts": { "start_ns": 3100000000, "end_ns": 3300000000 }, "id": 1 }
```

The stream is empty when no time window satisfies the ontology filters on every topic at once.

Because windows are streamed as soon as an overlap is confirmed, the response starts flowing without buffering the full result, keeping memory usage flat.
