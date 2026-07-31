---
title: Filtering
sidebar_position: 8
description: "How mosaicod's topic_filter_clusterize and topic_filter_intersect actions turn ontology filters into temporal windows. Covers the time-gap clustering rule, the cross-topic overlap rule, the request schemas, and the streamed JSONL responses."
---

The [`query`](query.md) action answers *which* sequences and topics contain data matching a filter. It does not tell you *when*, inside a matching topic, the matching data actually occurs. The **filtering** actions answer that complementary, temporal question, and return time windows instead of topic identities:

- [`topic_filter_clusterize`](#clustering) groups a **single** topic's matching timestamps into temporal clusters.
- [`topic_filter_intersect`](#intersection) clusters **several** topics of the same sequence and streams the windows where all of them match at once.

Both actions require the `read` [permission](api_key.md).

## Why filtering

Once `query` has narrowed the search down to a topic of interest, the natural follow-up question is: *at which points in time does the topic actually satisfy the filter?* A topic can span an entire recording, but the events matching an ontology filter, say, an IMU spike or a GPS speed threshold, are usually confined to a handful of short intervals within it. `topic_filter_clusterize` answers this for a single topic: it returns the exact `[start_ns, end_ns]` windows in which matching events occur.

Because sensor data is sampled, not continuous, "the interval in which an event occurs" is not a single well-defined thing. A given event might produce many matching samples in a row, sometimes with small gaps caused by sampling frequency, jitter, or noise. Grouping every isolated match into its own window would fragment a single physical event into dozens of tiny clusters. This is why `clusterize` takes a `clustering_dt_ns` parameter: it lets the caller decide how large a gap between two consecutive matches is still considered "the same event," and therefore how the underlying samples should be aggregated into clusters. Different topics and different sampling rates call for different thresholds, so this is left to the user rather than hard-coded.

A related but distinct question is: *when do several different events, on different topics, happen at the same time?* For example, when is the vehicle both accelerating hard **and** exceeding a given speed? `topic_filter_intersect` answers this by clustering each topic independently and then streaming the windows where the per-topic clusters overlap. Because topics are generally asynchronous, sampled at different rates and not aligned on the same timestamps, clusters that represent "the same moment" across topics rarely share exact boundaries. `intersect_dt_ns` exists for the same reason as `clustering_dt_ns`: it lets the caller define how close two clusters need to be to be considered co-occurring, instead of requiring an exact, unrealistic timestamp match.

Finally, `clusterize` and `intersect` are exposed as two separate actions rather than folded into `query` itself. Most queries only need to know *which* topics match, not *when* within them, so computing temporal windows on every query would be wasted work. Clustering can also be a comparatively expensive, data-scanning operation, so it is kept as an explicit, opt-in step that callers only pay for when they actually need temporal windows.

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
    "IMU.acceleration.x": { "$gt": 5.0 },
    "IMU.acceleration.z": { "$between": [-1.0, 1.0] }
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
        "IMU.acceleration.x": { "$gt": 5.0 },
        "IMU.acceleration.y": { "$lt": 10.0}
      },
      "timestamp_range": { "start_ns": 1000000000, "end_ns": 5000000000 }
    },
    {
      "locator": "test_run_01/sensors/gps",
      "clustering_dt_ns": 1000000000,
      "ontology": {
        "GPS.speed": { "$gt": 10.0 }
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
