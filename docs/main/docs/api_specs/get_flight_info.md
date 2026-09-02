---
title: GetFlightInfo
sidebar_position: 1
description: "Reference for the TopicAppMetadata JSON payload carried on a FlightEndpoint's app_metadata field in get_flight_info responses. Documents every field, when time_window_info is present versus null, and examples for each topic state."
---

This page documents the JSON shape of `TopicAppMetadata`, the payload mosaicod attaches to a
[`FlightEndpoint`](https://arrow.apache.org/docs/format/Flight.html)'s `app_metadata` field when
responding to a `get_flight_info` call, as described in [Retrieval](daemon/retrieval.md).

It appears in two places:

- Calling `get_flight_info` directly on a **topic** locator: the single endpoint returned carries it.
- Calling `get_flight_info` on a **sequence** locator: every topic endpoint under that sequence
  carries its own copy. (The sequence itself carries a separate, unrelated `SequenceAppMetadata`
  payload on `FlightInfo.app_metadata`.)

The payload is plain JSON.

## Field reference

| Field | Type | Notes                                                                                                                      |
|---|---|----------------------------------------------------------------------------------------------------------------------------|
| `created_at_ns` | `int64` | Topic creation timestamp, nanoseconds since epoch.                                                                         |
| `completed_at_ns` | `int64 \| null` | Set once the topic's session is finalized. `null` while the session is still open.                                         |
| `locked` | `bool` | Shorthand for `completed_at_ns != null`. A locked topic's data is immutable and safe to read via `do_get`.                 |
| `resource_locator` | `string` | The topic's locator, e.g. `"my_sequence/my_topic"`.                                                                        |
| `ontology_tag` | `string` | Client-provided ontology tag for the topic (e.g. `"imu"`, `"Lidar"`).                                                      |
| `serialization_format` | `string` | One of `"default"`, `"ragged"`, `"image"` — see [Retrieval](retrieval.md#metadata-context-headers).                        |
| `user_metadata` | `object \| null` | Arbitrary client-provided JSON supplied at topic creation. `null` if none was given.                                       |
| `data_info` | [`TopicAppMetadataDataInfo`](#topicappmetadatadatainfo) | Stats for the **whole topic**, regardless of any requested time window. Always present.                                    |
| `time_window_info` | [`TopicAppMetadataTimeWindow`](#topicappmetadatatimewindow) `\| null` | Stats scoped to the timestamp range passed to `get_flight_info`. **`null` unless a time range was requested** — see below. |

### `TopicAppMetadataDataInfo`

| Field | Type | Notes |
|---|---|---|
| `interval` | [`Timestamp`](#timestamp-object) `\| null` | First/last timestamp across all of the topic's data. `null` if the topic has no data yet. |
| `total_row_count` | `uint64` | Total number of rows across the whole topic. |
| `total_bytes` | `uint64` | Total size in bytes of the topic's data. |
| `total_chunks_count` | `uint64` | Number of data chunks (files) written for the topic. An uploaded batch with zero rows does not produce a chunk, so this can be `0` even after a successful `do_put`. |

### `TopicAppMetadataTimeWindow`

| Field | Type | Notes |
|---|---|---|
| `interval` | [`Timestamp`](#timestamp-object) `\| null` | First/last timestamp *within the requested window*. `null` if no rows fall inside the window. |
| `row_count` | `uint64` | Number of rows within the requested window. `0` if none fall inside it. |

### `Timestamp` object

Both `interval` fields above use the same shape:

```json
{ "start_ns": 10000, "end_ns": 10030 }
```

## When is `time_window_info` present?

`time_window_info` reflects the optional timestamp range passed to `get_flight_info`, independently
of `data_info`:

- **No timestamp range passed**: `time_window_info` is `null`.
  Only whole-topic stats (`data_info`) are returned.
- **A timestamp range passed**, whether or not the topic has data, or any of it falls inside the
  window: `time_window_info` is always an object. Its `row_count` is `0` and `interval` is `null`
  only when nothing matches the window (including a topic with no data at all).

A malformed range (`start >= end`) is rejected outright with a gRPC `InvalidArgument` status — no
`TopicAppMetadata` is produced in that case.

## Examples

### Unlocked topic, no data, no time window requested

Right after `topic_create`, before any `do_put` or session finalize:

```json
{
  "created_at_ns": 1735689600000000000,
  "completed_at_ns": null,
  "locked": false,
  "resource_locator": "test_sequence/my_topic",
  "ontology_tag": "imu",
  "serialization_format": "default",
  "user_metadata": null,
  "data_info": {
    "interval": null,
    "total_row_count": 0,
    "total_bytes": 0,
    "total_chunks_count": 0
  },
  "time_window_info": null
}
```

### Unlocked topic, no data, time window requested

Same topic as above, but `get_flight_info` is called with `Some(timestamp_range)`. `time_window_info`
is now present, but empty — there is nothing to report inside the window because there is no data
at all:

```json
{
  "created_at_ns": 1735689600000000000,
  "completed_at_ns": null,
  "locked": false,
  "resource_locator": "test_sequence/my_topic",
  "ontology_tag": "imu",
  "serialization_format": "default",
  "user_metadata": null,
  "data_info": {
    "interval": null,
    "total_row_count": 0,
    "total_bytes": 0,
    "total_chunks_count": 0
  },
  "time_window_info": {
    "interval": null,
    "row_count": 0
  }
}
```

### Locked topic, no data

A topic whose session was finalized without ever writing a non-empty batch (or where only
zero-row batches were written — those don't produce a chunk):

```json
{
  "created_at_ns": 1735689600000000000,
  "completed_at_ns": 1735689601000000000,
  "locked": true,
  "resource_locator": "test_sequence/my_empty_topic",
  "ontology_tag": "imu",
  "serialization_format": "default",
  "user_metadata": null,
  "data_info": {
    "interval": null,
    "total_row_count": 0,
    "total_bytes": 0,
    "total_chunks_count": 0
  },
  "time_window_info": null
}
```

### Locked topic with data, no time window requested

```json
{
  "created_at_ns": 1735689600000000000,
  "completed_at_ns": 1735689601000000000,
  "locked": true,
  "resource_locator": "test_sequence/my_topic",
  "ontology_tag": "imu",
  "serialization_format": "default",
  "user_metadata": { "custom_key": "custom_value" },
  "data_info": {
    "interval": { "start_ns": 10000, "end_ns": 10030 },
    "total_row_count": 7,
    "total_bytes": 895,
    "total_chunks_count": 1
  },
  "time_window_info": null
}
```

### Locked topic with data, time window covering a subset of rows

Same topic as above, `get_flight_info` called with a range covering only one of the 7 rows.
Note `data_info` is unchanged — it always reports the whole topic — while `time_window_info`
reports only what falls inside `[10014, 10018)`:

```json
{
  "created_at_ns": 1735689600000000000,
  "completed_at_ns": 1735689601000000000,
  "locked": true,
  "resource_locator": "test_sequence/my_topic",
  "ontology_tag": "imu",
  "serialization_format": "default",
  "user_metadata": { "custom_key": "custom_value" },
  "data_info": {
    "interval": { "start_ns": 10000, "end_ns": 10030 },
    "total_row_count": 7,
    "total_bytes": 895,
    "total_chunks_count": 1
  },
  "time_window_info": {
    "interval": { "start_ns": 10015, "end_ns": 10015 },
    "row_count": 1
  }
}
```

### Locked topic with data, time window matching no rows

Same topic, but the requested range falls entirely outside the topic's data
(e.g. `[10100, 10100)`). `time_window_info` is still present (a range was requested), but empty:

```json
{
  "created_at_ns": 1735689600000000000,
  "completed_at_ns": 1735689601000000000,
  "locked": true,
  "resource_locator": "test_sequence/my_topic",
  "ontology_tag": "imu",
  "serialization_format": "default",
  "user_metadata": { "custom_key": "custom_value" },
  "data_info": {
    "interval": { "start_ns": 10000, "end_ns": 10030 },
    "total_row_count": 7,
    "total_bytes": 895,
    "total_chunks_count": 1
  },
  "time_window_info": {
    "interval": null,
    "row_count": 0
  }
}
```
