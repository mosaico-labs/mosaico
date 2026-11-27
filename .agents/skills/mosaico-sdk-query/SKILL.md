---
name: mosaico-sdk-query
description: Write and run Python code that searches Mosaico data with the mosaicolabs SDK — find sequences by name or metadata, topics by name or sensor type, time windows where sensor values meet a condition, topics whose maximum, mean, sample count or sampling rate meets a condition, and correlate two sensors. Use when someone asks "find the recordings where…", "which sequences have…", "when did the IMU exceed…", "which IMUs never went above…", "which topics are sampled at 200 Hz", "write a Mosaico query", "how do I query X with the SDK", or wants to load the data a query matched. Not for aligning sensors into DataFrames or training sets (use mosaico-ml-dataset) or for connection problems (use mosaico-setup).
---

# Writing Mosaico SDK queries

Turn a question about recorded data into a short Python script that uses `mosaicolabs`, run it against
the user's daemon, and show the result. The documentation at https://docs.mosaico.dev is the reference;
this file tells you how to use it, it does not replace it. If a page cited here has moved, find it by
title in https://docs.mosaico.dev/llms.txt and cite the page you read.

- **Read-only.** Queries and streams only read. Never call `sequence_create`, `sequence_update`,
  `sequence_delete` or `session_delete`, and never run `mosaicolabs.ros_injector`,
  `mosaicolabs.mcap_injector` or `mosaicolabs.watchdog`: all of them write to the daemon. Explain them,
  never run them.
- **Versions.** Covers daemons 0.8+ and main. The SDK must have the daemon's 0.X
  (`pip install "mosaicolabs[cli]==0.X.*"`; for a daemon built from main, the SDK from the same commit).
  `client.info().version` gives the daemon's version: check it first. An SDK 0.7 or older fails to
  connect to a 0.8+ daemon (use mosaico-setup); between 0.8+ versions a mismatch can connect, so a
  connection does not prove the pair matches. For 0.6 and 0.7 daemons see section 6.

## 1. Discover before you filter

Never guess a sequence name, a topic name or an ontology tag. A wrong name does not raise, it silently
matches nothing. Run this first, against the same host and port the user works with:

```python
from mosaicolabs import MosaicoClient

with MosaicoClient.connect("localhost", 6726) as client:
    for seq in client.list_sequences():
        sh = client.sequence_handler(seq)
        if sh is None:  # handlers are Optional
            continue
        print(seq, sh.user_metadata)
        for t in sh.topics:
            th = client.topic_handler(seq, t)
            if th is None:
                continue
            # Topic metadata can be large (bridged ROS topics carry the whole message
            # definition under "_ros_"): print the keys, open one only when you need it.
            print("  ", t, th.ontology_tag, list(th.user_metadata), th.timestamp_ns_min, th.timestamp_ns_max)
            th.close()
        sh.close()
```

The ontology tag printed for each topic is how you map the user's words to a type: "odometry" is whatever
tag the odometry topics carry (for example `MotionState`), not a guess. To see the field paths of a class
before writing `.Q`, print its fields, then the fields of the nested types:

```python
import mosaicolabs as m
print(list(m.MotionState.model_fields))   # header, covariance, ..., pose, velocity, acceleration
print(list(m.Velocity.model_fields))      # header, covariance, ..., linear, angular
```

Nested types are exported by `mosaicolabs` too, so the path `MotionState.Q.velocity.linear.x` is built
one level at a time this way.

Host, port, TLS and API key: ask the user, or use `MosaicoClient.from_env(host, port)` if they configured the
environment (https://docs.mosaico.dev/python-sdk/SDK/client/). If the connection fails, stop and say so; do not write a query
you cannot run.

## 2. Pick the layer the question is about

| The question is about…                                | Builder                | Typical calls                                                                  |
| ----------------------------------------------------- | ---------------------- | ------------------------------------------------------------------------------ |
| the recording session (name, metadata, creation time) | `QuerySequence`        | `.with_name_match()`, `.with_user_metadata()`, `.with_created_timestamp()`     |
| a channel (topic name, sensor type, topic metadata)   | `QueryTopic`           | `.with_name()`, `.with_name_match()`, `.with_ontology_tag(IMU.ontology_tag())` |
| the recorded values (a field above/below/between)     | `QueryOntologyCatalog` | `.with_expression(IMU.Q.acceleration.z.gt(15.0))`                              |
| two different topics in the same session              | chained query          | `client.query(...)` → `.to_query_sequence()` → second `client.query(...)`      |
| _when_ a condition held, or two held at once          | temporal windows       | `topic.clusterize()`, `item.intersect()`                                       |
| a value over the whole recording (max, mean, rate…)   | aggregators            | `QueryOntologyCatalog(IMU.Q.acceleration.x.max().lt(15.0))`                    |

Pages to read before writing:

- https://docs.mosaico.dev/learn/query_sequences, https://docs.mosaico.dev/learn/query_topics: names,
  metadata, wildcards.
- https://docs.mosaico.dev/learn/multi_domain_query: combining layers, the `.Q` proxy.
- https://docs.mosaico.dev/learn/chained_queries: correlating two topics.
- https://docs.mosaico.dev/learn/temporal_windows: `clusterize()`, `intersect()`.
- https://docs.mosaico.dev/learn/queryable_fields_queries: data with no Python class (`QueryableNumeric`).
- https://docs.mosaico.dev/learn/aggregators: values over the whole recording.
- https://docs.mosaico.dev/python-sdk/SDK/query/: full reference, operators, constraints.
- https://docs.mosaico.dev/python-sdk/SDK/handling/reading/: loading the matched data.

### Aggregators

A plain condition matches a topic when at least one sample satisfies it. An aggregator combines all the
samples of the field in a topic into one value and checks the condition once:
`<Class>.Q.<field>.<aggregator>().<operator>(value)`. `max().lt(x)` reads "every sample below `x`",
`min().gt(x)` "every sample above `x`".

```python
QueryOntologyCatalog(
    IMU.Q.acceleration.x.rate().between(190, 210),   # sampled at about 200 Hz
    IMU.Q.acceleration.z.max().lt(15.0),             # never above 15 m/s²
)
```

- Numeric fields: `.max()` `.min()` `.range()` `.count()` `.count_valid()` `.mean()` `.var()` `.std()`
  `.first()` `.last()` `.rate()`. Strings: `.max()` `.min()` `.first()` `.last()` (lexicographic),
  `.count()` `.count_valid()` `.rate()`. Booleans: `.max()` (some sample `True`), `.min()` (every
  sample `True`). Lists, as a whole: `.max()` `.min()` `.range()` over the elements, `.count()`
  `.rate()` over the samples. Counts and rates always compare against a number, not another aggregate.
- `.count()` includes null and NaN samples, `.count_valid()` excludes both (it cannot tell null from
  NaN). For topics with some invalid samples, read N with `.count()` first, then `.count_valid().lt(N)`;
  `.count_valid().eq(0)` finds topics with no valid sample. `.rate()` is samples per second between
  the first and the last timestamp, invalid samples included.
- One aggregator per expression: chaining (`.max().min()`) or aggregating after `.any()`, `.all()` or
  `[i]` raises `TypeError`. A field reached through a list without `.any()`, `.all()` or `[i]` can only
  be aggregated: `RobotPath.Q.poses.position.z.gt(0.1)` raises `AttributeError`.
- A topic whose aggregate is undefined (mean of an always-null field, `std` of fewer than two valid
  samples, rate of a single sample) never matches, whatever the condition.
- Aggregators select topics, not time windows: `clusterize()` or `intersect()` on the response of a query
  with an aggregated expression makes the server return an error. They filter, they do not return the
  value itself.

## 3. Rules that decide whether the query returns anything

- **Everything in one `client.query()` call is AND, evaluated per topic.** A `QueryTopic` name and a
  `QueryOntologyCatalog` expression of a different sensor in the same call ask for one topic that is both:
  that returns nothing. Two topics means a chained query.
- **A range on one field is `.between([a, b])`.** `QuerySequence` and `QueryTopic` accept each key
  once (a second `with_name()` raises `NotImplementedError`); `QueryOntologyCatalog` accepts several
  expressions on the same field, plain and aggregated.
- **Name matching is a glob, not a substring.** `with_name_match("race")` is an exact match;
  `"race*"` is a prefix, `"*race*"` contains. `?` is one character, `#` is one digit and `[` `]` delimit a character set (see
  the wildcard table of the query_sequences page): when the text you look for contains brackets, check the
  pattern on a value you know matches before trusting an empty result.
- **Topic names are relative paths** such as `/imu`, exactly as the discovery step printed them.
- **Timestamps are integers in nanoseconds** (`1_700_000_000_000_000_000`).
- **`.Q` needs the ontology class** (`IMU`, `GPS`, `Image`, `Pose`…, importable from `mosaicolabs`). Some
  tags the discovery step shows have no class (data bridged from ROS or MCAP, e.g. `EventArray`). To
  filter topics by such a tag, pass the string: `QueryTopic().with_ontology_tag("EventArray")`. To filter
  its values, use `QueryableNumeric("<tag>.<field>")` / `QueryableString(...)` from
  `mosaicolabs.query.queryable_fields`. A typo in a tag or path matches nothing without an error.
- **Metadata operators are keyword arguments**: `.with_user_metadata("vmax_ms", gt=20)`,
  `.with_user_metadata("section", match="*Track*")`, `eq=`, `lt=` and the others listed in
  the query_sequences page. Nested keys use dot notation (`"project.name"`).
- **A few NaN samples do not hide a topic.** Which topics match is decided on the valid values; a topic
  whose field is NaN or null in every sample matches no value condition (section 5 says how to tell).
  Inside a matched topic the windows still compare NaN as a number: +NaN above every number, -NaN
  below. So any comparison (`gt`, `lt`, `between`, `outside`, `neq`…) can return windows made only of
  NaN, and a real window can stretch over adjacent NaN samples. Stream each window (`end + 1`), skip
  NaN, drop windows with no valid matching sample, and trim the bounds to the valid samples.
- **`client.query()` returns `None` when the server sends no response**; any other error raises: a connection
  error inside the call, while an invalid field path (`AttributeError`), a misused aggregator
  (`TypeError`) or a repeated sequence/topic key (`NotImplementedError`) already fails while the query is
  being built, before the call. Otherwise it returns a `QueryResponse`, a list of items: an empty one
  means the query ran and matched nothing. Check for `None` before iterating, and catch exceptions.

## 4. Write the script

```python
from mosaicolabs import MosaicoClient, QuerySequence, QueryTopic, QueryOntologyCatalog, IMU

with MosaicoClient.connect("localhost", 6726) as client:
    response = client.query(
        QuerySequence().with_name_match("race*"),
        QueryOntologyCatalog().with_expression(IMU.Q.acceleration.z.gt(15.0)),
    )
    if response is None:
        raise SystemExit("query failed: check the connection and the field paths")
    for item in response:
        print(item.sequence.name)
        for topic in item.topics:
            # Only when the query has no aggregated expression (section 2).
            # clustering_dt_ns is the largest gap merged into one window: set it just above the
            # sampling period (about 1.5x, from the discovery timestamps or .rate()). Larger merges
            # separate events: 100 ms at 200 Hz merged spikes 50 ms apart into one 1.0-6.0 s window.
            for cluster in topic.clusterize(clustering_dt_ns=int(7.5e6)):  # 7.5 ms at 200 Hz
                print("  ", topic.name, cluster.timerange.start, cluster.timerange.end)
```

Use only what you saw in the documentation or the discovery output. If a method is not in the docs, check
it exists before using it: `python -c "import mosaicolabs as m; help(m.QueryTopic)"`.

To load the matched data, open a handler and stream only the window the query returned
(https://docs.mosaico.dev/python-sdk/SDK/handling/reading/). A cluster's `end` is inclusive while the streamer's
`end_timestamp_ns` is exclusive: pass `end + 1`, or the last matching sample of every window is lost.
With no window to use (an aggregated query), omit both timestamps to stream the whole topic.
Inside the loops of the script above:

```python
            th = client.topic_handler(item.sequence.name, topic.name)
            if th is not None:
                start, end = cluster.timerange.start, cluster.timerange.end
                for msg in th.get_data_streamer(start_timestamp_ns=start, end_timestamp_ns=end + 1):
                    sample = msg.get_data(IMU)
                th.close()
```

## 5. Run it, then answer

Run the script and show its output with the answer.

When the result is empty or misses a topic you expected, do not declare "no data". Check in this order
and tell the user what you found:

1. Remove one filter at a time to find the one that excludes everything.
2. For a value condition, query the same field with `.count().gt(0)` and with `.count_valid().gt(0)`.
   A topic that matches the first but not the second has no valid value in that field (all NaN or
   null): no value condition can match it, and that is the data, not the query.
3. If the daemon was upgraded from 0.7, its migration cleared the per-chunk statistics and queued every
   topic for the store optimizer: until it rewrites them, those topics match no `QueryOntologyCatalog`
   condition.

When the question cannot be expressed with the SDK (an OR between conditions on different fields, an
aggregate per time window, the value of an aggregate itself), say so, query the closest superset and
finish the computation in Python on the streamed data.

When a question can be read two ways (a declared metadata value versus what the recorded data shows,
e.g. `duration_s` in the metadata versus the span of the timestamps), answer with the one the wording
points to, say which one you used, and give the other in one line if it differs.

## 6. Older daemons (0.6, 0.7)

The builders, `.Q`, `clusterize()`, `intersect()`, handlers and streamer bounds above work the same.
What differs: SDK 0.6.x reads the daemon version with `client.version()`; there are no aggregators;
`QueryOntologyCatalog` also accepts each field once (`NotImplementedError`); and NaN samples can hide a
whole topic: a field with some NaN made the daemon leave the topic out of every value condition on it,
even an always-true one (`.gt(-1e30)`), while `<Class>.Q.timestamp_ns.gt(0)` still found it. When the
timestamp condition matches and the always-true one does not, stream the topic, skip the NaN samples,
apply the condition in Python, and say the server-side query could not answer for that topic.
