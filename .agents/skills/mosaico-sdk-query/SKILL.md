---
name: mosaico-sdk-query
description: Write and run Python code that searches Mosaico data with the mosaicolabs SDK — find sequences by name or metadata, topics by name or sensor type, time windows where sensor values meet a condition, and correlate two sensors. Use when someone asks "find the recordings where…", "which sequences have…", "when did the IMU exceed…", "write a Mosaico query", "how do I query X with the SDK", or wants to load the data a query matched. Not for aligning sensors into DataFrames or training sets (use mosaico-ml-dataset) or for connection problems (use mosaico-setup).
---

# Writing Mosaico SDK queries

Turn a question about recorded data into a short Python script that uses `mosaicolabs`, run it against
the user's daemon, and show the result. The documentation at https://docs.mosaico.dev is the reference;
this file tells you how to use it, it does not replace it. Inside a checkout of mosaico-labs/mosaico the
same pages are under `docs/main/docs` and `docs/py/docs`.

- **Read-only.** Queries and streams only read. Never call `sequence_create`, `sequence_update`,
  `sequence_delete` or `session_delete`, and never run `mosaicolabs.ros_injector`,
  `mosaicolabs.mcap_injector` or `mosaicolabs.watchdog`: all of them write to the daemon. Explain them,
  never run them.
- **Versions.** Written for Mosaico main (`0.8.0-dev` after #817). The query API used here (builders,
  `.Q`, `clusterize()`, `intersect()`, handlers, streamer bounds) is the same in SDK 0.6.3 and 0.7. What
  differs: SDK and daemon must be on the same side of #817 (protobuf after, JSON before, so a 0.6/0.7
  daemon needs SDK 0.6.3/0.7), and SDK 0.6.x reads the daemon version with `client.version()`, 0.7 on
  with `client.info().version`. A mismatch fails at connect, not in the query: use mosaico-setup.

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

Pages to read before writing:

- https://docs.mosaico.dev/learn/query_sequences, https://docs.mosaico.dev/learn/query_topics: names,
  metadata, wildcards.
- https://docs.mosaico.dev/learn/multi_domain_query: combining layers, the `.Q` proxy.
- https://docs.mosaico.dev/learn/chained_queries: correlating two topics.
- https://docs.mosaico.dev/learn/temporal_windows: `clusterize()`, `intersect()`.
- https://docs.mosaico.dev/learn/queryable_fields_queries: data with no Python class (`QueryableNumeric`).
- https://docs.mosaico.dev/python-sdk/SDK/query/: full reference, operators, constraints.
- https://docs.mosaico.dev/python-sdk/SDK/handling/reading/: loading the matched data.

## 3. Rules that decide whether the query returns anything

- **Everything in one `client.query()` call is AND, evaluated per topic.** A `QueryTopic` name and a
  `QueryOntologyCatalog` expression of a different sensor in the same call ask for one topic that is both:
  that returns nothing. Two topics means a chained query.
- **One field path once per builder.** `acceleration.x.gt(a)` plus `acceleration.x.lt(b)` is an error:
  use `.between([a, b])`. Different fields of the same sensor are fine.
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
- **NaN values can hide a whole topic from value conditions.** Observed on mosaicod 0.8.0-dev with a
  bridged ROS odometry topic, not documented: when samples of a topic hold NaN in the field you filter
  on, the daemon left that topic out of every `QueryOntologyCatalog` condition on it, even an always-true
  one, with no error, while conditions on `timestamp_ns` still found it. Section 5 says how to detect it.
- **`client.query()` returns `None` when the server sends no response**; any other error raises: a connection
  error inside the call, while an invalid field path (`AttributeError`) or a field used twice
  (`NotImplementedError`) already fails while the query is being built, before the call. Otherwise it returns a `QueryResponse`, a list of
  items: an empty one means the query ran and matched nothing. Check for `None` before iterating. (Some
  learn pages say it "returns `None` on error"; the client code re-raises, so catch exceptions too; read it in the installed package with
  `python -c "import inspect, mosaicolabs as m; print(inspect.getsource(m.MosaicoClient.query))"`.)

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
            for cluster in topic.clusterize(clustering_dt_ns=int(1e8)):  # 100 ms
                print("  ", topic.name, cluster.timerange.start, cluster.timerange.end)
```

Use only what you saw in the documentation or the discovery output. If a method is not in the docs, check
it exists before using it: `python -c "import mosaicolabs as m; help(m.QueryTopic)"`.

To load the matched data, open a handler and stream only the window the query returned
(https://docs.mosaico.dev/python-sdk/SDK/handling/reading/). A cluster's `end` is inclusive while the streamer's
`end_timestamp_ns` is exclusive: pass `end + 1`, or the last matching sample of every window is lost.
With no window to use, as in the NaN case of section 5, omit both timestamps to stream the whole topic.
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
2. For a value condition, compare against an always-true condition on the same field
   (`.gt(-1e30)`) and against `<Class>.Q.timestamp_ns.gt(0)`. A topic that answers the timestamp
   condition but not the always-true one has NaN in that field: stream it
   (the reading page of section 4), skip the NaN samples, apply the condition in Python, and say
   that the server-side query could not answer for that topic.

When the question cannot be expressed with the SDK (an OR between conditions on different fields,
aggregations such as an average), say so, query the closest superset and finish the computation in
Python on the streamed data.

When a question can be read two ways (a declared metadata value versus what the recorded data shows,
e.g. `duration_s` in the metadata versus the span of the timestamps), answer with the one the wording
points to, say which one you used, and give the other in one line if it differs.
