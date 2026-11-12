---
name: mosaico-ml-dataset
description: Write and run read-only Python code that turns Mosaico recordings into pandas DataFrames for analysis and machine learning with mosaicolabs.ml — extract topics or a time window to a table, align several sensors on a fixed-rate grid (SyncHold, SyncAsOf, SyncDrop), and feed the result to a scikit-learn pipeline. Not for finding which sequences, topics or time windows match a condition (use mosaico-sdk-query) or for connection problems (use mosaico-setup). Use when someone asks "give me the IMU and odometry of X aligned at 100 Hz", "export this topic to a DataFrame", "resample these sensors", "build a training set from a sequence", or asks how DataFrameExtractor or SyncTransformer work.
---

# Building ML datasets from Mosaico

Turn a dataset request into a short Python script that uses `mosaicolabs.ml`, run it against the user's
daemon, and report what came out (rows, columns, the grid and policy used). Everything here only reads:
never write, delete or re-ingest data to build a dataset. The guide is
https://docs.mosaico.dev/python-sdk/SDK/bridges/ml/ (inside a checkout of mosaico-labs/mosaico it is
`docs/py/docs/SDK/bridges/ml.md`); the installed `mosaicolabs.ml` code is the authority where the two
disagree (section 6).

Covers the 0.7 release (SDK 0.7.0 from PyPI with a `release/v0.7` daemon) and Mosaico main (SDK
0.8.0-dev, after the protobuf wire switch). `mosaicolabs.ml` is the same in both and in SDK 0.6.3, so
sections 2–6 hold on each; what differs is connecting. The SDK and the daemon
must share `X.Y` (patch numbers are independent) and, on `-dev` builds, the same side of #817 (`python -c "import importlib.metadata as m; print(m.version('mosaicolabs'))"`):
SDK 0.6.3 against a 0.7 or newer daemon fails in `connect()` with "Server did not become available
within 5s" (it probes the `version` action, replaced by `info` in 0.7), and a 0.7 SDK or daemon cannot
talk to a main one (JSON vs protobuf payloads). That is a mosaico-setup problem, not a dataset one.

## 1. Discover first, then extract only what you need

Find the sequence and topic names, the ontology tags and the field paths exactly as the
**mosaico-sdk-query** skill says in its section 1: the same discovery script, the same rules for
connection, TLS and API key. If the user wants only the windows where a condition held, run that query
with mosaico-sdk-query first and extract those windows here. A wrong sequence name gives
`client.sequence_handler()` → `None`; a wrong topic name makes the extractor raise `ValueError` listing
the available topics.

Always pass `topics=`: with no list the extractor reads every topic of the sequence, images and event
streams included, which can be gigabytes.

## 2. Extract to a DataFrame

```python
import pandas as pd
from mosaicolabs import MosaicoClient
from mosaicolabs.ml import DataFrameExtractor

HOST, PORT = "<host>", 6726  # ask the user; 6726 is only the default port
TOPICS = ["/front/imu", "/base/odom"]  # placeholders: list the real names first
COLS = ["timestamp_ns", "/front/imu.IMU.angular_velocity.x", "/base/odom.MotionState.pose.position.x"]

with MosaicoClient.connect(HOST, PORT) as client:  # the user's host and port
    sh = client.sequence_handler("run_42")
    if sh is None:
        raise SystemExit("unknown sequence: list them with client.list_sequences()")
    parts = []
    for chunk in DataFrameExtractor(sh).to_pandas_chunks(topics=TOPICS, window_sec=5.0):
        if chunk.empty:
            continue
        parts.append(chunk.reindex(columns=COLS))  # see "chunks" below for why reindex
    sh.close()
if not parts:
    raise SystemExit("no data for these topics in this window")
df = pd.concat(parts, ignore_index=True)
```

`to_pandas_chunks(topics, window_sec=5.0, timestamp_ns_start=None, timestamp_ns_end=None)` is a
generator of time windows. What the frames look like:

- **Column names** are `{topic}.{ontology_tag}.{field.path}`, e.g. `/front/imu.IMU.angular_velocity.x`;
  a topic with no tag gives `{topic}.{field.path}`. `timestamp_ns` is the only shared column. Print
  `df.columns` once instead of guessing: flattening also creates columns for every nested header
  (often all NaN), and covariance fields are one column holding a list per row.
- **Sparse, one row per sample.** Topics are stacked, not joined: each row carries one topic's values
  and NaN in the other topics' columns, so `len(df)` is the sum of the samples of all topics. Use
  `df[col].notna().sum()` for one topic's count, and a sync step (section 3) to get one row per instant.
- **Time window**: `timestamp_ns_start` is inclusive, `timestamp_ns_end` exclusive, as in the streamers
  (mosaico-sdk-query, section 4): for "from A to B included" pass `timestamp_ns_end=B + 1`. An end beyond
  the last sample extracts to the end of the sequence.
- Rows are sorted by `timestamp_ns`, but the index is not reset: use `ignore_index=True` or `.iloc`.

**Chunks** are where scripts break silently:

- Windows start at the first sample of the whole sequence, not of your topics: when your topics start
  later, the first chunks come out empty. Skip them.
- Once a topic has no more data, the following chunks have **no columns** for it. `chunk[COLS]` then
  raises `KeyError`, and syncing whole chunks makes that topic NaN instead of holding its last value.
  `chunk.reindex(columns=COLS)` keeps every chunk on the same columns.

## 3. Align on a fixed-rate grid

```python
from mosaicolabs.ml import SyncTransformer, SyncHold, SyncAsOf, SyncDrop

sync = SyncTransformer(target_fps=100, policy=SyncHold())   # once, outside the loop
dense = pd.concat([sync.transform(p) for p in parts], ignore_index=True)
dense = dense.astype({c: float for c in COLS[1:]})          # policies return object columns with None
```

The grid (`SyncTransformer.transform`): the first tick is the first `timestamp_ns` the transformer sees, tick
`k` is `start + floor(k * 1e9 / target_fps + 0.5)` (half-up, not Python's `round`), and each chunk adds ticks up to its last timestamp, so the
last tick is at or before the last sample. Calling `transform()` chunk by chunk on one instance gives
the same rows as one call on the whole frame: the instance carries the grid and each column's last
sample across chunks. That is why it must be created once per sequence; call `sync.reset()` before
reusing it on another sequence.

Read single values by column, `dense["timestamp_ns"].iloc[i]`: `dense.iloc[i]` makes one Series of the
whole row, turns the int64 timestamp into float64 and loses its last digits (a tick of
`...066461638212` printed as `...066461638144`).

Each column is resampled on its own. Its NaN rows are dropped first (they are other topics' rows, or
readings that really are NaN), then the policy maps the remaining samples onto the ticks:

| Policy                     | A tick gets the latest sample at or before it…                      | Use for                                                        |
| -------------------------- | ------------------------------------------------------------------- | -------------------------------------------------------------- |
| `SyncHold()`               | always                                                              | states valid until changed; default policy                     |
| `SyncAsOf(tolerance_ns=…)` | only if `tick − sample_ts ≤ tolerance_ns`                           | signals that go stale; exposes dropouts (argument required)    |
| `SyncDrop(step_ns=…)`      | only if `tick − sample_ts < step_ns`, i.e. in `(tick − step, tick]` | down/upsampling without repeating a sample (argument required) |

Ticks before a column's first sample are `None` with every policy, never back-filled. Two consequences
to tell the user about: `SyncHold` carries the last valid value across a run of NaN readings (a dropout
becomes a flat line), and `SyncAsOf` with a tolerance of one or two sample periods is how to see it. For
`SyncDrop`, `step_ns = int(1e9 / target_fps)` means "a new sample since the previous tick". Ask for the
rate, the policy and the tolerance when the request does not say; state the ones you chose otherwise.

## 4. scikit-learn

`SyncTransformer` has `fit`/`transform`/`fit_transform` and works as the first step of a `Pipeline`.
scikit-learn is not a dependency of `mosaicolabs`: `pip install scikit-learn` in the active virtual environment (never `--user`, never `sudo`).

```python
from sklearn.pipeline import Pipeline
from sklearn.preprocessing import FunctionTransformer, StandardScaler

def features(d):  # the grid's timestamp is not a feature
    return d.drop(columns="timestamp_ns").astype(float)

pipe = Pipeline([
    ("sync", SyncTransformer(target_fps=50, policy=SyncHold())),
    ("features", FunctionTransformer(features)),
    ("scale", StandardScaler()),
])
X = pipe.fit_transform(df)   # df: the concatenated frame of section 2
```

- Without the `features` step, `StandardScaler` also scales `timestamp_ns` (the guide's example does).
- `fit_transform` on every chunk refits the scaler on that chunk alone. For statistics over the whole
  sequence, concatenate first or fit on a first pass.
- `SyncTransformer` keeps its parameters in private attributes, so `get_params()`, `clone()`, `repr()`
  of the pipeline, `GridSearchCV` and `cross_val_score` raise `AttributeError`. Use it in a plain
  `Pipeline` and do not display the pipeline object in a notebook.
- Leading `None` ticks become NaN; drop or impute them before a model that rejects NaN.

## 5. Memory

- Memory per chunk grows with `window_sec`. If `window_sec` covers the whole range, the extractor loads
  it all at once and logs a warning: fine for minutes of IMU, not for hours of images.
- Dense rows per chunk are `window_sec × target_fps`; the guide suggests staying under 100 000.
- Select columns before `transform()`: syncing every flattened column (about eighty for one odometry
  topic) is slow, and past a hundred columns pandas floods `PerformanceWarning`s.
- Image topics: decode with `VideoDecodingTransformer(topics=[…])` before syncing, never after (see
  "Video Decoding for ML Pipelines" in the guide). `Message.from_dataframe_row(row, topic_name)` rebuilds a
  typed message from a row.

## 6. Where the docs are stale (checked against the installed source)

- `from mosaicolabs.ml.sync_policies import SyncHold` fails: import everything from `mosaicolabs.ml`.
- `to_pandas_chunks()` has no `batch_size` argument: windows are set by `window_sec` only.

When something else does not behave as written, read the installed source before working around it:
`python -c "import inspect, mosaicolabs.ml as m; print(inspect.getsourcefile(m.SyncTransformer))"`
gives the file (likewise for `DataFrameExtractor`, `SyncHold`…), or `help(...)` the function.

## 7. Run it, then answer

Run the script and show its output. Report the row count, the column names you kept, the grid (start,
rate, number of ticks), the policy with its parameter, and how many `None` cells each column has: an
empty or half-empty column is an answer about the data, so say whether it comes from a late start, a
dropout, NaN readings or a topic missing from the window.
