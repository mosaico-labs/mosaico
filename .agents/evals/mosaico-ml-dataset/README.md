# Eval: mosaico-ml-dataset

Seven dataset requests a user might make, each with the values a correct script must print. The skill
passes when an agent that knows only the skill, the skills it refers to and this repository's
documentation turns every request into a script that runs and prints the expected answer.

What each request exercises:

| #   | Request                                    | What a wrong script gets wrong                                      |
| --- | ------------------------------------------ | ------------------------------------------------------------------- |
| 1   | one topic to a DataFrame                   | column naming (`{topic}.{tag}.{field}`)                             |
| 2   | a time window, both ends included          | the extractor's exclusive end (1000 rows instead of 1001)           |
| 3   | two topics on a 100 Hz grid, `SyncHold`    | the grid origin and the last-value lookup                           |
| 4   | staleness threshold, `SyncAsOf`            | NaN readings at the end of `race_2` odometry, held by `SyncHold`    |
| 5   | upsampling without repeats, `SyncDrop`     | the `(tick − step, tick]` window                                    |
| 6   | scikit-learn `Pipeline` + `StandardScaler` | scaling the timestamp column, refitting per chunk                   |
| 7   | chunked extraction of a long recording     | chunks without a finished topic's columns (770 empty rows, not 581) |

The expected values were computed without `mosaicolabs.ml`: every IMU, odometry and pose sample was
streamed with the plain topic handlers (`docs/py/docs/SDK/handling/reading.md`), and the grid, the
windows and each policy were rebuilt in numpy following the semantics in
`mosaico-sdk-py/src/mosaicolabs/ml/sync_transformer.py` and `sync_policies/hold.py` (stated in
`questions.md`, so no answer depends on an unstated convention). The same requests written with
`mosaicolabs.ml` agree with them on every key.

## Dataset

The three UZH-FPV sequences of the `mosaico-sdk-query` eval (see its README for the bags and the
ingestion): `race_1`, `race_2`, `indoor_forward_3_davis_with_gt`. The daemon and the SDK must be the
same version.

## Running it

Question 6 needs scikit-learn in the agent's environment (`pip install scikit-learn`); it is not a
dependency of `mosaicolabs`.

Give an agent the skill, the daemon address and `questions.md` only. Do not show it `expected.json`.
For each question it writes `qNN.py`, runs it and keeps the output in `qNN.out`. Then:

```bash
python check.py <run-dir> [host] [port]
```

`check.py` connects with host and port only (no TLS, no API key): run it against a plain local daemon.
It prints `client.info()` (SDK 0.7 and later) or `client.version()` (SDK 0.6).

The skill cites one doc page by its published URL and, for a checkout, by its path; from the repository
root, check that the path still exists:

```bash
grep -o 'docs/py/docs/[^`]*\.md' .agents/skills/mosaico-ml-dataset/SKILL.md | xargs ls
```

It prints the daemon version, PASS or FAIL per question with the keys that differ, and the total.
Row counts and timestamps must match exactly, floating-point values to a relative 1e-6, column names as
a set.
