# Eval: mosaico-sdk-query

Ten questions a user might ask, each with the result a correct query must return. The skill passes
when an agent that knows only the skill and the documentation it cites (https://docs.mosaico.dev) turns
every question into a script that runs and prints the expected result.

The expected results were computed without the query engine: by streaming every IMU and odometry sample
of each sequence and filtering in plain Python. A query that agrees with them agrees with the data.

## Dataset

Three bags of the UZH-FPV dataset, ingested with
[mosaico-test-datasets](https://github.com/mosaico-labs/mosaico-test-datasets) (`--datasets uzh_fpv`)
into an empty daemon:

| Sequence                         | Bag                                                                                         |
| -------------------------------- | ------------------------------------------------------------------------------------------- |
| `race_1`                         | https://download.ifi.uzh.ch/rpg/drone_racing_data/race_1.bag                                |
| `race_2`                         | https://download.ifi.uzh.ch/rpg/drone_racing_data/race_2.bag                                |
| `indoor_forward_3_davis_with_gt` | http://rpg.ifi.uzh.ch/datasets/uzh-fpv-newer-versions/v3/indoor_forward_3_davis_with_gt.bag |

Put the three files in one folder, set `PATH_TO_BAGS` in `src/Datasets/UZH_FPV/configs.py` to it, and
run the ingestion. Use a 0.8+ daemon and an SDK with the daemon's 0.X. On a daemon upgraded from 0.7,
wait until the store optimizer has rewritten every topic: until then ontology queries miss them.

Q9 is the NaN case: on a 0.8+ daemon a plain value condition on the odometry should find `race_2`; on
0.7 it needed the streaming fallback of the skill's section 6. Not yet run on these bags: the 0.8+
behavior was checked only on synthetic data (a topic with some NaN still matches, an all-NaN one does
not) against a 0.10 daemon.

## Running it

Give an agent the skill, the daemon address and `questions.md` only. Do not show it `expected.json`.
For each question it writes `qNN.py`, runs it and keeps the output. Compare each output with
`expected.json`: sets of sequences and topics must match exactly, time windows to the nanosecond.

Each `qNN.out` must end with a line `ANSWER:` followed by one item per line: a sequence name,
`sequence topic`, or `start end` in nanoseconds. Then:

```bash
python check.py <run-dir> [host] [port]
```

It prints the daemon version (`client.info()`), PASS or FAIL per question, and the total.

## Cited pages

The skill cites published URLs. From the repository root, check that each one answers 200 on the live
site (redirects followed); it prints nothing when all do:

```bash
grep -oE 'https://docs\.mosaico\.dev/[^ )`,:]+' .agents/skills/mosaico-sdk-query/SKILL.md | sort -u | while read -r u; do
  u=${u%.}; c=$(curl -sL -o /dev/null -w '%{http_code}' "$u"); [ "$c" = 200 ] || echo "$c $u"
done
```
