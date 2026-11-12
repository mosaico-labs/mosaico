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
run the ingestion. The daemon and the SDK must be the same version.

## Running it

Give an agent the skill, the daemon address and `questions.md` only. Do not show it `expected.json`.
For each question it writes `qNN.py`, runs it and keeps the output. Compare each output with
`expected.json`: sets of sequences and topics must match exactly, time windows to the nanosecond.

Each `qNN.out` must end with a line `ANSWER:` followed by one item per line: a sequence name,
`sequence topic`, or `start end` in nanoseconds. Then:

```bash
python check.py <run-dir> [host] [port]
```

It prints the daemon version, PASS or FAIL per question, and the total. It reads the version with
`client.info()`, so it needs SDK 0.7 or later (0.6.x has `client.version()` instead).

## Cited pages

The skill cites published URLs. From the repository root, check that each one maps to a page that
exists here (`/python-sdk/<p>/` is `docs/py/docs/<p>.md`, any other `/<p>` is `docs/main/docs/<p>.md(x)`);
it prints nothing when all are present:

```bash
grep -oE 'https://docs\.mosaico\.dev/[^ )`,:]+' .agents/skills/mosaico-sdk-query/SKILL.md | sort -u | while read -r u; do
  p=${u#https://docs.mosaico.dev/}; p=${p%/}
  case $p in python-sdk/*) b=docs/py/docs/${p#python-sdk/} ;; *) b=docs/main/docs/$p ;; esac
  for f in $b.md $b.mdx $b/index.md; do [ -f "$f" ] && continue 2; done
  echo "MISSING $u"
done
```
