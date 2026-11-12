# Eval: mosaico-setup

Ten scenarios a user might bring, each with the outcome a correct agent must reach: the command output it
must show and, for failures, the diagnosis it must give. The skill passes when an agent that knows only
the skill and this repository's documentation reaches every expected outcome without writing to the
daemon.

The expected values were computed by running the commands against the daemon described below, in a
fresh virtual environment with a throwaway `HOME`.

## Dataset and daemon

A `mosaicod` 0.8.0-dev (plain gRPC, no TLS, API keys not enforced) on `localhost:6726`, holding three
bags of the UZH-FPV dataset ingested with
[mosaico-test-datasets](https://github.com/mosaico-labs/mosaico-test-datasets) (`--datasets uzh_fpv`):
`race_1`, `race_2`, `indoor_forward_3_davis_with_gt`. It is the same dataset as the
`mosaico-sdk-query` eval (see its README for the bag URLs).

PyPI's newest `mosaicolabs` is 0.7.0, still older than the 0.8.0-dev daemon. Scenarios 1 and 8 depend
on that gap. An unpinned install now gets 0.7.0, which fails with `Last error: ... Bad request: body
decode error`, so scenario 8 needs the pin `mosaicolabs[cli]==0.6.3` to reproduce its evidence.

## Running it

1. Give the agent the skill, the daemon address and `scenarios.md` only. Do not show it
   `expected.json`.
2. Run every scenario with `HOME` set to an empty scratch directory (profiles are written to
   `$HOME/.mosaico/config.toml`) and in a new virtual environment, never the user's real ones.
3. Failures are simulated on the client side only: a wrong host or port, a TLS flag, a key. Never stop,
   restart or reconfigure the daemon, and never create or delete data on it: other evals may share it.
4. Keep the transcript of each scenario in `s01.log` .. `s10.log`: the commands run, their output, and
   the agent's final answer.

## Grading

Compare each transcript with `expected.json`:

- `must_show`: the outputs the transcript must contain (sequence names as a set, order is not stable;
  numbers exactly).
- `error_pattern`: a regular expression (Python `re`, `re.MULTILINE`) the failing command's output
  must match, which proves the agent reproduced the failure rather than guessed it. `trap(s)` notes the
  wrong turn the scenario is built to catch.
- `diagnosis` / `must_say`: the cause the final answer must name. `must_not_say` lists the wrong
  conclusions that fail the scenario even when the rest is right.
- `forbidden`: actions that fail the scenario outright (writing to the daemon, `sudo`, printing a key).

`python check.py <run-dir>` applies the machine-checkable fields (error patterns, sequence lines,
scenario 4 numbers, profile lines, no daemon writes) and lists the fields left for a human reader.
It also maps every `docs.mosaico.dev` URL and repository doc path the skill cites back to a page in
this checkout and fails if one is missing.

A scenario passes when all of its fields are satisfied. Report the score as passed / 10.

## Corrections

- Round 1: the scenario 7 diagnosis told the admin to start `mosaicod` with `--tls`. That flag was removed
  in 0.7 (`docs/main/docs/daemon/migration.md`, and `mosaicod server` accepts only `--host` and `--port`);
  the diagnosis now names `MOSAICOD_TLS_ENABLED` and the two certificate variables. The expected outputs
  did not change.
- Round 2: no expected value changed. `check.py` now looks for the real daemon write paths (the SDK's
  `sequence_delete`, `session_delete`, `sequence_create`, `sequence_update`, `clear_*_notifications`,
  the `mosaicolabs.ros_injector` / `mcap_injector` modules, `docker kill`, `docker compose stop|kill`);
  the old pattern named CLI subcommands that do not exist. The scenario 4 ontology check now requires
  `topic stat` output (`Ontology: IMU`), not a prose mention. The scenario 10 `fix` lists every way the
  skill teaches to send a key read from a file (interactive prompt, `--api-key "$(cat file)"`,
  `MOSAICO_API_KEY`) instead of a literal `--api-key ...` only. Re-run on the round 2 transcripts: 10/10.
- Round 3 (main 5b41767, after #817): no expected value changed. The scenario 6 `error_pattern` named only
  `localhost:6727`, but the skill tells the runner to retry with `127.0.0.1` for a single stable error, so a
  transcript that followed the skill showed `'127.0.0.1:6727'` and failed the check. The pattern now accepts
  `(localhost|127\.0\.0\.1):6727`. Re-verified live: both hosts exit 1 with `Connection refused`, and port
  6726 lists the three sequences.
- Round 4 (main 5b41767): only the scenario 8 `evidence` text changed. It described the `info` answer
  as JSON; after #817 it is protobuf, and the cause is that SDK 0.6.3 probes with the `version` action,
  which #789 replaced with `info`. `_daemon.commit` pins the daemon build. `check.py` now counts
  `mosaicolabs.watchdog` (a console script that ingests) as a daemon write and checks the skill's cited
  doc pages. Re-run on the run817b transcripts: 10/10, no cited page missing.
