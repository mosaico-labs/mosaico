# Eval: mosaico-onboarding

Fifteen questions a new Mosaico user might ask: twelve conceptual ones across the data model, sessions,
architecture, ontology, reading, query, security, the ML, ROS and MCAP bridges, the watchdog, the CLI and
the ecosystem, and three asking which page or skill to use. The skill passes when an agent that knows
only the skill and this repository's documentation answers every question with the expected facts, a
citation to an acceptable page for each claim, and none of the wrong claims.

Every expected fact in `expected.json` carries the documentation line it comes from (`path:line`). If a
page changes, re-check those lines before trusting the eval.

## Files

- `questions.md`: the questions, the only file the agent sees.
- `expected.json`: for each question, `must_contain` (facts with their source line),
  `acceptable_citations` (pages a correct answer may cite) and `wrong_claims` (statements that fail it).
- `check_paths.py`: maps every published URL the skill cites (`https://docs.mosaico.dev/...`) back to its
  page in this repository and verifies that it, every repository path in the skill and every path in
  `expected.json` exists. Run it after any change to the skill or the docs: `python check_paths.py`.

## Running it

Give an agent the skill and `questions.md` only; do not show it `expected.json`. No daemon is needed: the
questions are answered from the documentation. Keep each answer, then grade it against `expected.json`:

- **Pass** a question when the answer states every `must_contain` fact (any wording), cites at least one
  page from `acceptable_citations` (for routing questions, also names the expected skill), and contains
  none of the `wrong_claims`. A citation by published URL counts as the page it is built from
  (`https://docs.mosaico.dev/daemon` is `docs/main/docs/daemon/index.md`; the mapping is in `check_paths.py`).
- **Fail** it on a missing fact, a claim with no citation, a citation to a page that does not exist, or
  any wrong claim.

Grading is by a reviewer or a second agent holding `expected.json`; it is not automated.

## Limits

The skill's concept map states some expected facts almost verbatim (q2 SequenceUpdater, q9 ROS 2 `.mcap`
to the ROS bridge, q10 the 30 s `--min-file-age`): on those questions a pass shows the map is right, not
that an agent read the cited page. The other questions need the docs the skill points to.

## Corrections

- q2: the `SequenceUpdater` fact accepted only `SequenceHandler.update()`, but
  `docs/main/docs/learn/updating_sequences.mdx:47` presents `client.sequence_update()` as the preferred
  way. The fact now accepts either, and that page is an acceptable citation.
