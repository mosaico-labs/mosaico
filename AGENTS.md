# Mosaico agent instructions

## Repository map

- `mosaicod/`: Rust daemon, storage, database, query engine, and Arrow Flight services.
- `mosaico-sdk-py/`: Python SDK, `mosaico` CLI, ontology, bridges, and tests.
- `docs/main/`: Product, daemon, examples, and contributor documentation.
- `docs/py/`: Generated Python SDK reference and LLM context output.
- `scripts/`: Development environment, release, and test entry points.

Read the nearest nested `AGENTS.md` before changing a component.

## Working rules

1. Preserve public Rust/Python protocol compatibility unless the task explicitly changes it.
2. Never print API keys, authorization metadata, private-key material, or unredacted profiles.
3. Treat CLI output selected with `--output json` or `--output jsonl` as a versioned interface.
4. Update user documentation and tests in the same change as a public CLI or SDK change.
5. Prefer targeted validation during iteration and the repository test runner before handoff.

## Authoritative commands

```bash
# Python formatting, linting, and unit tests
cd mosaico-sdk-py
poetry install --extras cli
poetry run ruff format --check .
poetry run ruff check .
poetry run pytest ./src/testing -k unit

# Rust formatting, linting, and tests
cd mosaicod
cargo fmt -- --check
cargo lint
cargo test

# Repository integration suites
./scripts/tests --help
./scripts/tests --full-stack
```

Use `./scripts/tests`, not `./scripts/test`. Start the daemon with the `server` subcommand.

## Change-impact minimums

| Change | Required validation |
| --- | --- |
| Python CLI | CLI unit tests, Ruff, help/output docs |
| Python ontology/schema | Unit round trip, schema fingerprint integration test |
| Rust action or wire format | Rust tests and Python full-stack tests |
| SQL query or migration | SQLx metadata/migration validation and database tests |
| Environment variable | Code, Compose examples, daemon docs, and CLI diagnostics |
| Documentation example | Both documentation builds and executable snippet check where possible |

## Handoff format

Report the behavior changed, validations run, compatibility or security implications, and any work deliberately left for a later change.
