# Contributing

If you plan to contribute to the codebase, you need to set up the pre-commit hooks in addition to the standard installation. These hooks enforce code quality checks automatically on every commit.

## Prerequisites

* **Python:** Version **3.10** or newer is required.
* **Poetry:** For package management.

## Development Setup

Clone the repository and navigate to the SDK directory:

```bash
cd mosaico/mosaico-sdk-py
```

Install dependencies **and** register the pre-commit hooks in a single step:

```bash
poetry install && poetry run pre-commit install
```

The second command installs the Git hook under `.git/hooks/pre-commit`, wiring it to the rules defined in `.pre-commit-config.yaml`. From that point on, every `git commit` will automatically run **Ruff** (linting and formatting) against your staged files — the commit is blocked if any check fails, keeping the codebase consistently clean.

> **Why this matters:** Skipping `pre-commit install` means your commits will bypass all quality gates. CI will catch the issues anyway, but fixing them after the fact is more disruptive than catching them locally before pushing.

## What the hooks do

The `.pre-commit-config.yaml` currently configures the following actions via [Ruff](https://docs.astral.sh/ruff/):

- **Linting** — detects common errors, unused imports, and style violations
- **Formatting** — auto-formats code to a consistent style

## Running checks manually

You can trigger the hooks on demand without committing:

```bash
# Run on all files
poetry run pre-commit run --all-files

# Run on staged files only
poetry run pre-commit run
```

## Verify the hook is installed

After setup, confirm the hook is in place:

```bash
ls .git/hooks/pre-commit
```

You should see the file present. If it is missing, re-run `poetry run pre-commit install`.
