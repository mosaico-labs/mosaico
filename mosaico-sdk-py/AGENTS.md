# Python SDK and CLI instructions

These instructions extend the repository-level `AGENTS.md`.

## CLI contracts

- Put built-in commands in `src/mosaicolabs_cli/commands/` and register them in `main.py`.
- Keep profile resolution in `MosaicoProfile`; do not duplicate precedence rules in commands.
- Send human diagnostics to stderr when stdout is reserved for structured or piped data.
- Never serialize API-key values. Expose only a boolean such as `api_key_configured`.
- JSON collection output must include `schema_version`. JSON Lines emits one complete object per line.
- Avoid spinners, colors, headings, or explanatory prose in CSV, JSON, and JSON Lines output.
- Preserve non-interactive operation for automation and AI agents.

## Tests

- Unit CLI tests live in `src/testing/unit/cli/` and use `typer.testing.CliRunner`.
- Mock network and filesystem boundaries in unit tests.
- Add a test for redaction whenever configuration or authentication data is involved.
- Cover both terminal-friendly behavior and at least one structured-output mode.

## Security

- Write local configuration with owner-only permissions on POSIX systems.
- Treat discovered `mosaico-*` extensions as an external trust boundary.
- Do not forward credentials to a new subprocess or plugin without an explicit, documented permission model.
