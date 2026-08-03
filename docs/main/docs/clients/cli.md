---
title: Mosaico CLI
sidebar_position: 4
description: Configure connections, inspect resources, and diagnose Mosaico from the command line.
---

The `mosaico` command is included with the Python SDK CLI extra:

```bash
pip install "mosaicolabs[cli]"
```

## Configure a profile

Interactive setup:

```bash
mosaico profile add local
```

Non-interactive setup:

```bash
mosaico profile add local \
  --no-interactive \
  --host localhost \
  --port 6726 \
  --default
```

Profile files are written with owner-only permissions on POSIX systems. Prefer the `MOSAICO_API_KEY` environment variable when credentials should not be stored locally.

## Diagnose a connection

```bash
mosaico doctor
```

The command checks the resolved profile, configuration permissions, TLS certificate path, DNS, and TCP connectivity. It never prints the API-key value.

For automation and AI development tools, request structured output:

```bash
mosaico doctor --output json
mosaico profile ls --output json
mosaico sequence ls --output jsonl
mosaico topic ls --output csv
```

JSON collection documents include a `schema_version`. Scripts should inspect that field before depending on the document structure.

Skip network checks when validating only local configuration:

```bash
mosaico doctor --no-network
```

The command exits non-zero when a required check fails. Warnings, such as overly broad configuration permissions, are reported without hiding otherwise useful diagnostics.
