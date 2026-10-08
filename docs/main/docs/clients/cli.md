---
title: Mosaico CLI
sidebar_position: 1
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

## Output contracts

Listing commands default to a table on a terminal and headerless CSV when piped.
`doctor` defaults to a table on a terminal and JSON when piped. An explicit
`--output table|csv|json|jsonl` always takes precedence.

- JSON listings contain `schema_version` and a command-specific collection
  (`profiles`, `extensions`, `sequences`, or `topics`). An empty result retains
  the envelope, for example `{"schema_version": 1, "topics": []}`.
- JSON Lines emits one complete record per line, without a collection envelope
  or a version header. Each record is flushed as it is written. Empty JSON Lines
  and CSV listings produce no output; explanatory empty-result hints belong to
  table output only.
- Sequence and topic CSV retain the three fields expected by CLI pipelines:
  `locator,timestamp_ns_min,timestamp_ns_max`, without a header.
- Profile output never includes API-key values. JSON and JSON Lines expose only
  `api_key_configured` to indicate whether a key is configured.

`schema_version` identifies the JSON document contract independently of SDK and
server release versions. It changes for incompatible field, type, or meaning
changes, not when record values change. Version 1 retains string timestamps and
flattened `user_metadata` in sequence listings. JSON Lines uses the same record
fields; consumers must use documentation matching their installed CLI version.

The renderer supports incremental JSON Lines producers and repeated batches.
Current sequence/topic listing commands still collect their query results before
rendering; this does not add server-side streaming.
