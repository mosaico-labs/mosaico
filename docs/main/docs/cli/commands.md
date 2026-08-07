---
title: Commands
sidebar_position: 1
description: "Reference for every mosaico CLI command: sequence, topic, and extension listings, stats, and streaming, with the handler format used to pipe results between them."
---

The `mosaico` command line tool gives you terminal access to the platform catalog: listing and inspecting sequences and topics, and streaming raw message data, without writing any Python.

Install it with

```bash
pip install "mosaicolabs[cli]"
```

:::note
The commands below need a connection profile to reach a `mosaicod` instance. If you haven't configured one yet, start with [Connection Profiles](profiles.md), then come back here.
:::


## `mosaico sequence`

### `ls`

Lists sequences matching a search pattern.

```bash
mosaico sequence ls [OPTIONS]
```

| Option | Default | Description |
| :--- | --- | :--- |
| `--locator <PATTERN>` | `*` | Glob-style search pattern on the sequence name. |
| `--created-after <TIMESTAMP>` | | Only sequences created after this epoch-nanosecond timestamp. |
| `--created-before <TIMESTAMP>` | | Only sequences created before this epoch-nanosecond timestamp. |
| `-m`, `--metadata <KEY=VALUE>` | | Filter by user metadata. Repeatable; repeated filters are combined with a logical AND. |
| `--limit <N>` | | Cap the number of results returned. |
| `-o`, `--output <table\|csv>` | inferred | Force the output format (see [Piping](#piping)). |

#### Examples

List every sequence whose name starts with `drive_`:

```bash
mosaico sequence ls --locator "drive_*"
```

Filter on creation time and on two user metadata fields at once:

```bash
mosaico sequence ls --created-after 1704067200000000000 --metadata "vehicle=truck" --metadata "region=eu"
```

### `stat`

Shows detailed information (creation time, size, contained topics, time range, metadata) for one or more sequences.

```bash
mosaico sequence stat [HANDLER...]
```

Handlers can be passed as positional arguments or piped in via stdin; at least one source is required.

#### Examples

```bash
mosaico sequence stat "alpha_sequence,1700000000000000000,1700003600000000000"
```

```bash
mosaico sequence ls --locator "alpha_*" | mosaico sequence stat
```

## `mosaico topic`

### `ls`

Lists topics matching a search pattern.

```bash
mosaico topic ls [OPTIONS]
```

| Option | Default | Description |
| :--- | --- | :--- |
| `--locator <PATTERN>` | `*` | Glob-style search pattern on the topic locator (`<sequence>/<topic_path>`). |
| `-m`, `--metadata <KEY=VALUE>` | | Filter by user metadata. Repeatable; repeated filters are combined with a logical AND. |
| `--limit <N>` | | Cap the number of results returned. |
| `-o`, `--output <table\|csv>` | inferred | Force the output format (see [Piping](#piping)). |

#### Examples

```bash
mosaico topic ls --locator "*/imu/*"
```

### `stat`

Shows detailed information (creation time, size, ontology, time range, metadata) for one or more topics.

```bash
mosaico topic stat [HANDLER...]
```

Same handler resolution rules as `sequence stat`: positional arguments, or piped via stdin.

#### Examples

```bash
mosaico topic ls --locator "my_sequence/*" | mosaico topic stat
```

### `mcat`

Streams the raw messages of one or more topics to stdout as JSON Lines, one message per line. Because different topics can follow different ontologies (different fields entirely), this output is JSONL only; there is no tabular or CSV mode here.

```bash
mosaico topic mcat [OPTIONS] [HANDLER...]
```

| Option | Description |
| :--- | :--- |
| `--from-index <N>` | 0-based index of the first message to emit. |
| `--count <N>` | Number of messages to emit. Omit to stream everything from the starting index onward. |

Each emitted line carries the ontology fields plus `_timestamp` (epoch nanoseconds), `_topic`, and `_ontology`; any binary field is base64-encoded. If you pass several handlers that share the same topic locator but different time ranges, they're merged into a single stream covering the widest range (minimum start, maximum end) before reading begins.

#### Examples

Read the first 15 messages of a topic:

```bash
mosaico topic mcat --from-index 0 --count 15 "my_sequence/imu,1700000000000000000,1700003600000000000"
```

Chain a search straight into a read:

```bash
mosaico topic ls --locator "drive_01/*" | mosaico topic mcat --count 15
```

Extract a single field with `jq`:

```bash
mosaico topic mcat "my_sequence/imu,1700000000000000000,1700003600000000000" | jq '.x'
```

## `mosaico extension`

### `ls`

Lists every extension currently discoverable on `$PATH`, i.e. every executable named `mosaico-<name>`.

```bash
mosaico extension ls [--output table|csv]
```

If nothing is found, the command prints a hint instead of an empty table. Extensions are third-party subcommands, see [Extending the CLI](extending.md) for how they're built and dispatched.

## Piping

Most commands exchange CSV-formatted lines rather than raw arguments in the form of:

```
<locator>,<timestamp_ns_min>,<timestamp_ns_max>
```

- `<locator>` is a sequence name (e.g. `my_sequence`) or a topic path within a sequence (e.g. `my_sequence/imu/acceleration`); 
- the two timestamps are the time range, in epoch nanoseconds.

This is exactly what `sequence ls` and `topic ls` print in their CSV output, and exactly what `sequence stat`, `topic stat`, and `topic mcat` accept, so the typical flow is to pipe a listing straight into a follow-up command:

```bash
mosaico topic ls --locator "my_sequence/*" | mosaico topic mcat --count 100
```

Every listing command (`sequence ls`, `topic ls`, `extension ls`) detects whether its output is going to a terminal or being redirected. 
Attached to a TTY, it renders a formatted, colorized table. Piped into a file or another program, it switches to plain CSV, one record per line, so results can be fed straight into another command or into tools like `jq`, `grep`, or `awk`. You can force either mode explicitly with `--output table` or `--output csv`.

Chaining a search into a read this way turns each stage of the pipeline into a plain Unix filter: `topic ls` narrows the catalog down to a set of handlers by locator, `topic mcat` turns each handler into a stream of JSON messages, and a tool like `jq` reaches into those messages to pull out a single field. None of the three stages needs to know anything about the others beyond the line format they agree on, CSV handlers in, JSONL messages out.

For example, to read the X-axis linear acceleration reported by every `imu` topic across the catalog:

```bash
mosaico topic ls --locator "*/imu" | mosaico topic mcat | jq '.acceleration.x'
```

`topic ls` resolves the `*/imu` pattern to one handler per matching topic and prints them as CSV; `topic mcat` reads that CSV from stdin and streams every message from those topics as JSON Lines; `jq '.acceleration.x'` then picks out just the `x` component of the `acceleration` vector from each line. Narrow the locator (e.g. `--locator "drive_01/imu"`) or add `--count` to `mcat` to work on a smaller slice first.
