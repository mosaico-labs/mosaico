---
title: Extending the CLI
sidebar_position: 3
description: "The design philosophy behind the mosaico CLI's command structure, and how third parties can add their own subcommands as standalone executables, with no plugin API and no source changes."
---

`mosaico` is organized as a resource-oriented command tree: `mosaico <resource> <verb>`, rather than one command with many flags. `sequence`, `topic`, `extension`, and `profile` are the resources; `ls`, `stat`, and a handful of resource-specific verbs are the actions performed on them. That structure is what makes the command set open-ended: a new resource is just a new top-level word, and nothing about adding one requires touching how the existing ones work.

The command set is, in fact, designed to be extended by anyone, without a plugin API to implement, without source changes, and without rebuilding the core tool.

## How dispatch works

Any subcommand that isn't one of the four built-in resources (`profile`, `sequence`, `topic`, `extension`) is resolved dynamically: when you run `mosaico <name> ...`, the CLI looks for an executable called `mosaico-<name>` on your `$PATH`. If it finds one, it hands off execution to it, passing through every remaining argument untouched.

```bash
mosaico topino scan --depth 3
# resolves to, and executes:
mosaico-topino scan --depth 3
```

Because dispatch happens before any parsing of your extension's own flags, your extension is completely free to define its own CLI surface, subcommands, flags, positional arguments, whatever it needs, with no coordination with the core tool.

Built-in resources always take priority: an executable named `mosaico-sequence` or `mosaico-profile` on your `$PATH` will never shadow the core `sequence` or `profile` commands. Extensions only fill in names the core CLI doesn't already own.

## Shared credentials

The connection profile resolved by the core CLI (from `--profile`, `MOSAICO_PROFILE`, or the configured default, see [Connection Profiles](profiles.md)) is forwarded to the extension as environment variables: `MOSAICO_DAEMON_URL`, `MOSAICO_API_KEY`, `MOSAICO_TLS`, `MOSAICO_CERT_PATH`. An extension written against those variables, for instance using the SDK's own `MosaicoClient`, automatically speaks to whichever profile the user selected, without parsing `--profile` itself or knowing anything about the config file's format. Credentials always live in one place, the core CLI's profile configuration, no matter how many extensions you have installed.

## Writing your own extension

1. Name the executable `mosaico-<name>` (any language works; it just needs to be executable and on `$PATH`).
2. Read connection details from the `MOSAICO_*` environment variables listed above, rather than defining your own connection flags or config file.
3. Install it anywhere on `$PATH`, e.g. `~/.local/bin/mosaico-topino`.

That's it: no manifest, no registration step, no dependency on the CLI's source. `mosaico extension ls` will pick it up automatically, and `mosaico topino ...` will route to it.
