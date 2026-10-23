---
title: AI-assisted development
sidebar_position: 3
description: Repository guidance and validation expectations for developers working with AI coding agents.
---

AI coding tools should begin with the repository `AGENTS.md` and then read the closest component-specific instructions. These files define current commands, trust boundaries, and the minimum validation required for a change.

## Recommended workflow

1. Identify the affected Rust crates, Python packages, CLI commands, and documentation.
2. State the compatibility and security invariants before editing.
3. Run the smallest relevant tests while iterating.
4. Update examples and structured-output contracts with the implementation.
5. Run the change-impact validation described in `AGENTS.md` before handoff.

## Internal specialist agents

Maintainers can use the separate `mosaico-agents` repository for performance, security, consistency, and general maintenance reviews. Those agents share an evidence-first reporting contract:

- distinguish observations from confirmed defects;
- include commands, workloads, or code paths supporting conclusions;
- redact credentials and sensitive paths;
- report validation performed and residual risk;
- do not claim performance improvement without comparable before/after measurements.

## Machine-readable CLI diagnostics

Use the CLI rather than scraping formatted terminal output:

```bash
mosaico doctor --output json
mosaico profile ls --output json
```

Structured output is intended for automation. Human-oriented tables may change presentation without notice.

## CLI output implementation

`mosaicolabs_cli.output.OutputRenderer` accepts an explicit mapping from
`OutputFormat` to writer functions. Each command owns its mapping, so multiple
renderers can handle the same format with different input types and layouts.
There is no global registry, registration decorator, or import-order dependency.

A writer receives `(data, stream)` and returns `None`. The stream belongs to the
caller; writers must not close it. Dispatch does not consume or buffer the input.
Table writers own the presentation; CSV writers receive explicit column order;
JSON document writers own their envelopes. Use a safe public projection for
profiles instead of the credential-bearing `MosaicoProfile.to_dict()`.

For example, JSON Lines can consume a generator or append successive batches:

```python
from io import StringIO

from mosaicolabs_cli.output import OutputRenderer, render_jsonl
from mosaicolabs_cli.output_format import OutputFormat

renderer = OutputRenderer({OutputFormat.JSONL: render_jsonl})
stream = StringIO()
renderer.render(({"index": i} for i in range(2)), OutputFormat.JSONL, stream=stream)
renderer.render([{"index": 2}], OutputFormat.JSONL, stream=stream)
```

The shared JSON Lines writer flushes every complete record before requesting the
next. Producer failures propagate while leaving previously emitted lines valid.
The finite-collection JSON writer materializes its input for a single versioned
document. These different buffering policies are local to their writers.

Resolve the selected output before empty-result handling or starting a spinner.
Keep structured stdout free of progress messages, colors, and human explanations.
