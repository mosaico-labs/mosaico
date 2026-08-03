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
