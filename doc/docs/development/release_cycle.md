# Release Cycle

This document describes briefly how the release process is handled for Mosaico project.

## Monorepo

Mosaico utilizes a monorepo structure to simplify integration and testing between `mosaicod` daemon and Python SDK.
While these components reside in the same repository, they are decoupled: each component maintains its own release schedule and both follow [semantic versioning](gttps://semver.org).

## Development workflow

The development workflow relies on a specific set of branches and tags to manage stability and feature development.

- `main`: the primary integration branch. All stable features and fixes eventually land here. Official release tags are cut directly from main once sufficient changes have been accumulated.
- `issue/<num>`: feature or bug-fix branches linked to a specific GitHub issue. Branched from *main* and merged back via pull request upon completion.
- `release/[mosaicod|mosaico-py]/vX.Y.<Z+1>`: maintenance branches for *critical hotfixes*. Created from an existing version tag *when a patch is required* for an older release. The final commit is tagged with the incremented version. Relevant fixes should be cherry-picked or merged back into main if applicable. These branches might be used in the future to support pre-release stages.

## Tags

We use specific tag prefixes to trigger CI/CD pipelines and distinguish between *stable releases* of the daemon and the SDK

| Component  | Tag                 |
| ---------- | ------------------- |
| Daemon     | `mosaicod/vX.Y.Z`   |
| Python SDK | `mosaico-py/vX.Y.Z` |
