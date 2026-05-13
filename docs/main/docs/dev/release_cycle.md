---
title: Release Cycle
position: 1
description: "Mosaico's monorepo branching strategy, semantic versioning policy, and release process for mosaicod and the SDK. Covers the relationship between daemon and SDK versions and how breaking changes are communicated."
---

## Monorepo

Mosaico uses a monorepo to keep `mosaicod` and the SDK in sync.
Both components are independently versioned and released, each following [semantic versioning](https://semver.org).

## Branches

### `main`

The central integration branch. All finished work lands here via pull request.
The version on `main` is always a `-dev` snapshot (e.g. `v0.7.0-dev`). It is never directly released.

### `issue/<num>`

Short-lived branches for a single feature or bug fix. Always branched from `main`, merged back via PR, then deleted.

### `release/[py|doc]/vX.Y`

Stabilization branch for a specific release, cut from `main`. Once created, only targeted fixes are backported into it, ongoing development continues on `main`.
Three parallel branches are cut per release:

- `release/vX.Y`: `mosaicod` daemon, covers the full `vX.Y.*` patch series.
- `release/py/vX.Y`: Python SDK.
- `release/doc/vX.Y`: documentation.

## Release

### Cut the release branch

When `main` is ready, three branches are cut from it: `release/vX.Y`, `release/py/vX.Y`, and `release/doc/vX.Y`. At that point several version bumps happen simultaneously:

- `main`: every module advances to `vX.(Y+1).0-dev`, opening the next development cycle.
- `release/vX.Y`: `mosaicod` is set to `vX.Y.0-rc`.
- `release/py/vX.Y`: the Python SDK is set to `py/vX.Y.0-rc`.

### Release candidates

Every commit on `release/vX.Y` produces a `mosaicod` container image with two tags:

- A **floating tag** (`vX.Y.Z-rc`) pointing to the latest candidate.
- A **pinned SHA tag** (e.g. `vX.Y.Z-rc-abc1234`) identifying that exact build.

### Promote to stable

Change the version from `vX.Y.Z-rc` to `vX.Y.Z` and tag the commit.
CI/CD produces the final release artifacts: binaries, stable container images, and a docs deployment.

### Patch releases

Commit fixes directly to `release/[py|doc]/vX.Y` (or backport from `main`), increment the patch version.

## Tags

| Component | Tag format | Notes |
|---|---|---|
| Daemon | `vX.Y.Z` | Triggers binary and container builds |
| Python SDK | `py/vX.Y.Z` | Triggers PyPI publish and SDK artifacts |
| Documentation | `doc/vX.Y` (lightweight) | Rolled forward on each update; triggers a docs deployment |
