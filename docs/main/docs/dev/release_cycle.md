---
title: Release Cycle
position: 1
---

## Monorepo

Mosaico uses a monorepo to keep `mosaicod` and the SDK in sync.
Both components live in the same repository but are independently versioned and released, each following [semantic versioning](https://semver.org) on its own schedule.


## Branches

The repository uses a small, deliberate set of branch types.
Understanding their purpose is the key to understanding how work flows from idea to release.

### `main`

The central integration branch. **All finished work lands here.**

- Every feature and bug-fix branch is opened from `main` and merged back via pull request.
- The version on `main` is always a `-dev` snapshot (e.g. `v0.7.0-dev`), reflecting work-in-progress toward the *next* release.
- `main` should always be in a buildable and testable state, but it is never directly released.

### `issue/<num>`

Short-lived branches for a single feature or bug fix, linked to a GitHub issue.

- Always branched from `main`.
- Merged back into `main` via pull request once the work is reviewed and approved.
- Deleted after merging.

### `release/[py|doc]/vX.Y`

Stabilization and maintenance branches for a specific release.

- **Created from `main`** when enough changes have accumulated to warrant a new release.
- Once cut, the release branch is isolated from ongoing development on `main` — only targeted fixes are backported into it.
- A single `release/vX.Y` branch covers the full `vX.Y.*` patch series for the daemon. The Python SDK and documentation have their own parallel branches: `release/py/vX.Y` and `release/doc/vX.Y`.
- If a critical fix is needed for an older version that is no longer on `main`, the release branch is re-opened from the relevant version tag.

---

## Release process

The release cycle is designed to keep `main` always moving forward while stabilization work happens in isolation on a dedicated branch.

### 1. Cut the release branch

When `main` is ready for a new release, a `release/vX.Y` branch is created from it.
At that moment two version bumps happen simultaneously:

- **On `main`**: the version advances to `vX.(Y+1).0-dev`, opening the next development cycle immediately.
- **On the release branch**: the version is set to `vX.Y.0-rc`, marking the start of stabilization.

From this point forward, `main` and the release branch diverge. New features continue to land on `main` while the release branch receives only bug fixes and stability improvements.

### 2. Release candidates

Every commit on the release branch automatically produces a Docker image with two tags:

- A **floating tag** (`vX.Y.Z-rc`) that always points to the latest candidate.
- A **fixed short-SHA tag** (e.g. `vX.Y.Z-rc-abc1234`) that permanently identifies that exact build.

The fixed tag makes it possible to pin a deployment to a specific candidate for validation or rollback.

### 3. Promote to stable

When the release candidate is considered ready, the version string is changed from `vX.Y.Z-rc` to `vX.Y.Z` and the commit is tagged.
This tag is the trigger for the CI/CD pipeline to produce the final release artifacts: prebuilt binaries, stable Docker images, and a documentation deployment.

### 4. Patch releases

If a bug is found after a stable release, fixes are committed directly to the `release/vX.Y` branch (or backported from `main`), the patch version is incremented, and the process repeats from step 2.

---

## Tags

Tags are the mechanism that triggers CI/CD and distinguishes stable releases across components.

| Component     | Tag format               | Notes                                                                 |
| ------------- | ------------------------ | --------------------------------------------------------------------- |
| Daemon        | `vX.Y.Z`                 | Triggers binary and Docker image builds                               |
| Python SDK    | `py/vX.Y.Z`              | Triggers PyPI publish and SDK artifacts                               |
| Documentation | `doc/vX.Y` (lightweight) | Moved forward on each doc update; triggers a documentation deployment |

Documentation uses a lightweight (non-annotated) tag because it is a rolling release, the tag is advanced with each update rather than pinning a specific commit.
