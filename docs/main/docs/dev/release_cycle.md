---
title: Release Cycle
position: 1
---

## Monorepo

Mosaico utilizes a monorepo structure to simplify integration and testing between `mosaicod` daemon and the SDK.
While these components reside in the same repository, they are decoupled: each component maintains its own release schedule and both follow [semantic versioning](https://semver.org).

## Development workflow

The development workflow relies on a specific set of branches and tags to manage stability and feature development.

- `main`: the primary integration branch. All features and fixes land here. The version on `main` is always a development snapshot, suffixed with `-dev` (e.g. `v0.7.0-dev`).
- `issue/<num>`: feature or bug-fix branches linked to a specific GitHub issue. Branched from *main* and merged back via pull request upon completion.
- `release/[py|doc]/vX.Y`: release and maintenance branches. Created from `main` when a release cycle begins, or from an existing version tag when a critical hotfix is required for an older release. For example: `release/v0.7` for the daemon, `release/py/v0.7` for the Python SDK, `release/doc/v0.7` for the documentation.

## Release process

Releases follow a structured cycle designed to keep `main` always in a releasable state while allowing stabilization work to happen in isolation.

**Opening a release branch.** When enough changes have accumulated on `main` to warrant a new release, a `release/vX.Y` branch is cut. At that point, two version bumps happen simultaneously: the version on `main` is incremented to `vX.(Y+1).0-dev`, opening the next development cycle, while the release branch is set to `vX.Y.0-rc`, marking the start of the stabilization phase.

**Release candidates.** Every commit on the release branch automatically produces a Docker image. Two tags are published for each build: a floating `vX.Y.Z-rc` tag pointing to the latest candidate, and a fixed short-SHA tag (e.g. `vX.Y.Z-rc-abc1234`) that permanently identifies that specific build. This makes it possible to pin a deployment to any particular candidate for testing or rollback.

**Promoting to stable.** Once a release candidate is ready, the version is changed to `vX.Y.Z` and the commit is tagged. This tag triggers the CI/CD pipeline to produce the final release artifacts: prebuilt binaries, stable Docker images, and a documentation deployment.

## Tags

We use specific tag prefixes to trigger CI/CD pipelines and distinguish between *stable releases* of the daemon, SDK and documentation.
These tags are also used to generate prebuilt binaries, docker images and deployements.

| Component  | Tag                    |
| ---------- | ---------------------- |
| Daemon        | `vX.Y.Z`            |
| Python SDK    | `py/vX.Y.Z`         |
| Documentation | `doc/vX.Y`          |
