---
title: Release Cycle
position: 1
description: "Mosaico's monorepo branching strategy, versioning scheme, and weekly release process for mosaicod and the SDK. Covers how releases are tagged on main, how security updates are handled, and release signing."
---

## Monorepo

Mosaico uses a monorepo that holds `mosaicod`, the SDK and the documentation. All development happens on main: releases are weekly, and each one is a tag on main, one per component.

## Versioning

Neither `mosaicod` nor the SDK follows semantic versioning. Both use a version in the form `0.N.S`:

- `N` is an incremental number, increased by one with every weekly release.
- `S` is an incremental number for security updates to the `0.N` release. Every weekly release starts at `S = 0`.

The version number carries no compatibility guarantee: any weekly release can include breaking changes. The backend migration required by each release is documented in the dedicated migration section of the documentation.

The leading `0` marks the current phase, in which the API is not yet stable. The goal is to reach version `1.0.0`: from then on, Mosaico will follow [semantic versioning](https://semver.org) and commit to a stable API, with breaking changes requiring a new major version.

`mosaicod`, the SDK and the documentation are always released together and share the same version, security updates included: every `0.N.S` release contains `mosaicod`, the SDK and the documentation at that version.

### Breaking changes

What counts as a breaking change depends on the component:

- SDK: a change to a public API. Adding a new public API is not a breaking change.
- `mosaicod`: a change to the configuration files, the object store or the database.

For now, changes to the internal communication protocol between `mosaicod` and the SDK are not considered breaking changes.

Breaking changes are reported in the changelog of the component they affect.

## Release

`mosaicod` and the SDK follow a weekly release cycle. Every week a new `0.N.0` release is tagged on `main`. If no changes landed on `main` since the previous release, that week's release is skipped. Every release is signed.

Only the documentation of the latest release is available online. To read the documentation of an older release, check out its `v0.N.S` tag and build the documentation locally (see `docs/main/README.md`).

### Security updates

Security fixes always land on `main`, so upstream is always up to date with every security update.

Security updates for already published releases are released at the discretion of the development team. A security update to `0.N.S` is released as `0.N.(S+1)`: branch from the `v0.N.S` tag, apply the security patches, tag the result as `v0.N.(S+1)` and delete the branch.

## Branches

### `main`

The only long-lived branch. All finished work lands here via pull request, for the daemon, the SDK and the documentation. `main` must be in a releasable state at all times: every merged PR leaves it buildable, tested and ready to be tagged as a release.

There are no release branches: weekly releases are identified by tags on `main`. Security updates to published releases are the only exception, and are tagged on a temporary branch cut from the release tag, deleted once the new tag is created (see [Security updates](#security-updates)).

### `issue/<num>`

Short-lived branches for a single feature or bug fix. Always branched from `main`, merged back via PR, then deleted.

Documentation ships with the code it describes. Every issue that changes a behavior, an API or a configuration includes the matching documentation changes in the same branch, so code and documentation are merged and released together.

## Tags

Every release is identified by a single signed tag, shared by all components. Weekly releases are tagged on `main`, security updates on the temporary branch cut from the release they patch.

```
v0.N.S
```

The tag triggers the release of every component: the `mosaicod` binary and container builds, the PyPI publish of the Python SDK and the docs deployment.
