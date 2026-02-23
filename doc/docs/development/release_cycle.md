# Release Cycle

This document describes briefly how the release process is handled for Mosaico project.

We use [semantic versioning](https://semver.org/) `v<MAJOR>.<MINOR>.<PATCH>` to label every new official release.

## Development process

The basic idea is to use more than one develop branch to consent the progress of various versions simultaneously.

Below, we introduce the terminology of branches and tags involved in the process:

* `main`: this is the only stable branch, where every commit is an official release. Critical patches to the latest version are merged directly on this branch 
* `release/x.y.0`: this is the catch-all branch for the version `x.y.0`. Once ready it is merged back into `main` and deleted.
* `issue/<num>/x.y.z`: this kind of branch is associated to the corresponding Github issue `#<num>`. It can contain the development of a new feature or a bug-fix. It is a child of the corresponding `release/x.y.z` branch and it's merged back into it when completed 
* `hotfix/x.y.<z+1>`: this branch is intended to contain critical fixes. It is derived directly from `main` and merged back into it to produce the new official version `x.y.<z+1>`.
* `chore/x.y.<z+1>`: this is a particular branch used for maintainance tasks, like updating workflows or documentation.
* `vx.y.z` this tag is created when a new stable version is ready.

Let's have a look to an example

![git release cycle](../assets/git_flow.png)

## Maintenance process

Maintenance of older major versions (LTS) follows a slightly different process.

We add to the terminology the following branch:

* `lts/x` this branch is created from the last official release of version x present in `main` and lives until the end of support. Only fixes are permitted using `issue/<num>` branches. Once a new version is ready, it is tagged incrementing only the patch version (`vx.y.<z+1>`).

