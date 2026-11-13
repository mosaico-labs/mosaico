# Contributing to Mosaico

Mosaico is free and open-source software, and we welcome contributions. This guide explains how to propose a change and what we need from you before we can merge it.

## Did you find a bug?

If you have found a bug please open a new [Discussion](https://github.com/mosaico-labs/mosaico/discussions/categories/issue).

## Did you find a security vulnerability?

**Please do not report security vulnerabilities through public GitHub Discussions or Issues.** Instead, report them privately via [GitHub Security Advisories](https://github.com/mosaico-labs/mosaico/security/advisories/new). This allows us to assess and address the issue before it is publicly disclosed.

## How to Submit a Change

### Small fixes and dependency updates

Typos, broken links, similarly small changes, and dependency updates don't need any prior coordination: open a Pull Request directly.

### Everything else

We value your time (and ours), so we aim to avoid unnecessary work. Even if a PR is excellent, it may not align with our internal roadmap, so any other change has to start from one of the following:

- **An open, unassigned [Issue](https://github.com/mosaico-labs/mosaico/issues).** Comment on it to let everyone know you want to work on it. The first person to comment gets assigned, and only the assignee can open a Pull Request for that Issue. If the assignee stops working on it, a maintainer unassigns them and the Issue becomes available again. The maintainer who assigns you is also added as an assignee of the Issue and reviews the related Pull Request or Pull Requests that address it.
- **A new [Discussion](https://github.com/mosaico-labs/mosaico/discussions/categories/issue)** describing what you'd like to change. Once the discussion is finalized, a maintainer will create a corresponding Issue.

If you'd like to contribute but don't know where to start, look for issues labeled [`good first issue`](https://github.com/mosaico-labs/mosaico/issues?q=is%3Aissue%20is%3Aopen%20label%3A%22good%20first%20issue%22).

Once you're assigned to an Issue, you can:

1. **Fork** the repository.
2. **Create a branch** for your feature or fix.
3. **Commit** changes with clear, descriptive messages.
4. **Push** to your fork and submit a **Pull Request** that links the Issue.
5. Ensure all automated checks (Tests, Clippy, Black/Ruff) pass before requesting a review.

## Contributor License Agreement (CLA)

To contribute to Mosaico you must sign our Contributor License Agreement (CLA). This applies to every Pull Request, including small fixes and dependency updates.

After you open your Pull Request, a maintainer will ask you to sign the CLA. We can only merge your Pull Request once it's signed.

## Do you want to contribute code?

The backend (`mosaicod`), Python SDK (`mosaico-sdk-py`), and documentation (`doc`) each live in their own directory. For build and setup instructions, see the [official documentation](https://docs.mosaico.dev/daemon).

### Python SDK setup

Install [uv](https://docs.astral.sh/uv/getting-started/installation/) 0.13.1 or newer, then run from the repository root:

```bash
cd mosaico-sdk-py
uv sync --locked --extra cli
uv run --locked --extra cli pre-commit install
cd ..
./scripts/tests --sdk-python
```

The SDK supports Python 3.10 or newer. uv creates the local `.venv`, installs the SDK in editable mode, and includes development tools. Keep `--extra cli` on SDK runs so the optional CLI dependencies remain installed. Commit changes to `pyproject.toml` and `uv.lock` together. See the [development guide](docs/main/docs/dev/testing_and_dev.md) for dependency updates and documentation builds.

## Commit messages (Conventional Commits)

See the [release cycle documentation](https://docs.mosaico.dev/dev/release_cycle) for the commit message format.
