# Contributing to Mosaico

Mosaico is free and open-source software. However, for legal reasons, we are currently unable to accept open-source contributions (Pull Requests) from the general public.

If you are interested in contributing to the project and becoming an approved contributor, please reach out to us directly at [foss@mosaico.dev](mailto:foss@mosaico.dev). Once you're onboarded as a contributor, the process below applies to you.

> [!WARNING]
>
> Pull Requests opened by contributors who have not been approved through the process above will be closed immediately without review.

## Did you find a bug?

If you have found a bug please open a new [Discussion](https://github.com/mosaico-labs/mosaico/discussions/categories/issue).

## Did you find a security vulnerability?

**Please do not report security vulnerabilities through public GitHub Discussions or Issues.** Instead, report them privately via [GitHub Security Advisories](https://github.com/mosaico-labs/mosaico/security/advisories/new). This allows us to assess and address the issue before it is publicly disclosed.

## How to Submit a Change

> [!IMPORTANT]
>
> Proposing changes requires **[prior approval](#proposing-major-changes)** from the project maintainers.

We value your time (and ours), so we aim to avoid unnecessary work. Before opening a Pull Request, please [create a new Discussion](https://github.com/mosaico-labs/mosaico/discussions/categories/issue) so we can coordinate. Even if a PR is excellent, it may not align with our internal roadmap, and we want to ensure your efforts aren't wasted.

Once the discussion is finalized and a corresponding **Issue** is created, you can:

1. **Fork** the repository.
2. **Create a branch** for your feature or fix.
3. **Commit** changes with clear, descriptive messages.
4. **Push** to your fork and submit a **Pull Request**.
5. Ensure all automated checks (Tests, Clippy, Black/Ruff) pass before requesting a review.

## Do you want to contribute code?

The backend (`mosaicod`), Python SDK (`mosaico-sdk-py`), and documentation (`doc`) each live in their own directory. For build and setup instructions, see the [official documentation](https://docs.mosaico.dev/daemon).

## Proposing Major Changes

If you intend to modify critical portions of the project (e.g., the core Rust engine, complex algorithms, or fundamental SDK architecture), we strongly recommend [contacting the maintainers](mailto:foss@mosaico.dev) or opening a [discussion](https://github.com/mosaico-labs/mosaico/discussions).

This allows us to verify that your proposed changes align with the Mosaico roadmap and do not conflict with ongoing developments.

## Commit messages (Conventional Commits)

See the [release cycle documentation](https://docs.mosaico.dev/dev/release_cycle) for the commit message format.
