# Contributing to Mosaico

> [!IMPORTANT]
>
> Proposing changes requires **[prior approval](#proposing-major-changes)** from the project maintainers.

## Did you find a bug?

If you ave found a bug please open a new [Discussion](https://github.com/mosaico-labs/mosaico/discussions/categories/issue).

## How to Submit a Change

We value your time (and ours), so we aim to avoid unnecessary work. Before opening a Pull Request, please [create a new Discussion](https://github.com/mosaico-labs/mosaico/discussions/categories/issue) so we can coordinate. Even if a PR is excellent, it may not align with our internal roadmap, and we want to ensure your efforts aren't wasted.

Once the discussion is finalized and a corresponding **Issue** is created, you can:

1.  **Fork** the repository.
2.  **Create a branch** for your feature or fix.
3.  **Commit** changes with clear, descriptive messages.
4.  **Push** to your fork and submit a **Pull Request**.
5.  **CI:** Ensure all automated checks (Tests, Clippy, Black/Ruff) pass before requesting a review.

## Do you want to contribute code?

### Documentation

The documentation is located in the `doc` directory.

As Mosaico is in early development, documentation contributions are highly encouraged. You can expand our documentation in several ways:

* Improve code documentation: add missing docstrings or clarify existing ones.
* Enhance the public SDK documentation.
* Add new usage examples.


### Backend

The backend source code is located in the `mosaicod` directory. For detailed instructions on building from source, please refer to the [official daemon documentation](https://docs.mosaico.dev/latest/daemon). 

#### Update DBMS Schema

When modifying the database schema, you must navigate to the `mosaicod/crates/mosaicod-db` folder, run the command `cargo sqlx prepare -- --features postgres`, and ensure the generated `.sqlx` directory is committed to the repository. 

#### Coding Style

Regarding code safety, you should avoid `unsafe` blocks unless they are strictly required for FFI or zero-copy buffer handling. 
All code must be formatted using `cargo fmt` and must pass `cargo clippy` without any warnings to maintain high code quality standards.

### Python SDK

The SDK source code is located in the `mosaico-sdk-py` directory.

#### Guidelines

* **Type Hints:** Use strict Python type hints. This is critical for our Ontology and `Serializable` models.
* **Batching:** When modifying `SequenceDataStreamer` or `TopicDataStreamer`, you **must preserve the batching strategy**. Do not load full sequences into RAM.
* **ROS Parsing:** Use `rosbags` for parsing. If you are adding support for custom messages, use the `ROSTypeRegistry`.

### Tests

Comprehensive testing is mandatory for all logic changes.

Tests can be executed using the `scripts/tests.sh` script.

> [!NOTE]
>
> Integration tests are configured to run on a non-default port (**`6276`**) to prevent accidental writes to a live backend instance.


## Proposing Major Changes

If you intend to modify critical portions of the project (e.g., the core Rust engine, complex algorithms, or fundamental SDK architecture), we strongly recommend contacting the maintainers or opening a [discussion](https://github.com/mosaico-labs/mosaico/discussions) **before** submitting a Pull Request.

This allows us to verify that your proposed changes align with the Mosaico roadmap and do not conflict with ongoing developments.

## Pre-Release Checklist
Ensure the following files are updated to the target version number:
- **Rust**: `mosaicod/Cargo.toml`
- **Python**: `mosaico-sdk-py/pyproject.toml`
