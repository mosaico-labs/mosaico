# Contributing to Mosaico
Thank you for your interest in contributing to Mosaico! 

The platform consists of two primary components:

1. **`mosaicod`**: The core backend service, written in **Rust**, which handles data ingestion, storage, and retrieval.
2. **`mosaico-sdk-py`**: The client library and ingestion tools, written in **Python**, which manage primary user interfaces, ROS bridging, and client communication.

We welcome contributions to both the core engine and the SDK to help stabilize APIs, optimize storage, and expand ROS support.

> [!IMPORTANT]
> **Legal Compliance**
> 
> All contributors must strictly comply with our [Intellectual Property & Clean Room Policy](./IP.md) to ensure the project remains open and legally unencumbered.
>
>Proposing major changes is subjected to [prior approval](#proposing-major-changes) by the project maintainers

## Did you find a bug?
Please report any bugs or issues in the [Issues](https://github.com/mosaico-labs/mosaico/issues) section of our repository.

## Do you want to contribute code?
### `mosaicod` (Backend)
* **Setup:** Ensure you have a working [Rust toolchain](https://rust-lang.org/tools/install/) installed.
* **Build Instructions:** Refer to the [`mosaicod` main guide](./README.md) for detailed instructions on building from source.
* **Database Schema:** If you modify the database schema, you **must** run `cargo sqlx prepare` and commit the generated `.sqlx` directory.
* **Safety:** Avoid `unsafe` blocks unless absolutely necessary for FFI or zero-copy buffer handling.
* **Style & Linting:** Your code must be formatted via `cargo fmt` and pass `cargo clippy` without warnings. CI actions are in place to enforce these checks.

### `mosaico-sdk-py` (Python SDK)
We use **Poetry** for dependency management.

* **Type Hints:** Use strict Python type hints. This is critical for our Ontology and `Serializable` models.
* **Batching:** When modifying `SequenceDataStreamer` or `TopicDataStreamer`, you **must preserve the batching strategy**. Do not load full sequences into RAM.
* **ROS Parsing:** Use `rosbags` for parsing. If you are adding support for custom messages, use the `ROSTypeRegistry`.

### Tests
Comprehensive testing is mandatory.

Tests can be executed using the `scripts/test_suite.sh` script. Note that integration tests are configured to run on a non-default port (`6276`) to prevent accidental writes to a live backend instance.

#### Prerequisites
* **Python:** Version 3.13 or newer.
* **Poetry:** Version 1.8.0 or higher.
* **FFmpeg:** Required if working on video decoding features.

#### Installation
```bash
# Install dependencies and setup virtual environment
poetry install

# Activate the shell
eval $(poetry env activate)

```

## Proposing Major Changes
If you intend to modify critical portions of the project (e.g., the core Rust engine, complex algorithms, or fundamental SDK architecture), we strongly recommend contacting the maintainers or opening a proposal issue **before** submitting a Pull Request.

This preliminary step ensures:

* **Alignment:** Verification that your proposed changes align with the Mosaico roadmap and do not conflict with ongoing developments.
* **Legal Clearance:** Early identification of whether your contribution involves core IP that requires specific legal affidavits regarding public domain dedication.

## Do you want to improve the documentation?
* Update the relevant documentation if you modify any public interfaces.
* We encourage adding code snippets to the `examples/` directory, specifically for ROS ingestion or Streamer usage.

## Submission
1. **Fork** the repository.
2. **Create a branch** for your feature or fix.
3. **Commit** your changes.
4. **Push** to your fork and submit a Pull Request.
5. **CI:** Ensure all checks (Tests, Clippy, Black/Ruff) pass before requesting a review.
