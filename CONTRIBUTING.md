# Contributing to Mosaico

Thank you for your interest in contributing to Mosaico!

The platform consists of two primary components:

- **`mosaicod`**: The core backend service, written in **Rust**, which handles data ingestion, storage, and retrieval.
- **`mosaico-sdk-py`**: The client library and ingestion tools, written in **Python**, which manage user interfaces, ROS bridging, and client communication.

We welcome contributions to both the core engine and the SDK to help stabilize APIs, optimize storage, and expand ROS support.

> [!IMPORTANT]
>
> Proposing major changes requires **[prior approval](#proposing-major-changes)** from the project maintainers.

## Did you find a bug?

Please report any bugs or issues in the [Issues](https://github.com/mosaico-labs/mosaico/issues) section of our repository.

## Submission

1. **Fork** the repository.
2. **Create a branch** for your feature or fix.
3. **Commit** your changes with clear, descriptive messages.
4. **Push** to your fork and submit a Pull Request.
5. **CI:** Ensure all automated checks (Tests, Clippy, Black/Ruff) pass before requesting a review.

## Do you want to contribute code?

### Documentation

As Mosaico is in early development, documentation contributions are highly encouraged. You can expand our documentation in several ways:

* Improve code documentation: add missing docstrings or clarify existing ones.
* Enhance the public SDK documentation.
* Add new usage examples.

### Backend

The backend source code is located in the `mosaicod` directory.

* **Setup:** Ensure you have a working [Rust toolchain](https://rust-lang.org/tools/install/) installed.
* **Build Instructions:** Refer to the [`mosaicod` main guide](./mosaicod/README.md) for detailed instructions on building from source.
* **Database Schema:** If you modify the database schema, you **must** run `cargo sqlx prepare` and commit the generated `.sqlx` directory.
* **Safety:** Avoid `unsafe` blocks unless strictly necessary for FFI or zero-copy buffer handling.
* **Style & Linting:** Your code must be formatted via `cargo fmt` and pass `cargo clippy` without warnings.

### Python SDK

The SDK source code is located in the `mosaico-sdk-py` directory.

#### Prerequisites

- SDK requires Python 3.13+.
- Poetry 1.8.0+, for dependency management.
- To work on video decoding features, Mosaico requires FFmpeg. We currently use version 8.01; other versions have not been tested.

#### Guidelines

* **Type Hints:** Use strict Python type hints. This is critical for our Ontology and `Serializable` models.
* **Batching:** When modifying `SequenceDataStreamer` or `TopicDataStreamer`, you **must preserve the batching strategy**. Do not load full sequences into RAM.
* **ROS Parsing:** Use `rosbags` for parsing. If you are adding support for custom messages, use the `ROSTypeRegistry`.

---

### Tests

Comprehensive testing is mandatory for all logic changes.

Tests can be executed using the `scripts/tests.sh` script.

> [!NOTE]
>
> Integration tests are configured to run on a non-default port (**`6276`**) to prevent accidental writes to a live backend instance.

---

## Proposing Major Changes

If you intend to modify critical portions of the project (e.g., the core Rust engine, complex algorithms, or fundamental SDK architecture), we strongly recommend contacting the maintainers or opening a [discussion](https://github.com/mosaico-labs/mosaico/discussions) **before** submitting a Pull Request.

This allows us to verify that your proposed changes align with the Mosaico roadmap and do not conflict with ongoing developments.
