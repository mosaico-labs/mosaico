# Contributing to Mosaico

The platform consists of two primary components:

1.  **`mosaicod`**: the core backend service written in Rust.
2.  **`mosaico-sdk-py`**: the client library and ingestion tools, written in **Python**.

We welcome contributions to both the core engine and the SDK to help stabilize APIs, optimize storage, and expand ROS support.

## Did you find a bug?

Please report the bug in the [Issue](https://github.com/mosaico-labs/mosaico/issues) section.

## Do you want to contribute to the code?

### `mosaicod`

- The core daemon handles data ingestion, storage, and retrieval.
- Setup a working [rust toolchain](https://rust-lang.org/tools/install/)
- See the instration on how to build from source in [`mosaicod` main guide](mosaicod/README.md)
- If database schema modifictions are made be shure to run `cargo sqlx prepare` and commit the `.sqlx` directory generated.
- Avoid `unsafe` blocks unless absolutely necessary for FFI or zero-copy buffer handling.
- Ensure your code passes `cargo clippy` without warnings and code must be formatted via `cargo fmt`, there are actions to do these checks.

### Python SDK 

The SDK handles serialization, ROS bridging, and client communication. We use **Poetry** for dependency management.

- Use strict Python type hints. This is critical for our Ontology and `Serializable` models.
- When modifying `SequenceDataStreamer` or `TopicDataStreamer`, preserve the **batching strategy**. Do not load full sequences into RAM.
- Use `rosbags` for parsing. If adding custom message support, use the `ROSTypeRegistry`.

### Tests

Make many tests.

Tests can be executed using the `scripts/test_suite.sh` script. Be aware that integration tests are configured to run on a non-default port (`6276`) in order to avoid writes on a live instance of the backend.

#### Prerequisites**

  * **Python:** version 3.13 or newer.
  * **Poetry:** version 1.8.0 or higher.
  * **FFmpeg:** required if working on video decoding features.

#### Installation

```bash
# Install dependencies and virtual env
poetry install

# Activate shell
eval $(poetry env activate)
```

## Do you want to improve the documentation?

  - Update documentation if you modify public interfaces.
  - We encourage adding snippets to `examples/` directory, specifically for ROS ingestion or Streamer usage.

## Submission

1.  **Fork** the repository.
2.  **Create a branch** for your feature or fix.
3.  **Commit** your changes.
4.  **Push** to your fork and submit a Pull Request.
5.  **CI:** Ensure all checks (Tests, Clippy, Black/Ruff) pass before requesting review.
