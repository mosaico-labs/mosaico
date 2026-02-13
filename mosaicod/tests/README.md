# Integration Tests

This folder contains the project's integration tests. These tests exercise multiple components together and validate the system-level behavior of the project.

## Layout

```
tests/
├── Cargo.toml        # test crate metadata
├── src/              # test helper modules and library code
└── tests/            # integration test files
```

## Test helpers in `src/`

The modules inside `src/` are designed to make writing integration tests simple and consistent.

- The module `common` contains `Server` and `Client` structs. Theese are designed to be lightweight helpers that expose a simple API for starting a test server and interacting with it from tests.
- The `actions` module contains basic action helpers, such as `sequence_create` and `topic_create`, which perform common setup operations used across tests.


## Running the tests

From the repository root you can run the integration test crate with:

```bash
cargo test -p tests
```
