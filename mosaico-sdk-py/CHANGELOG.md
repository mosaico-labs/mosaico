# Changelog

All notable changes to this project are documented in this file.

---

## [Unreleased] — March 2026

### Breaking Changes

- **Timestamp field names renamed** — `created_timestamp` and `completed_timestamp` now replace previous inconsistent naming across Query structs and SDK parsing. Update any code that references the old field names. ([#311](https://github.com/mosaico-labs/mosaico/pull/311))
- **`api_key_create()` now accepts a single `permission` parameter** — the previous list-based signature has been removed. ([#313](https://github.com/mosaico-labs/mosaico/pull/313))
- **`SESSION_ABORT` action removed** — use `SESSION_DELETE` instead. ([#315](https://github.com/mosaico-labs/mosaico/pull/315))
- **`register_adapter` renamed to `register_default_adapter`** — update all call sites in code that registers custom ROS adapters. ([#279](https://github.com/mosaico-labs/mosaico/pull/279))

---

### New Features

#### SDK

- **PyPI distribution** — The package is now distributed as `mosaicolabs` on PyPI. Install with `pip install mosaicolabs`. CLI commands updated accordingly. ([#317](https://github.com/mosaico-labs/mosaico/pull/317))
- **`TopicWriterStatus`** — New exported class for tracking the topic-level ingestion lifecycle when using the writer as a context manager. A `last_error` property has also been added to `TopicWriter`. ([#295](https://github.com/mosaico-labs/mosaico/pull/295))
- **`with_user_metadata` in query API** — Query results can now include user-defined metadata via the new `with_user_metadata` flag. ([#270](https://github.com/mosaico-labs/mosaico/pull/270))
- **Runnable examples module** — A new `examples` module has been added inside `mosaicolabs`, along with a dedicated CLI to run SDK examples. Includes new catalog query examples and a `duration` property on the ROS loader. ([#300](https://github.com/mosaico-labs/mosaico/pull/300))
- **`session_delete` support** — `MosaicoClient.session_delete()` is now available. ([#241](https://github.com/mosaico-labs/mosaico/pull/241))

#### ROS Bridge

- **PointCloud2 support** — Added `PointCloud2` and `PointField` models to `ros_bridge/data_ontology`. Implemented `PointCloudAdapter` in `ros_bridge/adapters/sensor_msgs` as a fallback mechanism. ([#255](https://github.com/mosaico-labs/mosaico/pull/255))
- **Adapter override in `RosbagInjector`** — Custom adapter resolution logic added to `RosbagInjector` via a new parameter in `RosbagInjectionConfig`. `register_default_adapter` replaces the old `register_adapter`. ([#279](https://github.com/mosaico-labs/mosaico/pull/279))

#### Security

- **API Key permission documentation** — All `MosaicoClient` methods now document their required API Key permission level (Read, Write, Delete, Manage). A permission level table and usage examples have been added to the client documentation. TLS section now precedes the API Key section. ([#315](https://github.com/mosaico-labs/mosaico/pull/315))
- **API Key fingerprint support** — `MosaicoClient` now exposes the fingerprint of the key used during connection. `_get_fingerprint()` has been extracted as a dedicated internal function. ([#313](https://github.com/mosaico-labs/mosaico/pull/313))

---

### Bug Fixes

- **`TopicWriter.__exit__` context manager** — Exception suppression and propagation were incorrectly handled. Refactored `__exit__` for correct error lifecycle management. `_BaseSessionWriter._close_topics` updated accordingly. ([#293](https://github.com/mosaico-labs/mosaico/pull/293), [#295](https://github.com/mosaico-labs/mosaico/pull/295))
- **`API_KEY_CREATE` payload construction** — Fixed a bug in the payload builder for the `API_KEY_CREATE` do-action. ([#313](https://github.com/mosaico-labs/mosaico/pull/313))
- **`APIKeyStatus`** — Removed `api_key_fingerprint` from `APIKeyStatus`; the fingerprint must now be used directly when querying status. ([#313](https://github.com/mosaico-labs/mosaico/pull/313))
- **Error handlers** — Updated to raise exceptions instead of silently returning default values. ([#313](https://github.com/mosaico-labs/mosaico/pull/313))

---

### Refactoring

- **Detached versioning** — `mosaicod` and `mosaico-sdk` are now versioned independently. Updated README badges, GitHub Actions workflows, Dependabot configuration, and added a release script. ([#328](https://github.com/mosaico-labs/mosaico/pull/328))
- **Session and Topic manifest** — Topic and Sequence system information moved inside the manifest. `TopicMetadata` renamed to `TopicManifest`. ([#275](https://github.com/mosaico-labs/mosaico/pull/275))
- **`SessionLevelErrorPolicy` and split writer configs** — Introduced `SessionLevelErrorPolicy` alongside the existing `OnErrorPolicy` (kept for backwards compatibility). `WriterConfig` split into `SequenceWriterConfig` and `TopicWriterConfig` to support the new error policies. ([#268](https://github.com/mosaico-labs/mosaico/pull/268))
- **`APIKeyPermissionEnum`** — Added to the enum module API reference. ([#313](https://github.com/mosaico-labs/mosaico/pull/313))
- **Cache removal** — Switched to `dict.pop()` for safer cache entry removal in `MosaicoClient`. ([#313](https://github.com/mosaico-labs/mosaico/pull/313))

---

### Tests

- **Integration tests for `TopicLevelErrorPolicy`** — Full integration test suite written for topic-level error handling. ([#295](https://github.com/mosaico-labs/mosaico/pull/295))
- **Integration tests for API Key management** — Test fixtures reorganized into an integration-specific `conftest`. Added `--full-stack-api-key` CLI option with automatic API key generation. ([#313](https://github.com/mosaico-labs/mosaico/pull/313))
- **Integration tests for `session_update` and `session_delete`** — Covers session lifecycle operations. `test_tls_connections.py` now skipped when `--tls` option is not provided. ([#241](https://github.com/mosaico-labs/mosaico/pull/241))

---

### Documentation

- **Secure connection how-to** — New guide added for TLS and API Key configuration. A dedicated "Security Layer" section is now available in `client.md`. ([#300](https://github.com/mosaico-labs/mosaico/pull/300))
- **Examples section** — New `examples/` section in the SDK documentation with `examples/index.md` as the entry point, including how-tos and runnable code. ([#300](https://github.com/mosaico-labs/mosaico/pull/300))
- **ROS Bridge documentation** — Streamlined section headings, clarified adapter usage patterns, and added `PointCloud2` to the supported message types table. ([#293](https://github.com/mosaico-labs/mosaico/pull/293), [#255](https://github.com/mosaico-labs/mosaico/pull/255))
- **API Key permission table** — Added to client documentation with per-method permission requirements and usage examples. ([#315](https://github.com/mosaico-labs/mosaico/pull/315))

---

## [0.3.0] — March 2026

> This release consolidates Python 3.10 support, introduces session management, TLS, API Key handling,
> a complete online documentation site, and several foundational refactors across the SDK and server.

### Highlights

- Python minimum version lowered to **3.10** (previously 3.13). Added `typing_extensions` and `deprecated` as compatibility shims.
- Introduced **session-based ingestion** as the primary data writing abstraction.
- Added **TLS support** to `MosaicoClient` via `enable_tls` parameter and `MOSAICOD_TLS_CERT_FILE` environment variable.
- Added **API Key flight actions**: `create`, `status`, `revoke`.
- Added **auth middleware** for API Key interception and permissions filtering.
- Launched **online documentation** via MkDocs with multi-version support (mike) and an AI-optimized documentation page.
- Introduced the **ML module** with `DataFrameExtractor` and `SyncTransformer` components.
- Added **`MosaicoClient.list_sequences()`** for enumerating sequence names.
- Added **time-bounded data retrieval** for sequence and topic streams (`start_timestamp_ns` / `end_timestamp_ns`).
- Added **multi-ontology query support**.
- Added **timestamp range** in query results via `include_timestamp_range`.
- Added **PointCloud2** to the ROS bridge data ontology and adapters.
- Introduced **`DataFrameExtractor`** and `Message.from_dataframe_row()` for pandas round-trip workflows.
- Added scalar ontology models: **`Temperature`**, **`Pressure`**, **`Range`**.
- Added **`VarianceMixin`** to scalar ontology types.
- Improved **batch size calculation** using PyArrow native API (~10–100x faster).
- Introduced **`StatefulDecodingSession`** for H.264 and stateful image formats.
- Added **Ruff** and **pre-commit** for uniform code formatting across the Python SDK.
- Security fix: **Docker database and storage ports bound to `localhost` only** to prevent exposure via Docker port mapping.

---

> **Note:** Automated dependency bumps (Dependabot) for `ruff`, `numpy`, `pillow`, `pyarrow`, `pandas`, `opencv-python`, `rich`, `pydantic`, `av`, `click`, `pytest`, and `pyarrow-stubs` are not listed individually in this changelog.
