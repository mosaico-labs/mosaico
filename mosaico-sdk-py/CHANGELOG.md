# Changelog


## [0.6.1] - 2026-08-27

### Bug Fixes

- Changed the warning type emitted for deprecated import paths from `DeprecationWarning` to `FutureWarning`, so it is no longer silenced by default. ([#742](https://github.com/mosaico-labs/mosaico/pull/742))

### Documentation

- Reworked the SDK README: fixed broken links, added a Key Features overview, Quick Start examples for data ingestion and querying (previously only reading was covered), and direct links to the Client, Ontology, Data Handling, Query, and ROS Bridge documentation pages.

## [0.6.0] - 2026-07-30

This release completes the **Mosaico ↔ ROS round-trip translation** (including support for **Unmodeled ontologies**), introduces **class-free queries via Queryable Fields**, expands the **query engine** with list queries, the `outside()` operator, and the new **clusterize/intersect** temporal-window actions, and includes several performance and bug fixes.

### Breaking Changes

- **`query` package moved out of `models`**: `mosaicolabs.models.query` is now `mosaicolabs.query`. The old import path still works but raises a deprecation warning and will be removed in a future release. ([#657](https://github.com/mosaico-labs/mosaico/pull/657))
- **`Message` no longer has `recording_timestamp_ns` and `frame_id`**, and the semantic meaning of `timestamp_ns` has changed: it now represents a **monotonic timer** value rather than Unix time. Code relying on `timestamp_ns` as wall-clock time must be updated. ([#564](https://github.com/mosaico-labs/mosaico/pull/564))
- **`QueryTopic.with_name_match()` and `QuerySequence.with_name_match()` now matches against a glob-style pattern instead of requiring an exact string**. A plain string such as `"image_raw"` now requires an exact match against the topic/sequence name; to match anywhere in the name as before, wrap it in wildcards (e.g. `"*image_raw*"`). ([#622](https://github.com/mosaico-labs/mosaico/pull/622))

### Features

**ROS Bridge**
- Implemented Mosaico → ROS bag translation (`to_ros()`) across all ontology adapters, enabling full round-trip conversion from Mosaico messages back into ROS bags. ([#518](https://github.com/mosaico-labs/mosaico/pull/518))
- Added **`Path`**, **`Temperature`** and **`Pressure`** ROS adapters. ([#540](https://github.com/mosaico-labs/mosaico/pull/540))
- The ROS message type and enum are now loaded into Mosaico topic metadata, improving adapter resolution. ([#577](https://github.com/mosaico-labs/mosaico/pull/577))
- Adapter resolution now checks the **msgtype in topic metadata first**, falling back to the ontology tag. ([#637](https://github.com/mosaico-labs/mosaico/pull/637))
- Implemented the **Mosaico ↔ ROS adapter for Unmodeled ontologies**, supporting round-trip conversion of unregistered message types, including nested types and lists. ([#644](https://github.com/mosaico-labs/mosaico/pull/644))

**Ontology & Serialization**
- Introduced **`HeaderMixin`**: message timestamps now flow through `Serializable` via a dedicated `Header`, standardizing timestamp semantics across ontology types. Added the **`Duration`** ontology. ([#564](https://github.com/mosaico-labs/mosaico/pull/564))
- Added the **`Unmodeled`** ontology class and **class-free Queryable Fields**, enabling ingestion and querying of ROS message types that don't have a dedicated ontology class. ([#633](https://github.com/mosaico-labs/mosaico/pull/633))

**Query Engine**
- The **`match` operator** now accepts a custom, simplified regex syntax on sequence and topic metadata, in addition to sequence/topic names. ([#622](https://github.com/mosaico-labs/mosaico/pull/622))
- **Glob patterns `*` and `**`** are now supported as wildcards when querying `user_metadata` keys. ([#622](https://github.com/mosaico-labs/mosaico/pull/622))
- Queries over **lists of basic types** and **lists of PyArrow structs** are now supported. ([#600](https://github.com/mosaico-labs/mosaico/pull/600))
- Implemented **`clusterize()`** and **`intersect()`** SDK actions on `QueryResponseItem`/`QueryResponseItemTopic` for temporal window slicing. ([#614](https://github.com/mosaico-labs/mosaico/pull/614))
- New **`outside()`** operator added to queryable fields. ([#673](https://github.com/mosaico-labs/mosaico/pull/673))
- Moved the query package from `mosaicolabs.models` to `mosaicolabs.query`, with backwards-compatible aliasing for the old import path. ([#657](https://github.com/mosaico-labs/mosaico/pull/657))

**Streaming**
- Added **timestamp properties** to `TopicDataStreamer` and `SequenceDataStreamer`, with integration tests validating the time info returned by the server.

### Bug Fixes

- Fixed the **numpy array type** used when encoding messages from Mosaico to ROS. ([#681](https://github.com/mosaico-labs/mosaico/pull/681))

### Refactoring & Performance

- Optimized `Message` construction by removing redundant per-instance work. ([#676](https://github.com/mosaico-labs/mosaico/pull/676))
