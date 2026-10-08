# Changelog


## [0.7.0] - 2026-10-08

This release introduces the new **MCAP Bridge** (injection of MCAP files into Mosaico and reconstruction of the original MCAP from Mosaico data, with Protobuf and JSON Schema decoding), the **Watchdog** for automatic, unattended upload of MCAP and ROS bag files, and a reworked **ROS Bridge**, now moved to `mosaicolabs.bridges.ros`, with dry-run mode, sequence updates and scoped type registries. It also adapts the SDK to the backend's new **`info` action** (replacing `version`), using the server's own reported configuration to drive write-side batching instead of a hardcoded constant - correctly isolated per-connection, so a script talking to multiple Mosaico servers at once gets the right limits for each.

### Breaking Changes

- **`ros_bridge` package moved into `bridges`**: `mosaicolabs.ros_bridge` is now `mosaicolabs.bridges.ros`. The old import path still works but raises a deprecation warning and will be removed in a future release. ([#743](https://github.com/mosaico-labs/mosaico/pull/743))
- **Requires a `mosaicod` server >= 0.7**: the SDK relies on the new `INFO` action and on the reworked `GetFlightInfo` response, and is not compatible with older servers. ([#789](https://github.com/mosaico-labs/mosaico/pull/789), [#759](https://github.com/mosaico-labs/mosaico/pull/759))
- **`sequence_create()`, `sequence_update()` and `SequenceHandler.update()` no longer accept `max_batch_size_bytes`/`max_batch_size_records`**: the write-batching size is now always derived from the server's own reported message-size limit instead of a user-supplied override. ([#731](https://github.com/mosaico-labs/mosaico/pull/731))
- **The backend `VERSION` action was replaced by `INFO`**, which additionally reports the server's writing configuration (`grpc_max_decode_message_size`, `grpc_max_encode_message_size`, `grpc_target_encode_message_size`) alongside the version string. The server's decode and encode message-size limits are now decoupled, and the SDK derives its write-batching threshold from the decode limit. ([#731](https://github.com/mosaico-labs/mosaico/pull/731), [#820](https://github.com/mosaico-labs/mosaico/pull/820))
- **Topic platform metadata is no longer read from the Arrow schema metadata**: topic/sequence information (ontology tag, serialization format, timestamp range, message count, user metadata) is now read from the Flight `app_metadata` returned by `GetFlightInfo`, so client-provided schema/field metadata is no longer overwritten. `TopicHandler.serialization_format` (and the `serialization_format` field of the `Topic` model) now returns a `SerializationFormat` enum instead of a plain string. ([#759](https://github.com/mosaico-labs/mosaico/pull/759))
- **ROS loaders and injector reworked**: `ROSLoader`, `MosaicoLoader` and `ROSSequenceExtractor` now inherit from common `BaseLoader`/`SequenceBaseExtractor` bases shared with the MCAP Bridge, `TopicStatus` was replaced by the `CommonTopicStatus`/`ROSTopicStatus` hierarchy, the loaders accept either a typestore or a ROS distro, and `ProgressManager` moved to a dedicated `ui` module. Code subclassing or directly instantiating these classes may need to be updated. ([#706](https://github.com/mosaico-labs/mosaico/pull/706), [#794](https://github.com/mosaico-labs/mosaico/pull/794), [#837](https://github.com/mosaico-labs/mosaico/pull/837))
- **Examples moved**: the examples now live under `mosaicolabs.examples.bridges.{ros,mcap}` (e.g. `mosaicolabs.examples.ros_injection.main` is now `mosaicolabs.examples.bridges.ros.injection`). The `mosaicolabs.examples` CLI entry names `ros_injection` and `reconstruct_rosbags` are unchanged. ([#800](https://github.com/mosaico-labs/mosaico/pull/800))
- **ROSExtractorConfig field rename**: field `rosbag_path` has been renamed to `saving_path`

### Features

**MCAP Bridge**
- Introduced the new **`mosaicolabs.bridges.mcap`** package, with `MCAPBridge`, `MCAPAdapterBase`, `MCAPMessage` and a default **Unmodeled adapter** for MCAP channels without a dedicated ontology. ([#792](https://github.com/mosaico-labs/mosaico/pull/792))
- Implemented **MCAP message decoders** for **Protobuf** and **JSON Schema** encodings, with a decoder registry and a rule-based matcher to customize the decoding of specific fields. ([#791](https://github.com/mosaico-labs/mosaico/pull/791))
- Added schema converters from **Protobuf**, **JSON Schema** and **ROS 2 msg** definitions to PyArrow, including support for Protobuf **maps**, **`Any`** and **`oneof`** fields. ([#743](https://github.com/mosaico-labs/mosaico/pull/743), [#785](https://github.com/mosaico-labs/mosaico/pull/785))
- Implemented **`MCAPLoader`**, decoding MCAP messages into Mosaico `Message`s. ([#794](https://github.com/mosaico-labs/mosaico/pull/794))
- Added **`MCAPInjector`** / `MCAPInjectionConfig` and the new **`mosaicolabs.mcap_injector`** command-line script for injecting MCAP files into Mosaico. ([#797](https://github.com/mosaico-labs/mosaico/pull/797))
- **Mosaico → MCAP reconstruction**: the new `MCAPSequenceExtractor` rebuilds the original MCAP file from a Mosaico sequence through the adapters' `to_native()`. The original schema definition is now stored in the topic's `SchemaMetadata` (`schema_def`), and `SchemaMetadata` subclasses can declare `REQUIRED_KEYS` that are validated at creation time. ([#839](https://github.com/mosaico-labs/mosaico/pull/839))
- Added the `mcap_injection` and `reconstruct_mcaps` examples. ([#800](https://github.com/mosaico-labs/mosaico/pull/800), [#840](https://github.com/mosaico-labs/mosaico/pull/840))

**ROS Bridge**
- Moved the ROS Bridge from `mosaicolabs.ros_bridge` to `mosaicolabs.bridges.ros`, with backwards-compatible aliasing for the old import path. ([#743](https://github.com/mosaico-labs/mosaico/pull/743))
- **ROS injector**: added `update_if_exists` (`--update-if-exists`) to append the topics of a bag to an existing sequence (e.g. multi-part bags or reprocessed results), per-topic metadata via `topic_metadata` (`--topic-metadata`), and a **dry-run mode** (`--dry-run`) to preview the injection without writing any data. ([#706](https://github.com/mosaico-labs/mosaico/pull/706))
- **`ROSSequenceExtractor`** now supports a dry-run mode to preview the extraction results. ([#706](https://github.com/mosaico-labs/mosaico/pull/706))
- Introduced **`RosSchemaMetadata`** to manage the reserved `_ros_` topic-metadata namespace, and **scoped `ROSTypeRegistry`** instances that can be passed to the injector and the sequence extractor, improving the registration of custom message types. ([#706](https://github.com/mosaico-labs/mosaico/pull/706))
- Improved **Mosaico → ROS adapter resolution**: the adapter is looked up via the topic's `msgtype` first, then via the ontology tag (only if its schema fingerprint still matches the server's), falling back to an Unmodeled adapter otherwise; encoding errors are now handled and reported per topic. ([#703](https://github.com/mosaico-labs/mosaico/pull/703), [#706](https://github.com/mosaico-labs/mosaico/pull/706))

**Watchdog**
- Introduced the **Watchdog** (`mosaicolabs.watchdog`, also available as the `mosaicolabs.watchdog` command-line script), which monitors a folder (subfolders included) and automatically injects the MCAP and ROS bag files it finds, choosing the right injector for each file. It runs in `single-pass` or `daemon` mode, keeps track of already loaded and quarantined files, and supports configurable failure policies (`raise`, `retry`, `quarantine`). ([#849](https://github.com/mosaico-labs/mosaico/pull/849))
- Introduced the shared `InjectionConfigBase`/base injector module, extended by both the ROS and MCAP injection configs. ([#849](https://github.com/mosaico-labs/mosaico/pull/849))

**Client & Handlers**
- The SDK now discovers the server's message-size limit at connection time (via the new `info` action) and automatically derives its write-batching threshold from it, instead of a hardcoded constant. Each `MosaicoClient` connection carries its own resolved server configuration, so multiple concurrent connections to different servers each get correct, independent limits. ([#731](https://github.com/mosaico-labs/mosaico/pull/731))
- **`TopicWriter.push()` no longer silently drops an oversized record**: when a single record's size alone exceeds the transport limit, the writer reports it to the server as a topic-level notification (visible via `list_topic_notifications()`) and reflects it locally through the new `TopicWriterStatus.RecordTooLarge` status, `TopicWriter.last_error`, and the new `TopicWriter.dropped_record_count` property - the writer keeps accepting subsequent records instead of failing the whole upload. ([#731](https://github.com/mosaico-labs/mosaico/pull/731))

**Ontology**
- Added the **`ImageFormat.JPG`** format. ([#826](https://github.com/mosaico-labs/mosaico/pull/826))

### Bug Fixes

- Fixed typestore handling and custom message registration in the ROS Bridge components. ([#706](https://github.com/mosaico-labs/mosaico/pull/706))
- Client-provided Arrow schema/field metadata is no longer overwritten by the platform metadata. ([#759](https://github.com/mosaico-labs/mosaico/pull/759))

### Refactoring & Performance

- Introduced the `bridges` package, hosting the ROS and MCAP bridges together with their shared components (`BridgeAdapterBase`, `BaseSchemaMetadata`, `BaseLoader`, `SequenceBaseExtractor`, topic statuses and UI), and the `bridges.protocols` package for schema converters. ([#743](https://github.com/mosaico-labs/mosaico/pull/743), [#792](https://github.com/mosaico-labs/mosaico/pull/792), [#794](https://github.com/mosaico-labs/mosaico/pull/794), [#837](https://github.com/mosaico-labs/mosaico/pull/837))
- Topic time-window statistics are now read from `GetFlightInfo` and the internal platform metadata handling was consolidated into the new `platform.app_metadata` module. ([#759](https://github.com/mosaico-labs/mosaico/pull/759))
- Introduced `ConnectionContext`, bundling the Flight client together with the server configuration resolved at connection time, threaded through every internal handler/writer/reader factory in place of a bare `FlightClient`. ([#731](https://github.com/mosaico-labs/mosaico/pull/731))
- Removed the unused record-count buffering mode from the internal topic write buffer: batching is byte-size-only, matching actual runtime behavior. ([#731](https://github.com/mosaico-labs/mosaico/pull/731))
- Replaced a unit test that validated PyArrow IPC schema overhead against a hardcoded, disconnected 16MB constant with integration tests exercising real batch-splitting and the oversized-record path against a live server. ([#731](https://github.com/mosaico-labs/mosaico/pull/731))

### Dependencies

- Added `mcap` and `mcap-protobuf-support` as runtime dependencies. ([#743](https://github.com/mosaico-labs/mosaico/pull/743), [#797](https://github.com/mosaico-labs/mosaico/pull/797))
- Bumped `pyarrow` (25.0.1), `pydantic` (2.13.5), `rosbags` (0.11.5), `click` (8.5.0), `typer` (0.27.2).

## [0.6.3] - 2026-10-07

### Bug Fixes

- **`Image`**: fixed BGR(A) ↔ RGB(A) channel ordering in `to_pillow()`/`from_pillow()` conversions: only the color channels are now swapped, while the alpha channel stays last (previously `bgra*` encodings were fully reversed, moving alpha to the first position). Improved 16-bit handling: multi-channel 16-bit images are downscaled to 8-bit (most significant byte) when converted to Pillow, and 8-bit color data is expanded to the full 16-bit range (×257) when encoding to 16-bit, making the round-trip lossless. ([#826](https://github.com/mosaico-labs/mosaico/pull/826))
- **`DataFrameExtractor`**: the message at exactly `timestamp_ns_max` was dropped from the extraction. `timestamp_ns_start` is now an inclusive bound and `timestamp_ns_end` an exclusive one (`t < end`); when `timestamp_ns_end` is `None` or beyond the sequence `timestamp_ns_max`, the last window drains the readers so the final message of the sequence is included. ([#824](https://github.com/mosaico-labs/mosaico/issues/824))
- **ROS Bridge (`MosaicoLoader`, `ROSExtractorConfig`, sequence extractor CLI)**: aligned with the same bound semantics. An `end_timestamp_ns` beyond the sequence maximum is no longer clipped to `timestamp_ns_max` (which excluded the last message) but leaves the stream unbounded up to the end of the sequence. Docstrings and CLI help texts now state explicitly that the start bound is inclusive and the end bound exclusive.
- **`SyncTransformer`**: grid ticks are now computed from their index relative to the first observed timestamp (`origin + round(tick * period)`) instead of accumulating an integer-truncated step, eliminating the cumulative timing drift for target frequencies whose period is not an integer number of nanoseconds (e.g. 30 Hz). ([#829](https://github.com/mosaico-labs/mosaico/pull/829), closes [#787](https://github.com/mosaico-labs/mosaico/issues/787))

### Maintenance

- Updated locked dependencies (`poetry.lock`).

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
