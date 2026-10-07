---
title: MCAP
description: MCAP-Mosaico Bridge
---

The **MCAP Bridge** module serves as the bidirectional gateway between [MCAP](https://mcap.dev/) files and the Mosaico Data Platform: **ingesting** MCAP files into Mosaico sequences (via [`MCAPInjector`][mosaicolabs.bridges.mcap.MCAPInjector]), and **extracting** Mosaico sequences back out as MCAP files (via [`MCAPSequenceExtractor`][mosaicolabs.bridges.mcap.MCAPSequenceExtractor]). MCAP is a self-describing container: every channel points to a schema record that embeds the full definition of its messages. The bridge relies on this to ingest message types it has never seen before, without requiring any message definition or generated code up front.

!!! info "API-Keys"
    When the connection is established via the authorization middleware (i.e. using an [API-Key](../client.md#2-authentication-api-key)), the MCAP Ingestion employs the mosaico [Writing Workflow](../handling/writing.md), which is allowed only if the key has the `write` permission.

!!! warning "Supported schema encodings"
    The MCAP Bridge currently supports channels whose schema is encoded with `protobuf`. To ingest MCAP files recorded as ROS 2 bags, use the [ROS Bridge](ros.md) instead. See [Supported Schema Encodings](#supported-schema-encodings) for details.

The core philosophy of the module is the same as the [ROS Bridge](ros.md): **"Adaptation, Not Just Parsing."** Every message is translated into the **Mosaico Ontology** rather than stored as an opaque payload. Since an MCAP file can carry any schema, the bridge cannot ship adapters for them in advance: by default, every channel is translated into an [`Unmodeled`][mosaicolabs.models.core.unmodeled.Unmodeled] ontology generated at runtime from the schema embedded in the file. Specific schemas can then be mapped onto built-in or custom ontologies by writing a [custom adapter](#extending-the-bridge-custom-adapters).

!!! example "Try-It Out"
    You can experiment yourself the MCAP Bridge ingestion via the **[MCAP Ingestion](https://docs.mosaico.dev/examples/MCAP/mcap_ingestion) Example**.

## Architecture

The module is composed of collaborating components that handle both directions of the pipeline, MCAP file → Mosaico (ingestion) and Mosaico → MCAP file (extraction), from raw file and server access down to the shared adaptation layer.

### The Loaders (`MCAPLoader` & `MosaicoToMCAPLoader`)

Each direction has its own loader:

* **[`MCAPLoader`][mosaicolabs.bridges.mcap.loader.MCAPLoader]** (ingestion) acts as the abstraction layer over the physical MCAP file. It uses the [`mcap`](https://pypi.org/project/mcap/) library to read the file summary (channels, schemas, message counts) and delegates the decoding of each message to the [Decoder](#the-decoders) registered for the channel's message encoding. Every message is yielded as an [`MCAPMessage`][mosaicolabs.bridges.mcap.MCAPMessage], a container holding the decoded payload together with its channel and schema information.
* **[`MosaicoToMCAPLoader`][mosaicolabs.bridges.mcap.loader.MosaicoToMCAPLoader]** (extraction) is the mirror image: it streams messages back out of a Mosaico sequence via [`SequenceDataStreamer`][mosaicolabs.handlers.SequenceDataStreamer], resolving each topic's adapter from the `_mcap_` metadata recorded at ingestion time (see [Metadata: Reserved Keys & Custom Fields](#metadata-reserved-keys-custom-fields)).

Both loaders share the topic classification logic of the [ROS Bridge](ros.md) loaders, which is what powers `topics`, `rejected_topics`, and `resolve_adapter()` identically on either side.

* **Responsibilities:** decoding (or, for `MosaicoToMCAPLoader`, remote streaming) and channel filtering (supporting glob patterns like `/cam/*`).
* **Error Handling:** rejected channels are reported with a specific reason (filtered, unavailable schema, unresolved decoder, unresolved adapter, malformed metadata) rather than aborting the whole run; malformed *messages* on an otherwise accepted channel are skipped and counted rather than raised.

!!! note "Channel names and topic names"
    MCAP channel names become Mosaico topic names with dots replaced by slashes and a leading `/` (e.g. `front_car.imu` becomes `/front_car/imu`). The ingestion options (`channels`, `topic_metadata`, `topics_on_error`, `serialization_formats`) refer to the original channel names, while the extraction `topics` filter refers to the Mosaico topic names. The original channel name is restored on extraction.

### The Decoders

The payload of an MCAP message is a sequence of bytes whose layout depends on the message encoding of its channel. Decoders turn each of these payloads into a plain nested Python `dict`, the common input expected by every [adapter](#the-adaptation-layer-mcapbridge-adapters), regardless of the original encoding.

* **[`MCAPMsgDecoderBase`][mosaicolabs.bridges.mcap.decoders.decoder_base.MCAPMsgDecoderBase]:** the abstract base class of every decoder. A subclass declares the channel message encoding it handles (`SUPPORTED_CHANNEL_ENCODING`), provides the `mcap` [`DecoderFactory`](https://mcap.dev/docs/python/mcap-apidoc/mcap.decoder) that deserializes the raw bytes into a native object, and converts that object into a `dict`. Before iteration starts, `register_schema()` is called once per channel for any schema-dependent setup; `decode()` is then called on every message.
* **[`DecoderRegistry`][mosaicolabs.bridges.mcap.decoders.DecoderRegistry]:** maps each message encoding to its decoder class, and is populated through the [`@register_decoder`][mosaicolabs.bridges.mcap.decoders.registry.register_decoder] decorator. `MCAPLoader` keeps one decoder instance per channel, since a decoder can hold schema-dependent state. Channels whose message encoding has no registered decoder are rejected as `Unresolved decoder`.

Supporting a new encoding only requires a new `MCAPMsgDecoderBase` subclass decorated with `@register_decoder`, with no change to the loader.

#### Protobuf Decoder (`MCAPProtobufMsgDecoder`)

[`MCAPProtobufMsgDecoder`][mosaicolabs.bridges.mcap.decoders.MCAPProtobufMsgDecoder] handles `protobuf` channels. In `register_schema()` it loads the `FileDescriptorSet` embedded in the channel's schema into a private `DescriptorPool`, so messages are decoded without any generated Python class. Each message is then converted through the protobuf JSON mapping, keeping `google.protobuf.Any` fields as their raw `type_url` and `value` bytes instead of unpacking them.

The JSON mapping does not always produce what the Arrow schema of the topic expects, so the decoder fixes the resulting `dict` through two classes:

* **[`Rule`][mosaicolabs.bridges.mcap.decoders.rule.Rule]:** pairs a predicate on a schema field (e.g. "is this field a `Timestamp`?") with a callback that fixes that field in the decoded `dict`.
* **[`ProtobufRulesMatcher`][mosaicolabs.bridges.mcap.decoders.protobuf.rules_matcher.ProtobufRulesMatcher]:** walks the message descriptor once per schema, recursing into nested messages, repeated fields and maps, and associates each field path with the rules it matches. During `decode()`, every rule is applied only to the paths it was matched against.

| Protobuf field | JSON mapping output | Fix applied |
| --- | --- | --- |
| `int64` family (`int64`, `uint64`, `sint64`, `fixed64`, `sfixed64`) | May be a string | Coerced to a Python `int` |
| `google.protobuf.Timestamp` | RFC 3339 string | Converted to a `{"seconds", "nanos"}` struct |
| `bytes` | Base64 string | Decoded back to raw `bytes` |
| `oneof` members, singular messages | Omitted when unset | Filled with `None` |

### The Orchestrator (`MCAPInjector`)

The **[`MCAPInjector`][mosaicolabs.bridges.mcap.MCAPInjector]** is the central command center of the MCAP Bridge module. It is designed to be the primary entry point for developers who want to embed MCAP ingestion directly into their Python applications or automation scripts.

The injector orchestrates the interaction between the **[`MCAPLoader`][mosaicolabs.bridges.mcap.loader.MCAPLoader]** (file access and decoding), the **[`MCAPBridge`][mosaicolabs.bridges.mcap.MCAPBridge]** (data adaptation), and the **[`MosaicoClient`][mosaicolabs.comm.MosaicoClient]** (network transmission). It handles the complex lifecycle of a data upload, including connection management, batching, and transaction safety, while providing real-time feedback through a visual CLI interface.

#### Core Workflow Execution: `run()`

1. **Handshake**: Establishes a connection to the Mosaico server and opens the MCAP file via `MCAPLoader`, which resolves the decoder and the adapter of every channel.
2. **Sequence Creation**: Requests the server to initialize a new data sequence based on the provided name and metadata.
3. **Adaptive Streaming**: Iterates through the MCAP messages. For each message, it identifies the correct adapter, translates the decoded dictionary into a Mosaico object, and pushes it into an optimized write buffer. Each topic is created on its first message, together with its `_mcap_` metadata.
4. **Transaction Finalization**: Once the file is exhausted, it flushes all remaining buffers and signals the server to commit the sequence.

#### Configuring the Ingestion

The behavior of the injector is entirely driven by the **[`MCAPInjectionConfig`][mosaicolabs.bridges.mcap.MCAPInjectionConfig]**. This configuration object ensures that the ingestion logic is decoupled from the user interface, allowing for consistent behavior whether triggered via the CLI or a complex script.

Compared to `ROSInjectionConfig`, it filters the input through `channels` instead of `topics`, and it has no `ros_distro`, `custom_msgs` or `registry` field, since no message definition has to be provided (see [No Message Definitions Required](#no-message-definitions-required)). Two more options are worth mentioning:

* **`serialization_formats`**: selects, per channel, the [`SerializationFormat`][mosaicolabs.enum.SerializationFormat] of the Unmodeled ontology created for it. Channels not listed use `SerializationFormat.Default`.
* **`dry_run`**: if `True`, reports which channels would be ingested (and with which adapter) and which would be rejected (and why), without connecting to the Mosaico server or writing any data.

#### Per-Topic Metadata (`topic_metadata`)

Besides the sequence-level `metadata` dict, `topic_metadata: Optional[Dict[str, dict]]` lets you attach metadata to individual topics by exact channel name (the same exact-name convention as `topics_on_error`, rather than the glob patterns used by `channels`). Entries for channels excluded by `channels` filtering are simply unused. See [Metadata: Reserved Keys & Custom Fields](#metadata-reserved-keys-custom-fields) below for how it merges with metadata the bridge computes automatically.

```python
config = MCAPInjectionConfig(
    ...,
    topic_metadata={
        "/imu": {"unit": "rad/s"},
    },
)
```

It can also be loaded from a JSON file, handy for the CLI's `--topic-metadata` flag, or to keep large mappings out of your script:

```json title="topic_metadata.json"
{
  "/imu": {"unit": "rad/s"},
  "/estimation/pose": {"algorithm": "ekf_v2", "offline": true}
}
```

```python
import json

config = MCAPInjectionConfig(
    ...,
    topic_metadata=json.loads(Path("topic_metadata.json").read_text()),
)
```

#### Updating an Existing Sequence (`update_if_exists`)

By default, ingesting into a `sequence_name` that already exists on the server raises an error. Setting **`update_if_exists=True`** switches this to an append/merge: the injector appends this file's topics to the existing sequence instead of creating a new one. This covers two distinct real-world scenarios with the same mechanism:

* **Multi-part recordings**: a single logical recording split across several MCAP files, all of which should land in one sequence.
* **Reprocessing / augmentation**: a derived MCAP file (e.g. offline estimation results computed from an already ingested recording, with timestamps aligned to the original) whose topics should be merged into the sequence that was already ingested from the original recording.

Since sequence metadata can't be changed after creation, the per-topic `source_file` metadata (see [Metadata: Reserved Keys & Custom Fields](#metadata-reserved-keys-custom-fields) below) is what keeps this traceable: inspecting a topic's own metadata tells you which MCAP file introduced it, even after several `update_if_exists=True` runs against the same sequence.

```python
config = MCAPInjectionConfig(
    file_path=Path("estimation_results.mcap"),
    sequence_name="on_track_experiment",  # already ingested from the original recording
    metadata={},  # ignored: the sequence already exists, its metadata is immutable
    update_if_exists=True,
    topic_metadata={
        "/estimation/pose": {"algorithm": "ekf_v2", "offline": True},
    },
)
```

If `sequence_name` doesn't exist yet, `update_if_exists=True` simply creates it, same as leaving it at the default `False`.

!!! warning "Resuming after a crash is not idempotent"
    If the process crashes midway through a file and you re-run the same command with `update_if_exists=True`, the injector has no memory of which topics it already fully ingested before the crash: that bookkeeping lives only in an in-memory cache scoped to the crashed process's session. Expect `topic_create` to be called again for those topics on resume, which the server is expected to reject as duplicates. There is currently no built-in dedup against the sequence's already existing topics before re-creating them, so a genuinely safe resume isn't supported yet. Plan for re-ingesting into a fresh sequence name if a run fails partway through, rather than relying on `update_if_exists` to pick up where it left off.

#### Practical Example: Programmatic Usage

```python
from pathlib import Path
from mosaicolabs import SerializationFormat, SessionLevelErrorPolicy, TopicLevelErrorPolicy
from mosaicolabs.bridges.mcap import MCAPInjector, MCAPInjectionConfig

def run_injection():
    # Define the Injection Configuration
    # This data class acts as the single source for the operation.
    config = MCAPInjectionConfig(
        # Input Data
        file_path=Path("data/session_01.mcap"),

        # Target Platform Metadata
        sequence_name="test_mcap_sequence",
        metadata={
            "driver_version": "v2.1",
            "weather": "sunny",
            "location": "test_track_A"
        },

        # Channel Filtering (supports glob patterns)
        # This will only upload channels starting with '/cam' or '/lidar'
        channels=["/cam*", "/lidar*"],

        # Serialization Format of the Unmodeled ontologies, per channel
        serialization_formats={
            "/lidar/points": SerializationFormat.Ragged,
        },

        # Per-Topic Metadata (exact channel name to dict; the reserved "_mcap_" key,
        # containing schema info and source_file, always overrides this on conflict)
        topic_metadata={
            "/cam/front": {"lens": "wide-angle", "calibrated": True},
        },

        # Update instead of Create
        # If "test_mcap_sequence" already exists (e.g. a previous file of a multi-part
        # recording, or a sequence to merge reprocessed results into), append to it
        # instead of raising an error.
        update_if_exists=False,

        # Execution Settings
        log_level="WARNING",  # Reduce verbosity for automated scripts

        # Session Level Error Handling
        on_error=SessionLevelErrorPolicy.Report, # Report the error and terminate the session

        # Topic Level Error Handling
        topics_on_error=TopicLevelErrorPolicy.Raise # Re-raise any exception
    )

    # Instantiate the Controller
    injector = MCAPInjector(config)

    # Execute
    # The run method handles connection, loading, and uploading automatically.
    # It raises exceptions for fatal errors, allowing you to wrap it in try/except blocks.
    try:
        injector.run()
        print("Injection job completed successfully.")
    except Exception as e:
        print(f"Injection job failed: {e}")

# Use as script or call the injection function in your code
if __name__ == "__main__":
    run_injection()
```

### The Adaptation Layer (`MCAPBridge` & Adapters)

This layer represents the semantic core of the module, translating decoded MCAP messages into the Mosaico Ontology.

* **[`MCAPAdapterBase`][mosaicolabs.bridges.mcap.adapter_base.MCAPAdapterBase]:** an abstract base class that establishes the contracts for converting MCAP messages into Mosaico Ontology types and back. An adapter is identified by the `schema_name` and `schema_encoding` pair it handles, and implements `from_dict()` (MCAP → Mosaico) and, optionally, `to_mcap()` (Mosaico → MCAP).
* **No built-in adapters:** unlike the ROS Bridge, which ships adapters for the standard ROS messages, the structure of the messages contained in an MCAP file cannot be known in advance. The only adapter provided by the library is the [Unmodeled Adapter](#extending-the-bridge-unmodeled-adapters), used whenever no custom adapter is registered for a schema.
* **[`MCAPBridge`][mosaicolabs.bridges.mcap.MCAPBridge]:** a central registry and dispatch mechanism that maps each `(schema_name, schema_encoding)` pair to its adapter class. Registration fails if the adapter's `schema_encoding` is not supported, or if an adapter is already registered for the same pair.

!!! note "MCAP log time and publish time"
    Every MCAP message carries two timestamps, both in nanoseconds:

    * **`log_time`**: the time at which the message was recorded, assigned by the recorder that wrote the file. MCAP orders and indexes messages by this value.
    * **`publish_time`**: the time at which the message was published, assigned by its publisher. When not available, it is set equal to `log_time`.

    Every Mosaico `Message` produced by the bridge is timestamped with the MCAP `log_time`. Mosaico orders and indexes records by `Message.timestamp_ns`, so all the topics of a sequence must share a common time origin (see [`Message` (The Envelope)](../ontology.md#message-the-envelope)). `log_time` meets this requirement, since a single recorder stamps every channel of the file on the same clock. `publish_time`, instead, comes from the clock of each publisher and may be missing, so it is not suitable for indexing.

    The publish time is not lost: it is passed to `from_dict()` alongside the decoded payload, so a custom adapter can store it in the ontology (the [example below](#extending-the-bridge-custom-adapters) uses it as the `Header` timestamp), while the Unmodeled Adapter keeps it in the payload as `publish_time_ns`. On extraction, `log_time` is restored from `Message.timestamp_ns`, and `publish_time` from the value returned by `to_mcap()`.

#### Extending the Bridge (Custom Adapters)

Users can map an MCAP schema onto a built-in or custom ontology by implementing a custom adapter and registering it. This is also the way to store an MCAP schema as your own [ontology](../ontology.md) (see [No Message Definitions Required](#no-message-definitions-required)).

1.  **Inherit from `MCAPAdapterBase`**: Define the `schema_name` and `schema_encoding` handled by the adapter (they must match the schema recorded in the MCAP file) and the target Mosaico Ontology type.
2.  **Implement `from_dict`**: Define the logic to convert the decoded message dictionary (and its publish time) into an instance of the target ontology object.
3.  **Implement `to_mcap`** (optional): Define the reverse conversion, returning the native message (for `protobuf`, an instance of the generated message class) and its publish time. It is only needed to [extract](#the-extractor-mcapsequenceextractor) the topic back to MCAP.
4.  **Register**: Decorate the class with [`@register_default_adapter`][mosaicolabs.bridges.mcap.register_default_adapter], and import its module before running the injector or the extractor.

The adapter below maps a `protobuf` message named `Mosaico.Imu` onto the built-in [`IMU`][mosaicolabs.models.sensors.IMU] ontology:

```python
from typing import Optional, Tuple, Type

from mosaicolabs import IMU, Header, Time, Vector3d
from mosaicolabs.bridges.mcap import MCAPAdapterBase, register_default_adapter

# Class generated by protoc from the .proto definition of the message
from my_project.generated.imu_pb2 import Imu as ImuProtobuf


@register_default_adapter
class MyImuProtobufAdapter(MCAPAdapterBase[IMU, ImuProtobuf]):
    schema_name = "Mosaico.Imu"
    schema_encoding = "protobuf"
    __mosaico_ontology_type__: Type[IMU] = IMU

    @classmethod
    def from_dict(cls, mcap_data: dict, publish_time_ns: int) -> IMU:

        mcap_acceleration = mcap_data["linear_acceleration"]
        mcap_angular_velocity = mcap_data["angular_velocity"]

        return IMU(
            header=Header(timestamp=Time.from_nanoseconds(publish_time_ns)),
            acceleration=Vector3d(
                x=mcap_acceleration["x"],
                y=mcap_acceleration["y"],
                z=mcap_acceleration["z"],
            ),
            angular_velocity=Vector3d(
                x=mcap_angular_velocity["x"],
                y=mcap_angular_velocity["y"],
                z=mcap_angular_velocity["z"],
            ),
        )

    @classmethod
    def to_mcap(cls, mosaico_data: IMU) -> Tuple[ImuProtobuf, Optional[int]]:
        out = ImuProtobuf()

        out.linear_acceleration.x = mosaico_data.acceleration.x
        out.linear_acceleration.y = mosaico_data.acceleration.y
        out.linear_acceleration.z = mosaico_data.acceleration.z
        out.angular_velocity.x = mosaico_data.angular_velocity.x
        out.angular_velocity.y = mosaico_data.angular_velocity.y
        out.angular_velocity.z = mosaico_data.angular_velocity.z

        publish_time_ns = None
        if mosaico_data.header and mosaico_data.header.timestamp:
            publish_time_ns = mosaico_data.header.timestamp.to_nanoseconds()

        return out, publish_time_ns
```

#### Extending the Bridge (Unmodeled Adapters)

Not every MCAP schema needs a hand-written adapter before it can be ingested. When the bridge encounters a channel whose `(schema_name, schema_encoding)` pair has no registered adapter, it doesn't reject it. Instead, it synthesizes an **[`UnmodeledAdapter`][mosaicolabs.bridges.mcap.adapters.unmodeled.UnmodeledAdapter]** for it at runtime, transparently and with no user intervention required, capable of translating that schema in both directions: MCAP to Mosaico, and back again from Mosaico to MCAP.

The schema definition embedded in the MCAP file is converted into an equivalent PyArrow schema, which is wrapped into a dynamically-generated [`Unmodeled`][mosaicolabs.models.core.unmodeled.Unmodeled] ontology class via [`resolve_ontology_class`][mosaicolabs.models.core.helpers.resolve_ontology_class]. Its ontology tag is the last component of the schema name (e.g. `foxglove.CompressedVideo` becomes `CompressedVideo`), and the MCAP publish time is stored in its payload as `publish_time_ns`. On extraction, the adapter rebuilds the native message class from the schema definition recorded at ingestion time (for `protobuf`, the generated message class) and fills it with the stored data.

See [Advanced: Ingesting Unmodeled Ontologies](../ontology.md#advanced-ingesting-unmodeled-ontologies) for the full mechanics of the `Unmodeled` ontology type, and [Class-Free Queries](../query.md#class-free-queries) for how to **query** this data on the server without ever needing to resolve a Python class for it.

#### Metadata: Reserved Keys & Custom Fields

!!! info "Internal behavior"
    This section documents implementation detail useful for interpreting or querying ingested metadata, not something you need to configure to use the bridge.

Sequence and topic metadata are populated from a mix of auto-computed and user-supplied sources. At the topic level, exactly **one** key is reserved because the bridge writes it itself: `_mcap_`, encapsulated by [`MCAPSchemaMetadata`][mosaicolabs.bridges.mcap.adapter_base.MCAPSchemaMetadata] so that the literal string `"_mcap_"` exists in a single place in the codebase. It holds everything needed to rebuild the original channel on extraction:

* `schema_name`, `schema_encoding` and `schema_def`: the original MCAP schema, with its definition stored as a string (base64 for `protobuf`).
* `channel_name` and `channel_encoding`: the original MCAP channel.
* `source_file`: the name of the MCAP file (`file_path.name`) that first created that topic. Written once per topic, at topic-creation time, regardless of `update_if_exists`.

A topic whose `_mcap_` block is missing one of the schema or channel fields, or holds a value of an unexpected type, is rejected on extraction as `Malformed metadata`.

`_mcap_` is fully reserved: `topic_metadata` ([Per-Topic Metadata](#per-topic-metadata-topic_metadata) above) is merged in *first*, then the bridge-computed `_mcap_` block is applied on top, so the bridge always wins that key regardless of what `topic_metadata` sets for it. Every other key is fully user-owned.

* **`metadata`** *(sequence-level)*: only applied at sequence-creation time. The Mosaico server does not support mutating a sequence's metadata after ingestion, so it's simply ignored when `update_if_exists=True` targets an already existing sequence.
* **`topic_metadata`**: merged underneath the auto-computed `_mcap_` block (see above).

!!! note "CLI-only sequence metadata"
    When using the `mosaicolabs.mcap_injector` CLI, an additional `mcap_injection` key (the MCAP file name) is automatically merged into `metadata` for traceability. This only happens in the CLI entry point, not when constructing `MCAPInjectionConfig` directly in Python.

!!! tip "Querying these keys"
    `_mcap_` and its nested fields (e.g. `_mcap_.schema_name`, `_mcap_.channel_name`) are ordinary metadata as far as the query engine is concerned: they're queryable through [`QuerySequence`][mosaicolabs.query.builders.QuerySequence] and [`QueryTopic`][mosaicolabs.query.builders.QueryTopic] `with_user_metadata()` exactly like any other metadata field, including [glob patterns for nested keys](../query.md#using-glob-pattern-for-metadata-keys) (e.g. `QueryTopic().with_user_metadata("_mcap_.schema_name", eq="foxglove.CompressedVideo")`). See the [Query Workflow](../query.md) guide for the full API.

#### CLI Usage

The module includes a command-line interface for quick ingestion tasks. The full list of options can be retrieved by running `mosaicolabs.mcap_injector -h`

```bash
# Basic Usage
mosaicolabs.mcap_injector ./data.mcap --name "Test_Run_01"

# Advanced Usage: Filtering channels and adding metadata
mosaicolabs.mcap_injector ./data.mcap \
  --name "Test_Run_01" \
  --channels "/camera/front/*" /gps/fix \
  --metadata ./metadata.json

# Advanced Usage: Per-topic metadata and appending to an existing sequence
# (e.g. a second file of a multi-part recording, or reprocessed results
# to merge into an already ingested sequence)
mosaicolabs.mcap_injector ./estimation_results.mcap \
  --name "Test_Run_01" \
  --topic-metadata '{"/estimation/pose": {"algorithm": "ekf_v2"}}' \
  --update-if-exists

# Dry Run: report accepted and rejected channels, without connecting to the server
mosaicolabs.mcap_injector ./data.mcap --name "Test_Run_01" --dry-run
```

### The Extractor (`MCAPSequenceExtractor`)

The **[`MCAPSequenceExtractor`][mosaicolabs.bridges.mcap.MCAPSequenceExtractor]** runs the ingestion pipeline in reverse: it reads a Mosaico sequence back out and writes it as an `.mcap` file, using the adapters' `to_native()` and the [`_mcap_` metadata][mosaicolabs.bridges.mcap.adapter_base.MCAPSchemaMetadata] recorded at ingestion time to recreate the original channels and schemas. Like the ROS Bridge's [`ROSSequenceExtractor`][mosaicolabs.bridges.ros.ROSSequenceExtractor], it is built on the bridge-agnostic `SequenceExtractor` base class.

!!! note "Same schema encoding as ingestion"
    Extraction returns each topic to the schema encoding it was ingested with: a channel ingested as `protobuf` is written back as `protobuf`. Converting a topic to a different schema encoding is not supported.

#### Core Workflow Execution: `run()`

1. **Prepare Output Path**: resolves `saving_path / sequence_name` and enforces the `overwrite` policy: raises `FileExistsError` if the path exists and `overwrite=False`, otherwise deletes it first. The file is then written as `saving_path / sequence_name / sequence_name.mcap`.
2. **Handshake**: connects to the Mosaico server and opens a [`MosaicoToMCAPLoader`][mosaicolabs.bridges.mcap.loader.MosaicoToMCAPLoader] for the requested sequence (optionally filtered by `topics` and a `start_timestamp_ns`/`end_timestamp_ns` window).
3. **Adaptive Streaming**: for each `(topic, message)` pair, resolves the topic's adapter (a custom adapter registered for the recorded schema, otherwise an Unmodeled Adapter), serializes the message via `to_native()`, and writes it on the topic's original channel with its original log and publish times. Each channel and schema is recreated once, from the `_mcap_` metadata. Topics that fail to be encoded or written are skipped (logged as an error) rather than aborting the whole extraction.

#### Configuring the Extraction

The behavior of the extractor is entirely driven by **[`MCAPExtractorConfig`][mosaicolabs.bridges.mcap.MCAPExtractorConfig]**, which only uses the options shared by every bridge extractor. Its main knobs:

* **`topics`**: glob-based include/exclude filtering (see [Configuring the Ingestion](#configuring-the-ingestion) above), applied to the Mosaico topic names of the sequence.
* **`start_timestamp_ns`** / **`end_timestamp_ns`**: an optional time window to extract, with an inclusive start and an exclusive end (`start <= t < end`). Out-of-range bounds are *clipped* to the sequence's own bounds (with a warning) rather than raising: an `end_timestamp_ns` beyond the end of the sequence extracts everything up to, and including, its last message.
* **`overwrite`**: if the resolved output path (`saving_path / sequence_name`) already exists, `overwrite=False` (default) raises `FileExistsError`; `overwrite=True` deletes and recreates it.

#### Dry Run (`dry_run`)

Setting **`dry_run=True`** (or `--dry-run` on the CLI) resolves the sequence's topics and prints a report (per topic, the resolved adapter and schema name, or the rejection reason, plus message counts) **without** opening a writer or touching the output path. The dry run also reports what *would* happen to that path (created, deleted and recreated, or a `FileExistsError`) without doing it.

```python
config = MCAPExtractorConfig(
    saving_path=Path("./exports"),
    sequence_name="on_track_experiment",
    dry_run=True,
)
MCAPSequenceExtractor(config).run()  # prints a report; nothing is written or deleted
```

```bash
python -m mosaicolabs.bridges.mcap.sequence_extractor on_track_experiment --mcap_path ./exports --dry-run
```

#### Practical Example: Programmatic Usage

```python
from pathlib import Path
from mosaicolabs.bridges.mcap import MCAPSequenceExtractor, MCAPExtractorConfig

config = MCAPExtractorConfig(
    saving_path=Path("./exports"),
    sequence_name="on_track_experiment",

    # Topic Filtering (supports glob patterns, applied to the Mosaico topic names)
    topics=["/cam*", "!/cam/debug*"],

    # Optional time-window clipping (nanoseconds); out-of-range bounds are clipped
    # to the sequence's own bounds rather than raising.
    start_timestamp_ns=None,
    end_timestamp_ns=None,

    overwrite=True,
)

extractor = MCAPSequenceExtractor(config)
extractor.run()  # writes ./exports/on_track_experiment/on_track_experiment.mcap
```

#### CLI Usage

The extractor can be run as a module. The full list of options can be retrieved by running `python -m mosaicolabs.bridges.mcap.sequence_extractor -h`.

```bash
# Basic Usage
python -m mosaicolabs.bridges.mcap.sequence_extractor on_track_experiment --mcap_path ./exports

# Advanced Usage: Filtering topics and overwriting a previous export
python -m mosaicolabs.bridges.mcap.sequence_extractor on_track_experiment \
  --mcap_path ./exports \
  --topics "/cam/*" "!/cam/debug*" \
  --overwrite
```

### No Message Definitions Required

Unlike the ROS Bridge, the MCAP Bridge has no type registry and no `custom_msgs` option. A ROS bag may lack the definition of some of its messages, which then have to be registered through the [`ROSTypeRegistry`][mosaicolabs.bridges.ros.ROSTypeRegistry] before they can be decoded. An MCAP file, instead, embeds the definition of every schema it uses: any channel with a supported encoding can be decoded and ingested as is, and later rebuilt from the definition stored in the topic metadata.

Defining new ontologies is still possible, and useful to simplify the query process: a [`Serializable`][mosaicolabs.models.core.Serializable] class gives an MCAP schema typed fields and the `.Q` query proxy, instead of the [class-free queries](../query.md#class-free-queries) used for Unmodeled ontologies. In this case, a [custom adapter](#extending-the-bridge-custom-adapters) mapping the MCAP schema onto the new ontology has to be implemented.

### Testing & Validation

The MCAP Bridge has been validated against the **[Labelbox robotics datasets](https://huggingface.co/datasets/Labelbox/robotics-datasets)** on Hugging Face, whose recordings use `protobuf` schemas, both Foxglove well-known schemas (e.g. `foxglove.CompressedVideo`, `foxglove.FrameTransforms`) and vendor-specific ones. The [MCAP Ingestion](https://docs.mosaico.dev/examples/MCAP/mcap_ingestion) and [Reconstruct MCAP](https://docs.mosaico.dev/examples/MCAP/reconstruct_mcap) examples ingest four of these recordings and write them back to MCAP.

#### Known Issues & Limitations

* **Schema encodings**: only `protobuf` schemas are supported (see [Supported Schema Encodings](#supported-schema-encodings)).
* **Summary section**: `MCAPLoader` reads channels, schemas and message counts from the summary section of the file, and refuses files written without one.
* **Message sequence counter**: the per-message MCAP `sequence` counter is not restored on extraction, and messages are written with `sequence` set to `0`.
* **`google.protobuf.Any`**: these fields are stored as their raw `type_url` and `value` bytes, without unpacking the packed message.
* **Integer map keys**: protobuf `map` fields can have [integer or string keys](https://protobuf.dev/programming-guides/proto3/#maps), but the protobuf JSON mapping (`MessageToDict`) turns every map key into a string, and the [Protobuf Decoder](#protobuf-decoder-mcapprotobufmsgdecoder) does not post-process map keys. For a map with integer keys, the decoded keys therefore do not match the integer key type of the PyArrow schema, and the ingestion of that channel fails.
* **Point cloud extraction**: reconstructing an MCAP file from a Mosaico sequence is slow for topics holding point clouds.


## Supported Schema Encodings

| Schema Encoding | Support | Notes |
| :--- | :--- | :--- |
| `protobuf` | Full | |
| `ros2msg`, `ros2idl`, `ros1msg` | Not supported | Use the [ROS Bridge](ros.md) instead. |

Channels whose encoding is not supported are rejected and listed in the run report, while the rest of the file is still processed. Support for further encodings will be added in future releases.
