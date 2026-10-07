"""
Shared configuration for the bridge injectors.

This module defines `InjectionConfigBase`, the data class holding the settings that every
bridge injector needs regardless of the source file format: the target sequence, the
Mosaico server connection, the session- and topic-level error policies, sequence and
per-topic metadata, logging, and dry-run mode.

Each bridge extends it with the options specific to its source format:

- `MCAPInjectionConfig` (`mosaicolabs.bridges.mcap`): MCAP files, adds `channels` filtering.
- `ROSInjectionConfig` (`mosaicolabs.bridges.ros`): ROS 1/2 bag files, adds `topics`
  filtering, `ros_distro`, custom message definitions and adapter overrides.

Typical usage (through a subclass):
    config = MCAPInjectionConfig(file_path=Path("data.mcap"), sequence_name="run_01")
    injector = MCAPInjector(config)
    injector.run()
"""

from dataclasses import dataclass, field
from enum import Enum
from pathlib import Path
from typing import Dict, Optional, Union

from mosaicolabs.enum import (
    SerializationFormat,
    SessionLevelErrorPolicy,
    TopicLevelErrorPolicy,
)

_DEFAULT_TOPIC_ON_ERROR = TopicLevelErrorPolicy.Raise
_DEFAULT_SESSION_ON_ERROR = SessionLevelErrorPolicy.Report


class InjectionStatus(Enum):
    """
    Outcome of an injector's `run()`.

    Errors are not a status: `run()` logs and re-raises them, so callers detect a failure
    through the exception.
    """

    COMPLETED = "completed"
    """The file was injected into the Mosaico server."""

    CANCELLED = "cancelled"
    """The user interrupted the injection (`KeyboardInterrupt`, e.g. Ctrl-C) before it
    completed."""

    DRY_RUN = "dry_run"
    """`dry_run` was set: the file was only analysed and nothing was written to the server."""


# --- Configuration ---
@dataclass
class InjectionConfigBase:
    """
    The configuration settings shared by every bridge injector.

    This data class holds the injection settings that do not depend on the source file
    format, decoupling the orchestration logic from CLI arguments or configuration files.
    It is not meant to be instantiated directly: each bridge extends it with its own
    format-specific options (e.g. topic/channel filtering), see `MCAPInjectionConfig`
    and `ROSInjectionConfig`.

    Attributes:
        file_path (Path): Absolute or relative path to the input file.
        sequence_name (str): The name of the sequence to create on the Mosaico server
            (or to update, see `update_if_exists`).
        metadata (dict): User-defined metadata to attach to the sequence (e.g., driver, weather, location).
            Ignored when an existing sequence is updated.
        topic_metadata (Optional[Dict[str, dict]]): Mapping of exact topic name to metadata to
            attach to that topic, alongside the metadata the bridge computes from the message
            schema and the source file. Default: None
        update_if_exists (bool): If `True`, append this file's topics to an existing sequence with
            the same name instead of raising an error. Default: False.
        host (str): Hostname or IP of the Mosaico server. Defaults to "localhost".
        port (int): Port of the Mosaico server. Defaults to 6726.
        on_error (SessionLevelErrorPolicy): Behavior when an ingestion error occurs (Delete the partial sequence or Report the error).
            Default: [`SessionLevelErrorPolicy.Report`][mosaicolabs.enum.SessionLevelErrorPolicy.Report]
        topics_on_error (Union[TopicLevelErrorPolicy, Dict[str, TopicLevelErrorPolicy]]): Behavior when a topic write fails.
            Default: [`TopicLevelErrorPolicy.Raise`][mosaicolabs.enum.TopicLevelErrorPolicy.Raise]
            Set to a [`TopicLevelErrorPolicy`][mosaicolabs.enum.TopicLevelErrorPolicy] to apply the same policy to all topics.
            Set to a `Dict[str, TopicLevelErrorPolicy]` to apply different policies to different (subset of) topics.
        serialization_formats (Optional[Dict[str, SerializationFormat]]): Mapping to the
            `SerializationFormat` used when synthesizing an `Unmodeled` ontology for topics that
            have no registered Mosaico adapter. What the keys identify depends on the bridge
            (see the subclass). Default: None
        log_level (str): Logging verbosity level ("DEBUG", "INFO", "WARNING", "ERROR", "CRITICAL").
            Default: "INFO"
        mosaico_api_key (Optional[str]): The API key for authentication on the mosaico server.
            If provided it must have the `write` permission.
            Default: None
        tls_cert_path (Optional[str]): Path to the TLS certificate file for secure connection on the mosaico server.
            Default: None
        enable_tls (bool): Enable the TLS communication protocol. Defaults to False.
        dry_run (bool): If `True`, resolves and reports which topics would be ingested without
            connecting to the Mosaico server or writing any data. Default: False.
    """

    file_path: Path
    """
    The path to the file to ingest.
    """

    sequence_name: str
    """
    The name of the sequence to create, or to update when `update_if_exists` is `True`
    and a sequence with this name already exists.
    """

    metadata: dict = field(default_factory=dict)
    """
    Metadata to associate with the sequence.
    """

    topic_metadata: Optional[Dict[str, dict]] = None
    """
    A mapping of exact topic name metadata to associate with that topic.

    It is merged with the metadata the bridge computes from the message schema and the source
    file (see `_process_message`), which lives under a single bridge-reserved key (e.g. `_mcap_`
    or `_ros_`). That reserved key always wins on conflict; every other key is user-owned.

    Only applied to topics that end up being ingested; entries for topics excluded by the
    bridge's topic filtering are simply unused (see `dry_run` to detect them). Default: None.
    """

    update_if_exists: bool = False
    """
    Controls what happens when a sequence named `sequence_name` already exists on the server.

    If `True`, the injector appends this file's topics to the existing sequence instead of
    creating a new one. Use this both when a recording is split across multiple files that
    should all land in the same sequence, and when re-ingesting a derived/reprocessed file
    (e.g. offline estimation results) whose topics should be merged into a sequence that was
    already ingested from the original recording.

    If `False` (default), the injector creates a new sequence and raises an error if a sequence
    with the same name already exists.

    Each topic's metadata records the source file it was ingested from (see `schema_metadata`
    handling in `_process_message`), so which file contributed which topics remains traceable
    even after multiple updates to the same sequence.

    Caveat: existence is checked and then acted upon in two separate steps (not atomically),
    so running concurrent injections against the same `sequence_name` can race. Avoid
    concurrent ingestion into the same sequence name.

    Caveat: resuming after a crash is NOT idempotent. `session_writer.get_topic_writer()`
    (see `_process_message`) only consults an in-memory cache scoped to the current process's
    session (`_BaseSessionWriter._topic_writers`); it has no knowledge of topics created by a
    previous, crashed run. So re-running the same file with `update_if_exists=True` after a
    crash will call `topic_create` again for topics that were already fully ingested before
    the crash, which the server is expected to reject as duplicates (behavior not covered by
    SDK-level tests as of this writing). There is currently no dedup against the topics already
    present in the target sequence (available server-side via `MosaicoClient.sequence_handler(
    sequence_name).topics`, the same mechanism `MosaicoLoader` already uses) before calling
    `topic_create`. A safe resume would need to check that list first and skip topics already
    present, rather than only checking the local per-process cache.
    """

    host: str = "localhost"
    """
    The hostname of the Mosaico server.
    """

    port: int = 6726
    """
    The port of the Mosaico server.
    """

    on_error: SessionLevelErrorPolicy = _DEFAULT_SESSION_ON_ERROR
    """
    The `SequenceWriter` `on_error` behavior when a sequence write fails (Report vs Delete).
    Default is `SessionLevelErrorPolicy.Report`.
    """

    topics_on_error: Union[TopicLevelErrorPolicy, Dict[str, TopicLevelErrorPolicy]] = (
        _DEFAULT_TOPIC_ON_ERROR
    )
    """
    The TopicWriter `on_error` behavior ([`TopicLevelErrorPolicy`][mosaicolabs.enum.TopicLevelErrorPolicy])
    when a topic write fails. Default is `TopicLevelErrorPolicy.Raise` for all topics.
    Set to a `TopicLevelErrorPolicy` to apply the same policy to all topics.
    Set to a `Dict[str, TopicLevelErrorPolicy]` to apply different policies to different topics:
    keys are exact topic names (matched like `topic_metadata`), and topics missing from the
    mapping fall back to `TopicLevelErrorPolicy.Raise`.
    """

    serialization_formats: Optional[Dict[str, SerializationFormat]] = None
    """A mapping to the [`SerializationFormat`][mosaicolabs.enum.SerializationFormat] used when
    synthesizing an `Unmodeled` ontology for topics that have no registered Mosaico adapter.

    What the keys identify depends on the bridge:

    - MCAP: the original channel name (e.g. "camera/pointcloud").
    - ROS: the message type (e.g. "sensor_msgs/msg/PointCloud2").

    Only applies to non-adapted (unmodeled) topics. Topics whose key is not present in this
    mapping default to `SerializationFormat.Default`.
    """

    log_level: str = "INFO"
    """The logging verbosity level ("DEBUG", "INFO", "WARNING", "ERROR", "CRITICAL")."""

    mosaico_api_key: Optional[str] = None
    """
    The API key for authentication on the mosaico server. Defaults to None.

    If provided it must have the `write` permission.
    """

    tls_cert_path: Optional[str] = None
    """Path to the TLS certificate file for secure connection on the mosaico server. Defaults to None."""

    enable_tls: bool = False
    """Enable the TLS communication protocol. Defaults to False"""

    dry_run: bool = False
    """
    If `True`, resolves and reports which topics would be ingested (and with which adapter),
    which topics would be rejected (and why), and which `topic_metadata` entries would be
    unused, without connecting to the Mosaico server or writing any data. Default: False.
    """
