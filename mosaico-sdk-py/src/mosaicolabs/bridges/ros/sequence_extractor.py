"""
ROSSequenceExtractor: extracts a Mosaico sequence and writes it as a ROS bag file.

Provides [`ROSSequenceExtractor`][mosaicolabs.bridges.ros.ROSSequenceExtractor] and the
[`ROSExtractorConfig`][mosaicolabs.bridges.ros.ROSExtractorConfig] dataclass, the ROS
specialization of
[`SequenceExtractor`][mosaicolabs.bridges.sequence_extractor.SequenceExtractor]. The base
class drives the pipeline; this module supplies the ROS half of it:

1. Connect to the Mosaico server (base class).
2. Stream every message in the requested sequence (optionally filtered by topic or
   time window) through
   [`MosaicoToROSLoader`][mosaicolabs.bridges.ros.MosaicoToROSLoader].
3. Convert each message to its ROS equivalent via the registered
   [`ROSBridge`][mosaicolabs.bridges.ros.ROSBridge] adapters.
4. Write the result into a new ROS 1 (`.bag`) or ROS 2 (`.mcap` / `.db3`) bag file.

The module also exposes `ros_sequence_extractor()`, an `argparse` entry point that can be
run with `python -m mosaicolabs.bridges.ros.sequence_extractor`.
"""

import argparse
from dataclasses import dataclass
from pathlib import Path
from typing import Any, Optional, Type, Union

from rosbags.interfaces import Connection
from rosbags.rosbag1 import Writer as Ros1Writer
from rosbags.rosbag2 import StoragePlugin, Writer as Ros2Writer
from rosbags.typesys import Stores, get_typestore
from rosbags.typesys.store import Typestore

from mosaicolabs import Message, MosaicoClient
from mosaicolabs.bridges.ros.adapter_base import ROSAdapterBase
from mosaicolabs.bridges.ros.loader import MosaicoToROSLoader
from mosaicolabs.bridges.ros.qos import get_qos_for_topic
from mosaicolabs.bridges.ros.registry import ROSTypeRegistry
from mosaicolabs.bridges.ui import ProgressManager
from mosaicolabs.logging_config import get_logger

from ..base_sequence_extractor import (
    ExtractorConfig,
    SequenceExtractor,
    _add_common_arguments,
    _common_config_kwargs,
)
from ..loader_base import MosaicoLoader

# Set the hierarchical logger
logger = get_logger(__name__)


# --- Configuration ---
@dataclass
class ROSExtractorConfig(ExtractorConfig):
    """
    Configuration for
    [`ROSSequenceExtractor`][mosaicolabs.bridges.ros.ROSSequenceExtractor].

    Extends
    [`ExtractorConfig`][mosaicolabs.bridges.sequence_extractor.ExtractorConfig] with the
    ROS-specific fields: the target distribution, the ROS 2 storage plugin, and the custom
    message definitions needed to encode back into ROS messages.
    """

    ros_distro: Optional[Stores] = None
    """
    The specific ROS distribution to use for message parsing (e.g., Stores.ROS2_HUMBLE). If None, defaults to Empty/Auto.

    See [`rosbags.typesys.Stores`](https://ternaris.gitlab.io/rosbags/topics/typesys.html#type-stores).
    """

    storage_plugin: StoragePlugin = StoragePlugin.MCAP
    """
    Storage plugin to use. Available: StoragePlugin.SQLITE3 or StoragePlugin.MCAP
    """

    custom_msgs: Optional[list[tuple[str, Path, Optional[Stores]]]] = None
    """
    A list of tuples (package_name, path, store) to register custom .msg definitions before loading.

    For example, for "my_robot_msgs/msg/Location" pass:

    package_name = "my_robot_msgs"; path = path/to/Location.msg; store = Stores.ROS2_HUMBLE (e.g.) or None

    See [`rosbags.typesys.Stores`](https://ternaris.gitlab.io/rosbags/topics/typesys.html#type-stores).

    Registered into `registry` (or a fresh, private `ROSTypeRegistry` if `registry` is
    `None`) before the loader's `Typestore` is built. Needed when encoding an ontology
    value back to a ROS message whose `msgdef` isn't recoverable from the topic's own
    `_ros_` metadata (e.g. the sequence wasn't ingested from a ROS bag in the first place).
    """

    registry: Optional[ROSTypeRegistry] = None
    """
    The `ROSTypeRegistry` instance to register `custom_msgs` into and to pull existing
    definitions from. If `None` (default), a fresh, private instance is created for this
    extractor alone, so its custom types can never leak into another injector/extractor
    run in the same process. Pass the *same* `ROSTypeRegistry` instance across multiple
    configs to deliberately share a centrally pre-registered set of definitions between them.
    """


# --- Main Extractor Class ---


class ROSSequenceExtractor(SequenceExtractor):
    """
    Extracts a Mosaico sequence into a ROS 1 or ROS 2 bag file.

    The ROS specialization of
    [`SequenceExtractor`][mosaicolabs.bridges.sequence_extractor.SequenceExtractor], which
    owns the pipeline itself (output path, connection, streaming, progress and dry-run
    reporting). This class supplies the three ROS-specific hooks:

    * `_open_mosaicoloader`: opens a
      [`MosaicoToROSLoader`][mosaicolabs.bridges.ros.MosaicoToROSLoader] over the local
      `typestore`.
    * `_open_writer`: opens a `rosbags` ROS 1 or ROS 2 writer, choosing the bag format from
      `cfg.ros_distro` and `cfg.storage_plugin`.
    * `_process_message`: encodes each message with its
      [`ROSAdapterBase`][mosaicolabs.bridges.ros.ROSAdapterBase] and writes it on the
      topic's connection.

    On construction it also builds the `Typestore` for the configured distribution and
    registers `cfg.custom_msgs` into it, so custom message types are available before the
    first message is encoded.

    A topic whose `to_ros()` call or bag write fails is logged at `error` level, marked in
    the progress UI, and added to `ignored_topics` so its remaining messages are skipped
    without retrying. Extraction of the other topics continues.

    Attributes:
        typestore (Typestore): The ROS type registry for `cfg.ros_distro`, including the
            definitions loaded from `cfg.custom_msgs`.
        accepted_connections (dict[str, Connection]): Per-topic bag connections, opened
            lazily on a topic's first message and reused for the rest of the run.
    """

    cfg: ROSExtractorConfig
    """The ROS configuration this extractor was built with, narrowing the base class'
    `ExtractorConfig` attribute."""

    def __init__(self, config: ROSExtractorConfig):
        """
        Builds the ROS typestore and registers the configured custom message types.

        Args:
            config (ROSExtractorConfig): The ROS extraction configuration.
        """

        # Shared state, console and SDK logging are set up by the base class
        super().__init__(config)

        self.typestore: Typestore = get_typestore(self.cfg.ros_distro or Stores.EMPTY)

        # Bag connections are a ROS concept, so they live here rather than on the base
        # class. Opened lazily per topic in `_process_message`.
        self.accepted_connections: dict[str, Connection] = {}

        # Own a private registry by default, so this extractor's custom types can never
        # leak into another injector/extractor run in the same process. Pass the same
        # `ROSTypeRegistry` instance via `cfg.registry` to deliberately share definitions
        # across multiple runs (e.g. a centralized setup routine).
        self._registry: ROSTypeRegistry = self.cfg.registry or ROSTypeRegistry()

        # Register custom ROS messages to the local typestore
        self._typestore_custom_msgtypes()

    def _typestore_custom_msgtypes(self):
        """
        Registers any custom ROS message definitions provided in `cfg.custom_msgs`
        into `self._registry`, then pulls every definition currently registered there
        (including ones registered elsewhere on a *shared* `cfg.registry` instance) into
        the local typestore. Safe to always run: `self._registry` is either private to
        this extractor, or an instance the caller explicitly chose to share.
        """
        if self.cfg.custom_msgs:
            logger.info("Registering custom message definitions...")
            for package, path, store in self.cfg.custom_msgs:
                try:
                    self._registry.register_directory(
                        package_name=package, dir_path=path, store=store
                    )
                    logger.debug(f"Registered package '{package}' from '{path}'")
                except Exception as e:
                    logger.error(f"Failed to register custom msgs at '{path}': '{e}'")

        self._register_definitions()

    def _register_definitions(self):
        """
        Compiles every definition held by `self._registry` for the configured distribution
        into `self.typestore`.
        """
        from rosbags.typesys import get_types_from_msg

        custom_types = self._registry.get_types(self.cfg.ros_distro)
        if not custom_types:
            return

        logger.info(
            f"Registering {list(custom_types.keys())} definitions to typestore..."
        )
        for msg_type, msg_def in custom_types.items():
            try:
                add_types = get_types_from_msg(msg_def, msg_type)
                self.typestore.register(add_types)
            except Exception as e:
                logger.warning(f"Failed to register type '{msg_type}': '{e}'")

    def _open_mosaicoloader(self, mclient: MosaicoClient) -> MosaicoLoader:
        """
        Opens a fresh loader for the configured sequence.

        Builds a [`MosaicoToROSLoader`][mosaicolabs.bridges.ros.MosaicoToROSLoader] bound
        to the local `typestore`, so that the custom types registered at construction are
        visible when the loader resolves each topic's adapter.

        Args:
            mclient (MosaicoClient): The open connection the loader should read through.

        Returns:
            MosaicoLoader: A new loader for the configured sequence.
        """

        return MosaicoToROSLoader(
            mclient,
            self.typestore,
            self.cfg.sequence_name,
            self.cfg.topics,
            self.cfg.start_timestamp_ns,
            self.cfg.end_timestamp_ns,
        )

    def _open_writer(self, path: Path) -> Union[Ros1Writer, Ros2Writer]:
        """
        Opens the bag writer for the given path.

        Dispatches on `cfg.ros_distro`:

        1. For **ROS 1** (`Stores.ROS1_NOETIC`): creates the output directory and opens a
           `rosbags.rosbag1.Writer` targeting `<path>/<path.name>.bag`, since a ROS 1 bag
           is a single file rather than a directory.
        2. For **ROS 2**: opens a `rosbags.rosbag2.Writer` on `path` itself with the
           requested `storage_plugin`; the writer creates the directory. The bag format
           version is inferred from the distro (version 9 for Jazzy / Kilted / LATEST,
           version 8 for all others).

        Args:
            path (Path): The output directory for the bag file, already validated and
                cleared by `_prepare_output_path`.

        Returns:
            Union[Ros1Writer, Ros2Writer]: The open bag writer, usable as a context manager.
        """
        bagwriter = None

        # Importing correct writer
        if self.cfg.ros_distro is Stores.ROS1_NOETIC:
            path.mkdir(parents=True, exist_ok=True)
            full_path = path / (path.name + ".bag")
            bagwriter = Ros1Writer(full_path)

        else:
            # Deducing rosbag2 version from ROS_DISTRO
            if (
                self.cfg.ros_distro is Stores.ROS2_JAZZY
                or self.cfg.ros_distro is Stores.ROS2_KILTED
                or self.cfg.ros_distro is Stores.LATEST
            ):
                bagversion = 9
            else:
                bagversion = 8

            bagwriter = Ros2Writer(
                path, storage_plugin=self.cfg.storage_plugin, version=bagversion
            )

        return bagwriter

    def _process_message(
        self,
        writer: Any,
        ms_loader: MosaicoLoader,
        t_name: str,
        ms_msg: Message,
        ui: ProgressManager,
    ):
        """
        Encodes a single Mosaico message and writes it into the bag.

        Steps:

        1. **Skip**: drops the message if `t_name` was already ignored by an earlier failure.
        2. **Resolve Adapter**: locates the
           [`ROSAdapterBase`][mosaicolabs.bridges.ros.ROSAdapterBase] the loader resolved
           for this topic.
        3. **Translate**: encodes the payload into a native ROS message, using the ROS
           message type recorded in the topic's `_ros_` metadata.
        4. **Resolve Connection**: reuses, or lazily opens, the writer connection for this
           topic, attaching the QoS profile on ROS 2.
        5. **Write**: serializes the message (ROS 1 or CDR) and writes it at the Mosaico
           recording timestamp.

        Encoding and write failures never propagate: the topic is added to
        `ignored_topics` and reported in the progress UI, so the rest of the extraction
        continues.

        Args:
            writer (Any): The `Ros1Writer` or `Ros2Writer` returned by `_open_writer`.
            ms_loader (MosaicoLoader): The loader `run` is streaming from, used to resolve
                the topic's adapter and ROS message type.
            t_name (str): The topic the message belongs to.
            ms_msg (Message): The Mosaico message to encode and write.
            ui (ProgressManager): The live progress reporter for this run.

        Raises:
            ValueError: If `cfg.ros_distro` is not a supported ROS 1 or ROS 2 distribution.
        """

        if t_name in self.ignored_topics:
            ui.advance_global()
            return

        # --- Resolve Adapter Check ---
        # For each Mosaico type Message find its adapter
        adapter = ms_loader.resolve_adapter(t_name)

        if adapter is None:
            # Should not happen: MosaicoToROSLoader rejects topics it cannot adapt, so
            # they never reach the stream. Still advance the global bar, otherwise it
            # would never reach the message count computed by ProgressManager.setup().
            ui.advance_global()
            return

        # --- Translate Check ---
        ros_msg_type = ms_loader.resolve_native_msg_type(t_name)

        ros_msg = self._encode_ros_message(adapter, ms_msg, ros_msg_type, t_name, ui)
        if ros_msg is None:
            return

        # --- Resolve Check ---
        ros_msgtype = ros_msg.__msgtype__
        ros_recording_timestamp_ns = ms_msg.timestamp_ns

        # --- Resolve Connection check ---
        if t_name not in self.accepted_connections:  # New connection available
            if self.cfg.ros_distro is Stores.ROS1_NOETIC and isinstance(
                writer, Ros1Writer
            ):
                new_connection = writer.add_connection(
                    t_name,
                    ros_msgtype,
                    typestore=self.typestore,
                )
            elif self.cfg.ros_distro in [
                Stores.LATEST,
                Stores.ROS2_DASHING,
                Stores.ROS2_ELOQUENT,
                Stores.ROS2_FOXY,
                Stores.ROS2_GALACTIC,
                Stores.ROS2_HUMBLE,
                Stores.ROS2_IRON,
                Stores.ROS2_JAZZY,
                Stores.ROS2_KILTED,
            ] and isinstance(writer, Ros2Writer):
                new_connection = writer.add_connection(
                    t_name,
                    ros_msgtype,
                    typestore=self.typestore,
                    offered_qos_profiles=get_qos_for_topic(t_name),
                )
            else:
                raise ValueError(f"Unsupported ros distro: {self.cfg.ros_distro}")

            self.accepted_connections.update({t_name: new_connection})

        connection = self.accepted_connections[t_name]

        # --- Write check ---
        try:
            if self.cfg.ros_distro is Stores.ROS1_NOETIC:  # ROS1
                writer.write(
                    connection,
                    ros_recording_timestamp_ns,
                    self.typestore.serialize_ros1(ros_msg, ros_msgtype),
                )
            else:  # ROS2
                writer.write(
                    connection,
                    ros_recording_timestamp_ns,
                    self.typestore.serialize_cdr(ros_msg, ros_msgtype),
                )
        except Exception as e:
            return self._handle_encoding_error(
                t_name,
                ui,
                f"Could not write topic '{t_name}' to bag. Skipping the topic. Reason: {e}",
                f"Failed writing to bag because: {e}",
                style="yellow",
            )

        ui.advance_all(t_name)

    def _encode_ros_message(
        self,
        adapter: Type[ROSAdapterBase],
        ms_msg: Message,
        ros_msg_type: Optional[str],
        t_name: str,
        ui: ProgressManager,
    ):
        """
        Encodes one Mosaico message into its native ROS message, or drops the topic.
        """
        try:
            return adapter.to_native(
                ms_msg, typestore=self.typestore, ros_msg_type=ros_msg_type
            )
        except (TypeError, NotImplementedError) as e:
            return self._handle_encoding_error(
                t_name,
                ui,
                f"Could not encode to ros '{ms_msg.ontology_tag()}' type. "
                f"Skipping the topic associated to this message. Reason: {e}",
                "Failed encoding",
            )
        except KeyError as e:
            return self._handle_encoding_error(
                t_name,
                ui,
                f"Schema mismatch or partially adapted message type for "
                f"'{ms_msg.ontology_tag()}'. Skipping the topic associated to this "
                f"message. Reason: {e}",
                "Schema mismatch",
            )
        except Exception as e:
            return self._handle_encoding_error(
                t_name,
                ui,
                f"Unexpected error while encoding '{ms_msg.ontology_tag()}' type. "
                f"Skipping the topic associated to this message. Reason: {e}",
                "Error occurred",
            )


# --- CLI Entry Point ---


def ros_sequence_extractor():
    """
    Command-line entrypoint.
    Parses arguments, sets up configuration, and initiates the sequence extractor
    """

    parser = argparse.ArgumentParser(
        description="Extracts sequences from Mosaico and encodes them as rosbags"
    )

    _add_common_arguments(
        parser,
        output_flag="--rosbag_path",
        output_help="Path where to save the rosbag file",
    )

    # ROS arguments
    parser.add_argument(
        "--ros_distro",
        default="ROS2_JAZZY",
        choices=[s.name for s in Stores],
        help="Target ROS distribution for messages. If not set defaults to ROS2_JAZZY",
    )
    parser.add_argument(
        "--storage_plugin",
        default="MCAP",
        choices=[sp.name for sp in StoragePlugin],
        help="Storage plugin to save rosbag. If not set defaults to MCAP",
    )

    args = parser.parse_args()

    selected_distro = Stores.__members__[args.ros_distro]
    selected_storage_plugin = StoragePlugin.__members__[args.storage_plugin]

    configs = ROSExtractorConfig(
        **_common_config_kwargs(args),
        ros_distro=selected_distro,
        storage_plugin=selected_storage_plugin,
        # custom_msgs=,
    )

    # --- Execution ---
    extractor = ROSSequenceExtractor(configs)
    extractor.run()


if __name__ == "__main__":
    ros_sequence_extractor()
