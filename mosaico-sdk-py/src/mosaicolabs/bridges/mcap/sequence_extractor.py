"""
MCAPSequenceExtractor — extracts a Mosaico sequence and writes it as an MCAP file.

Provides [`MCAPSequenceExtractor`][mosaicolabs.bridges.mcap.MCAPSequenceExtractor] and the
[`MCAPExtractorConfig`][mosaicolabs.bridges.mcap.MCAPExtractorConfig] dataclass, the MCAP
specialization of
[`SequenceExtractor`][mosaicolabs.bridges.base_sequence_extractor.SequenceExtractor]. The base
class drives the pipeline; this module supplies the MCAP half of it:

1. Connect to the Mosaico server (base class).
2. Stream every message in the requested sequence (optionally filtered by topic or
   time window) through
   [`MosaicoToMCAPLoader`][mosaicolabs.bridges.mcap.loader.MosaicoToMCAPLoader].
3. Convert each message to its native MCAP payload via the registered
   [`MCAPAdapterBase`][mosaicolabs.bridges.mcap.adapter_base.MCAPAdapterBase] adapters.
4. Write the result into a new ``.mcap`` file.

The module also exposes `mcap_sequence_extractor()`, an `argparse` entry point that can be
run with `python -m mosaicolabs.bridges.mcap.sequence_extractor`.
"""

import argparse
from dataclasses import dataclass
from pathlib import Path
from typing import Any, Optional, Type

from mosaicolabs import Message, MosaicoClient
from mosaicolabs.bridges.mcap.loader import MosaicoToMCAPLoader
from mosaicolabs.bridges.ui import ProgressManager
from mosaicolabs.logging_config import get_logger

from ..base_sequence_extractor import (
    ExtractorConfig,
    SequenceExtractor,
    _add_common_arguments,
    _common_config_kwargs,
)
from ..loader_base import MosaicoLoader
from .adapter_base import MCAPAdapterBase, McapReturnType, MCAPSchemaMetadata
from .mcap_file import MCAPFileWriter

# Set the hierarchical logger
logger = get_logger(__name__)


# --- Configuration ---
@dataclass
class MCAPExtractorConfig(ExtractorConfig):
    """
    Configuration for
    [`MCAPSequenceExtractor`][mosaicolabs.bridges.mcap.MCAPSequenceExtractor].
    """


# --- Main Extractor Class ---


class MCAPSequenceExtractor(SequenceExtractor):
    """
    Orchestrates the extraction of a Mosaico sequence into an MCAP file.

    The MCAP specialization of
    [`SequenceExtractor`][mosaicolabs.bridges.base_sequence_extractor.SequenceExtractor], which
    owns the pipeline itself (output path, connection, streaming, progress and dry-run
    reporting). This class supplies the three MCAP-specific hooks:

    * `_open_mosaicoloader`: opens a
      [`MosaicoToMCAPLoader`][mosaicolabs.bridges.mcap.loader.MosaicoToMCAPLoader] for the
      configured sequence.
    * `_open_writer`: opens an
      [`MCAPFileWriter`][mosaicolabs.bridges.mcap.mcap_file.MCAPFileWriter] for the
      ``.mcap`` file.
    * `_process_message`: encodes each message with its
      [`MCAPAdapterBase`][mosaicolabs.bridges.mcap.adapter_base.MCAPAdapterBase], resolves its channel
      from the topic's recorded ``_mcap_`` metadata, and writes it.
    """

    def __init__(self, config: MCAPExtractorConfig):

        # Calling init from subclass
        super().__init__(config)

    def _open_mosaicoloader(self, mclient: MosaicoClient) -> MosaicoLoader:
        """
        Opens a fresh loader for the configured sequence.

        Args:
            mclient (MosaicoClient): The open connection the loader should read through.

        Returns:
            MosaicoLoader: A new loader for the configured sequence.
        """

        return MosaicoToMCAPLoader(
            mclient,
            self.cfg.sequence_name,
            self.cfg.topics,
            self.cfg.start_timestamp_ns,
            self.cfg.end_timestamp_ns,
        )

    def _open_writer(self, path: Path) -> MCAPFileWriter:
        """
        Creates the output directory and opens the mcap writer for the ``.mcap`` file inside it.

        Args:
            path (Path): The output directory, already validated and cleared by
                `_prepare_output_path()`. The actual file is written to
                `<path>/<path.name>.mcap`.

        Returns:
            MCAPFileWriter: A wrapper to the mcap writer itself.
        """

        return MCAPFileWriter(path)

    def _process_message(
        self,
        writer: Any,
        ms_loader: MosaicoLoader,
        t_name: str,
        ms_msg: Message,
        ui: ProgressManager,
    ):
        """
        Encodes a single Mosaico message and writes it into the mcap file.

        Steps:
        1. **Skip**: drops the message if `t_name` was already ignored by an earlier failure.
        2. **Resolve Adapter**: locates the `MCAPAdapterBase` the loader resolved for this topic.
        3. **Translate**: encodes the payload into its native MCAP bytes via `to_native()`.
        4. **Resolve Channel**: reads the topic's recorded `_mcap_` metadata
           (`MCAPSchemaMetadata`) to recover its original channel/schema, and registers (or
           reuses) the writer's channel id for it.
        5. **Write**: writes the encoded message to that channel at the Mosaico recording
           timestamp.

        Encoding and write failures never propagate: the topic is added to `ignored_topics`
        and reported in the progress UI, so the rest of the extraction continues.
        """

        if not isinstance(writer, MCAPFileWriter):
            raise TypeError(
                f"Type error in `{self.__class__.__name__}`: expects a mcap writer of type "
                f"`{MCAPFileWriter.__name__}` but got `{type(writer).__name__}`."
            )

        if t_name in self.ignored_topics:
            ui.advance_global()
            return

        # --- Resolve Adapter Check ---
        # For each Mosaico type Message find its adapter
        adapter = ms_loader.resolve_adapter(t_name)

        if adapter is None:
            return  # This should not happen since MosaicoToMCAPLoader should filter unsupported message types

        # --- Translate Check ---
        mcap_return_type = self._encode_mcap_message(adapter, ms_msg, t_name, ui)
        if mcap_return_type is None:
            return

        # --- Resolve Connection check ---
        mcap_metadata = ms_loader.resolve_metadata(t_name)

        if not mcap_metadata or not isinstance(mcap_metadata, MCAPSchemaMetadata):
            return self._handle_encoding_error(
                t_name,
                ui,
                f"Missing `_mcap_` metadata for topic '{t_name}': its original channel "
                "cannot be recovered. Skipping the topic.",
                "Failed writing to mcap because metadata are missing",
                style="yellow",
            )

        # --- Write check ---
        try:
            # Registers the topic's channel on its first message; later ones reuse it
            channel_id = writer.register_or_get_channel_id(mcap_metadata)

            # FIXME: the MCAP `sequence` is a per-message counter, but only the first message's
            # value is stored (in the topic's `_mcap_` metadata), so it is not written back and
            # the writer's default (0) is used. Pass it here once it is stored per message
            # (e.g. like `publish_time_ns`).
            writer.write(
                channel_id,
                mcap_return_type.data_bytes,
                ms_msg.timestamp_ns,
                mcap_return_type.publish_time_ns,
            )

        except Exception as e:
            return self._handle_encoding_error(
                t_name,
                ui,
                f"Could not write topic '{t_name}' to mcap. Skipping the topic. Reason: {e}",
                f"Failed writing to mcap because: {e}",
                style="yellow",
            )

        ui.advance_all(t_name)

    def _encode_mcap_message(
        self,
        adapter: Type[MCAPAdapterBase],
        ms_msg: Message,
        t_name: str,
        ui: ProgressManager,
    ) -> Optional[McapReturnType]:
        """
        Encodes one Mosaico message into its native MCAP bytes payload, or drops the topic.
        """
        try:
            return adapter.to_native(ms_msg)
        except (NotImplementedError, TypeError) as e:
            return self._handle_encoding_error(
                t_name,
                ui,
                f"Could not encode to mcap '{ms_msg.ontology_tag()}' type. "
                f"Skipping the topic associated to this message. Reason: {e}",
                "Failed encoding",
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


def mcap_sequence_extractor():
    """
    Command-line entrypoint.
    Parses arguments, sets up configuration, and initiates the sequence extractor
    """

    parser = argparse.ArgumentParser(
        description="Extracts sequences from Mosaico and encodes them as mcap files"
    )

    _add_common_arguments(
        parser,
        output_flag="--mcap_path",
        output_help="Path where to save the mcap file",
    )

    args = parser.parse_args()

    configs = MCAPExtractorConfig(**_common_config_kwargs(args))

    # --- Execution ---
    extractor = MCAPSequenceExtractor(configs)
    extractor.run()


if __name__ == "__main__":
    mcap_sequence_extractor()
