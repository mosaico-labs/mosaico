from pathlib import Path
from typing import Dict, List, Optional

from mcap.decoder import DecoderFactory
from mcap.reader import McapReader, make_reader
from mcap.writer import Writer as McapWriter

from ..protocols.mcap import McapSchemaRegistry
from .adapter_base import MCAPSchemaMetadata


class MCAPFileReader:
    """
    Thin resource wrapper around a single MCAP file on disk.

    It owns the file handle and the underlying `mcap` library reader, lazily
    opening the file (and validating its path/extension) on first access to
    `.reader` rather than at construction time.

    Attributes:
        ACCEPTED_EXTENSIONS: Set of supported file extensions {'.mcap'}.
    """

    ACCEPTED_EXTENSIONS = {".mcap"}

    def __init__(
        self, file_path: Path, decoder_factory: Optional[List[DecoderFactory]]
    ):
        self._file_path: Path = file_path
        self._decoder_factory: List[DecoderFactory] = decoder_factory or []
        self._file = None
        self._reader = None

    def __del__(self):
        self.close()

    def _validate_file(self):
        if not self._file_path.exists():
            raise FileNotFoundError(f"MCAP file not found: {self._file_path}")
        if self._file_path.suffix not in self.ACCEPTED_EXTENSIONS:
            raise ValueError(
                f"Unsupported format '{self._file_path.suffix}'. Supported: {self.ACCEPTED_EXTENSIONS}"
            )

    def _open(self) -> McapReader:

        if self._reader is not None:
            return self._reader

        self._validate_file()

        try:
            self._file = open(self._file_path, "rb")
        except Exception as e:
            raise IOError(f"Could not open mcap file: '{e}'") from e

        self._reader = make_reader(self._file, decoder_factories=self._decoder_factory)

        return self.reader

    def close(self):
        if self._file is not None:
            self._file.close()
            self._file = None
            self._reader = None

    @property
    def reader(self) -> McapReader:
        if self._reader is None:
            return self._open()

        return self._reader


class MCAPFileWriter:
    def __init__(self, path: Path, **writer_kwargs):
        # mcap.writer.Writer opens (and later closes) the file itself when given a path str.

        path.mkdir(parents=True, exist_ok=True)
        full_file_path = path / (path.name + ".mcap")

        self._writer = McapWriter(str(full_file_path), **writer_kwargs)
        self._writer.start()
        self._channel_ids: Dict[str, int] = {}
        self._finished = False

    def register_or_get_channel_id(self, mcap_metadata: MCAPSchemaMetadata) -> int:
        """
        Registers the schema + channel pair described by `mcap_metadata`, or returns the
        already-registered channel_id if its channel name was registered before.

        The metadata is only unpacked, and its stringified schema definition only turned back
        into bytes, the first time a channel is seen.

        Args:
            mcap_metadata (MCAPSchemaMetadata): The topic's `_mcap_` metadata, recording its
                original channel and schema.

        Returns:
            int: The id of the channel to write the topic's messages to.

        Raises:
            ValueError: If no `McapSchemaConverter` is registered for the schema encoding.
        """
        channel_name = mcap_metadata.get_channel_name()

        # Cached
        if channel_name in self._channel_ids:
            return self._channel_ids[channel_name]

        # schema_def is stored as a string, while its original bytes are interpreted
        # differently depending on the encoding (`protobuf`, `jsonschema`, ...): they are
        # recovered through the McapSchemaConverter registered for that encoding.
        schema_encoding = mcap_metadata.get_schema_encoding()
        schema_converter = McapSchemaRegistry.get_converter(schema_encoding)

        if schema_converter is None:
            raise ValueError(
                f"Cannot register channel '{channel_name}': no schema converter is registered "
                f"for `{schema_encoding}` encoding. "
                f"Supported encodings: {McapSchemaRegistry.all_supported_encodings()}"
            )

        # Create a new one (schema + channel registration)
        schema_id = self._writer.register_schema(
            name=mcap_metadata.get_schema_name(),
            encoding=schema_encoding,
            data=schema_converter.destringify_schema_def(
                mcap_metadata.get_schema_def()
            ),
        )
        channel_id = self._writer.register_channel(
            topic=channel_name,
            message_encoding=mcap_metadata.get_channel_encoding(),
            schema_id=schema_id,
        )
        self._channel_ids[channel_name] = channel_id
        return channel_id

    def write(
        self,
        channel_id: int,
        data: bytes,
        log_time: int,
        publish_time: Optional[int] = None,
        sequence: Optional[int] = None,
    ):
        if channel_id not in self._channel_ids.values():
            raise ValueError(
                f"channel id '{channel_id}' is not registered; "
                f"call register_or_get_channel_id() before write()"
            )
        self._writer.add_message(
            channel_id=channel_id,
            log_time=log_time,
            data=data,
            publish_time=publish_time if publish_time is not None else log_time,
            sequence=sequence if sequence else 0,
        )

    def finish(self):
        if not self._finished:
            self._writer.finish()
            self._finished = True

    def __enter__(self):
        return self

    def __exit__(self, exc_type, exc, tb):
        self.finish()
