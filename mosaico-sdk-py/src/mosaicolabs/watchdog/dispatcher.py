"""
Choice of the injector for each file found by the Watchdog.

The `InjectorDispatcher` holds the file extensions the Watchdog supports and decides, for a
given file, whether it is loaded by `RosbagInjector` or by `MCAPInjector`. It returns the
injector class: the Watchdog builds the matching config and runs it.
"""

from pathlib import Path
from typing import Tuple, Type

from mcap.summary import Summary
from mcap.well_known import SchemaEncoding

from mosaicolabs.bridges.mcap import MCAPInjector
from mosaicolabs.bridges.mcap.mcap_file import MCAPFileReader
from mosaicolabs.bridges.ros import RosbagInjector


class InjectorDispatcher:
    """
    Chooses the injector for a file, from its extension and, for `.mcap` files, from the
    encoding of its channels:

    - `.bag` and `.db3`: `RosbagInjector`.
    - `.mcap`: `RosbagInjector` if the file has at least one channel and every channel has a
      ROS 2 schema (`ros2msg` or `ros2idl`). Otherwise `MCAPInjector`, which skips the
      channels it cannot decode: this covers schemaless channels, other encodings and
      ROS 1 `.mcap` files.

    Injectors are not registered: the dispatcher depends on them directly.
    """

    SUPPORTED_EXTENSIONS: Tuple[str, ...] = (".mcap", ".db3", ".bag")
    """The file extensions the Watchdog can load. Matching is case-sensitive."""

    @classmethod
    def list_supported_ext(cls) -> Tuple[str, ...]:
        """
        Returns the supported file extensions, to pass to the File Source so that it only
        lists files the dispatcher can handle.

        Returns:
            Tuple[str, ...]: The extensions, with the leading dot (e.g. `".mcap"`).
        """
        return cls.SUPPORTED_EXTENSIONS

    @classmethod
    def get_injector(
        cls, absolute_file_path: Path
    ) -> Type[RosbagInjector] | Type[MCAPInjector]:
        """
        Returns the injector class to load a file with. For `.mcap` files the file is
        opened to read its summary (channels and schemas), so it must be available
        locally: the Watchdog passes the path returned by `FileSource.open_local()`.

        Args:
            absolute_file_path (Path): The local path of the file.

        Returns:
            Type[RosbagInjector] | Type[MCAPInjector]: The injector class, not an instance.

        Raises:
            RuntimeError: If the extension is not supported, or if an `.mcap` file has no
                summary section.
            FileNotFoundError: If an `.mcap` file does not exist (e.g. deleted after the scan).
            mcap.exceptions.McapError: If an `.mcap` file is corrupt or truncated.
        """
        file_suffix = absolute_file_path.suffix

        if file_suffix not in cls.SUPPORTED_EXTENSIONS:
            raise RuntimeError(
                f"File {absolute_file_path} has extension `{file_suffix}`, that is not supported."
                f"Supported extensions are: {cls.list_supported_ext()}."
            )

        if file_suffix in (".db3", ".bag"):
            return RosbagInjector

        with MCAPFileReader(absolute_file_path, None) as mcap_file:
            summary: Summary | None = mcap_file.reader.get_summary()
            if summary is None:  # Cannot get injector if summary is unknown
                raise RuntimeError(
                    f"Impossible to get injector for {absolute_file_path} since it has no summary."
                )

            channels = summary.channels
            schemas = summary.schemas

            # Check that all channels have encoding that are ROS2.
            # In case of schemaless channels fallback to MCAPInjector
            is_ros = bool(channels) and all(
                channel.schema_id in schemas
                and schemas[channel.schema_id].encoding
                in (
                    SchemaEncoding.ROS2,
                    SchemaEncoding.ROS2IDL,
                )  # NOTE: ROS1 .mcap should still be handled by MCAPInjector
                for channel in channels.values()
            )

            if is_ros:
                return RosbagInjector
            else:
                return MCAPInjector
