"""
ROS Message Type Registry.

This module provides the central configuration point for the ROS Bridge.
It implements a **Context-Aware Singleton Registry** that manages custom message definitions.

ROS message definitions (`.msg`) are not self-contained; they depend on the specific
ROS distribution (e.g., `std_msgs/Header` differs between ROS 1 and ROS 2).
A naive global registry causes conflicts when analyzing data from mixed sources.

This registry stores definitions in "Profiles" (Stores).
1.  **GLOBAL Profile**: Definitions shared across all versions (e.g., simple custom types).
2.  **Scoped Profiles**: Definitions valid only for a specific typestore (e.g., `Stores.ROS1_NOETIC`).

When a `ROSDataLoader` is initialized, it requests a merged view of the Global + Scoped
definitions relevant to its specific data file.
"""

from pathlib import Path
from typing import Dict, Union, Optional
from collections import defaultdict
from rosbags.typesys import Stores
from ..logging import get_logger

# Set the hierarchical logger
logger = get_logger(__name__)


class ROSTypeRegistry:
    """
    A singleton registry for managing custom ROS message definitions.

    It allows users to register `.msg` files or raw definition strings, either globally
    or scoped to a specific ROS distribution. This ensures that the `ROSDataLoader`
    can deserialize proprietary or non-standard message types.

    **Structure:**
    The internal storage is a nested dictionary:
    ```python
    {
        "GLOBAL": { "pkg/msg/Type": "def..." },
        "ros1_noetic": { "pkg/msg/Type": "def_v1..." },
        "ros2_foxy": { "pkg/msg/Type": "def_v2..." }
    }
    ```
    """

    # Internal storage.
    # Key: Store Name (e.g. "GLOBAL", "ros2_foxy")
    # Value: Dict[MsgType, Definition]
    _registry: Dict[str, Dict[str, str]] = defaultdict(dict)

    @classmethod
    def register(
        cls,
        msg_type: str,
        source: Union[str, Path],
        store: Optional[Union[Stores, str]] = None,
    ):
        """
        Registers a single custom message type.

        Args:
            msg_type (str): The full ROS type name (e.g., "my_robot_msgs/msg/Status").
            source (Union[str, Path]): The definition source. Can be:
                - A `Path` object pointing to a `.msg` file.
                - A `str` containing the raw message definition text.
            store (Optional[Union[Stores, str]]): The scope of this definition.
                - `None` (Default): Registers as **GLOBAL**. Applies to all loaders.
                - `Stores.ROS2_FOXY` (or similar): Registers only for loaders using this specific store.

        Notes:
            Overwrites existing definition if the same type is registered twice in the same scope.

        Raises:
            FileNotFoundError: If `source` is a Path that does not exist.
            IOError: If reading the file fails.
        """
        try:
            # Resolve input to raw text string
            definition = cls._resolve_source(source)

            # Determine the registry key (Profile)
            # Convert enum to string if necessary to ensure consistent keys
            key = str(store) if store else "GLOBAL"

            # Store definition
            # Overwrites existing definition if the same type is registered twice in the same scope
            cls._registry[key][msg_type] = definition

            logger.debug(f"Registered custom type '{msg_type}' for scope: '{key}'")

        except Exception as e:
            logger.error(f"Failed to register type '{msg_type}': '{e}'")
            raise

    @classmethod
    def register_directory(
        cls,
        package_name: str,
        dir_path: Union[str, Path],
        store: Optional[Union[Stores, str]] = None,
    ):
        """
        Batch registers all `.msg` files in a directory.

        This helper infers the message type name based on the filename and the provided package name.
        e.g., `dir/Status.msg` -> `{package_name}/msg/Status`.

        Args:
            package_name (str): The ROS package name to prefix (e.g., "my_robot_msgs").
            dir_path (Union[str, Path]): The directory containing `.msg` files.
            store (Optional[Union[Stores, str]]): The scope for these definitions.

        Raises:
            ValueError: If `dir_path` is not a valid directory.
        """
        path = Path(dir_path)
        if not path.is_dir():
            raise ValueError(f"Path '{path}' is not a directory.")

        logger.debug(f"Scanning directory '{path}' for .msg files...")
        count = 0

        for msg_file in path.glob("*.msg"):
            # Construct standard ROS type name convention
            # filename "MyData.msg" -> type "MyData"
            type_name = f"{package_name}/msg/{msg_file.stem}"

            cls.register(type_name, msg_file, store=store)
            count += 1

        if count == 0:
            logger.warning(f"No .msg files found in '{path}'.")

    @classmethod
    def get_types(cls, store: Optional[Union[Stores, str]]) -> Dict[str, str]:
        """
        Retrieves the effective message definitions for a specific ROS distribution.

        This method implements a cascade logic:
        1.  Start with all **GLOBAL** definitions.
        2.  Overlay (update) with **Store-Specific** definitions.

        This ensures that a specific loader gets the most specific definition available,
        while falling back to global defaults for shared types.

        Args:
            store (Union[Stores, str]): The target typestore identifier (Optional).
                                        If None, returns the GLOBAL definitions.

        Returns:
            Dict[str, str]: A flat dictionary of `{msg_type: definition}` ready for
            injection into a `rosbags` Reader.
        """
        # Start with Global defaults
        # We use .copy() to ensure we don't accidentally mutate the registry itself
        merged = cls._registry["GLOBAL"].copy()

        if store:
            store_key = str(store)
            # Override
            if store_key in cls._registry:
                specific_types = cls._registry[store_key]
                # Update merges keys, overwriting globals if duplicates exist
                merged.update(specific_types)

        return merged

    @classmethod
    def reset(cls):
        """
        Clears the entire registry.
        Useful for unit testing to ensure isolation between tests.
        """
        cls._registry.clear()

    @staticmethod
    def _resolve_source(source: Union[str, Path]) -> str:
        """
        Internal helper to normalize input sources into a raw definition string.

        Args:
            source: A file path or a string.

        Returns:
            str: The raw text content of the message definition.
        """
        if isinstance(source, Path):
            if not source.exists():
                raise FileNotFoundError(f"Msg file not found: '{source}'")
            return source.read_text(encoding="utf-8")
        elif isinstance(source, str):
            # Heuristic check: is this a path string or a definition?
            # If it looks like a path and exists, treat as file.
            # Otherwise treat as raw definition.
            # (Note: This is a design choice; explicit Path objects are safer).
            possible_path = Path(source)
            try:
                if possible_path.exists() and possible_path.suffix == ".msg":
                    return possible_path.read_text(encoding="utf-8")
            except OSError:
                pass  # Filename too long or invalid, treat as definition string

            return source
        else:
            raise TypeError(
                f"Invalid source type: '{type(source)}'. Expected str or Path."
            )
