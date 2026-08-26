"""
ROS Message Type Registry.

This module provides a small, self-contained registry of custom ROS message
definitions, used by [`RosbagInjector`][mosaicolabs.bridges.ros.RosbagInjector] and
[`ROSSequenceExtractor`][mosaicolabs.bridges.ros.ROSSequenceExtractor] to build the
`Typestore` they hand to their respective loader.

ROS message definitions (`.msg`) are not self-contained; they depend on the specific
ROS distribution (e.g., `std_msgs/Header` differs between ROS 1 and ROS 2).

Each `ROSTypeRegistry` instance stores definitions in "Profiles" ([`rosbags.typesys.Stores`](https://ternaris.gitlab.io/rosbags/topics/typesys.html#type-stores)).
1.  **GLOBAL Profile**: Definitions shared across all versions (e.g., simple custom types).
2.  **Scoped Profiles**: Definitions valid only for a specific typestore (e.g., `Stores.ROS1_NOETIC`).

`RosbagInjector`/`ROSSequenceExtractor` each own a private, freshly-created instance by
default — so registering custom types for one run never leaks into another. To share
definitions across multiple injector/extractor runs (e.g. a centralized setup routine
covering hundreds of proprietary types), construct one `ROSTypeRegistry()` yourself and
pass that same instance to every config's `registry` field.
"""

from collections import defaultdict
from pathlib import Path
from typing import Dict, Optional, Union

from rosbags.typesys import Stores

from ...logging_config import get_logger

# Set the hierarchical logger
logger = get_logger(__name__)


class ROSTypeRegistry:
    """
    A context-aware registry for custom ROS message definitions.

    ROS message definitions (`.msg`) are not self-contained; they often vary between
    distributions (e.g., `std_msgs/Header` has different fields in ROS 1 Noetic vs. ROS 2 Humble).
    `ROSTypeRegistry` resolves this by organizing schemas into "Profiles"
    ([`rosbags.typesys.Stores`](https://ternaris.gitlab.io/rosbags/topics/typesys.html#type-stores)).

    Each instance is independent. By default, [`RosbagInjector`][mosaicolabs.bridges.ros.RosbagInjector]
    and [`ROSSequenceExtractor`][mosaicolabs.bridges.ros.ROSSequenceExtractor] each construct
    their own private instance, so one run's custom types can never leak into another's
    in the same process. To deliberately share a set of definitions across many runs,
    construct a single instance and pass it explicitly wherever it should apply (e.g. via
    each config's `registry` field) — that explicit passing is the only way definitions
    become visible outside the instance they were registered on.

    ### Why Use a Registry?
    * **Conflict Resolution**: Keeps custom types registered for one injector/extractor run from silently affecting another's, even within the same process.
    * **Proprietary Support**: Allows the system to ingest non-standard messages (e.g., custom robot state or specialized sensor payloads).
    * **Cascading Logic**: Implements an "Overlay" mechanism where global definitions provide a baseline, and distribution-specific definitions provide high-precision overrides.

    Attributes:
        _registry (Dict[str, Dict[str, str]]): Internal private storage for definitions.
            The first key level represents the **Scope** (e.g., "GLOBAL", "Stores.ROS2_FOXY"),
            and the second level maps the **Message Type** to its raw **Definition String**.
    """

    def __init__(self):
        # Internal storage.
        # Key: Store Name (e.g. "GLOBAL", "ros2_foxy")
        # Value: Dict[MsgType, Definition]
        self._registry: Dict[str, Dict[str, str]] = defaultdict(dict)

    def register(
        self,
        msg_type: str,
        source: Union[str, Path],
        store: Optional[Union[Stores, str]] = None,
    ):
        """
        Registers a single custom message type into this registry instance.

        This is the primary method for adding individual schema definitions. The `source`
        can be a physical file path or a raw string containing the ROS `.msg` syntax.

        Example:
            ```python
            registry = ROSTypeRegistry()

            # Register a proprietary battery message for a specific distro
            registry.register(
                msg_type="limo_msgs/msg/BatteryState",
                source=Path("./msgs/BatteryState.msg"),
                store=Stores.ROS2_HUMBLE
            )

            # Register a simple custom type globally using a raw string
            registry.register(
                msg_type="custom_msgs/msg/SimpleFlag",
                source="bool flag_active\\nstring label"
            )
            ```

        Args:
            msg_type (str): The canonical ROS type name (e.g., "package/msg/TypeName").
            source (Union[str, Path]): A `Path` object to a `.msg` file or a raw text string of the definition.
            store (Optional[Union[Stores, str]]): The target scope. If `None`, the definition is stored in the **GLOBAL** profile
                and becomes available regardless of the distribution requested via `get_types()`.

        Raises:
            FileNotFoundError: If `source` is a `Path` that does not exist on the filesystem.
            TypeError: If the `source` is neither a `str` nor a `Path`.
        """
        try:
            # Resolve input to raw text string
            definition = self._resolve_source(source)

            # Determine the registry key (Profile)
            # Convert enum to string if necessary to ensure consistent keys
            key = str(store) if store else "GLOBAL"

            # Store definition
            # Overwrites existing definition if the same type is registered twice in the same scope
            self._registry[key][msg_type] = definition

            logger.debug(f"Registered custom type '{msg_type}' for scope: '{key}'")

        except Exception as e:
            logger.error(f"Failed to register type '{msg_type}': '{e}'")
            raise

    def register_directory(
        self,
        package_name: str,
        dir_path: Union[str, Path],
        store: Optional[Union[Stores, str]] = None,
    ):
        """
        Batch registers all `.msg` files found within a specified directory.

        This helper is essential for ingesting entire ROS packages. It automatically
        infers the full ROS type name by combining the `package_name` with the
        filename (e.g., `Status.msg` becomes `package_name/msg/Status`).

        Example:
            ```python
            # Register all messages in a local workspace for ROS1
            registry = ROSTypeRegistry()
            registry.register_directory(
                package_name="robot_logic",
                dir_path="./src/robot_logic/msg",
                store=Stores.ROS1_NOETIC
            )
            ```

        Args:
            package_name (str): The name of the ROS package to use as a prefix.
            dir_path (Union[str, Path]): The filesystem path to the directory containing `.msg` files.
            store (Optional[Union[Stores, str]]): The target distribution scope for the entire directory.

        Raises:
            ValueError: If `dir_path` does not point to a valid directory.
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

            self.register(type_name, msg_file, store=store)
            count += 1

        if count == 0:
            logger.warning(f"No .msg files found in '{path}'.")

    def get_types(self, store: Optional[Union[Stores, str]]) -> Dict[str, str]:
        """
        Retrieves a merged view of message definitions for a specific distribution.

        This method implements the **Registry Cascade**:
        1. It starts with a base layer of all **GLOBAL** definitions.
        2. It overlays (overwrites) those with any **Store-Specific** definitions matching
           the provided `store` parameter.

        This ensures that distribution-specific nuances are respected while maintaining
        access to shared custom types.

        Args:
            store (Optional[Union[Stores, str]]): The distribution identifier (e.g., `Stores.ROS2_HUMBLE`) to fetch overrides for.

        Returns:
            Dict[str, str]: A flat dictionary mapping `msg_type` to `definition`, formatted for
                direct injection into `rosbags` high-level readers.
        """
        # Start with Global defaults
        # We use .copy() to ensure we don't accidentally mutate the registry itself
        merged = self._registry["GLOBAL"].copy()

        if store:
            store_key = str(store)
            # Override
            if store_key in self._registry:
                specific_types = self._registry[store_key]
                # Update merges keys, overwriting globals if duplicates exist
                merged.update(specific_types)

        return merged

    def reset(self):
        """
        Completely clears this registry instance.

        This is primarily used for **Unit Testing** and CI/CD pipelines to ensure
        total isolation between different test cases.
        """
        self._registry.clear()

    @staticmethod
    def _resolve_source(source: Union[str, Path]) -> str:
        """
        Internal utility to normalize varied inputs into raw definition text.

        Args:
            source (Union[str, Path]): A `Path` or `str`. If a string is provided that exists as a file
                path ending in `.msg`, it is read from the disk.

        Returns:
            str: The raw text content of the ROS message definition.
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
