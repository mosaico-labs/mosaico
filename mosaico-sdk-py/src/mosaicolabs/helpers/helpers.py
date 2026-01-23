"""
Helper Utilities.

Provides utility functions for dict manipulation and other things
"""

import ast
from dataclasses import is_dataclass
from pathlib import Path
import re
from typing import Any, Iterable, Optional

from pydantic import BaseModel


def camel_to_snake(name: str) -> str:
    """
    Converts a string from CamelCase or PascalCase into snake_case.

    This function inserts an underscore before any capital letter that is
    not already preceded by a lowercase letter (s1), and then inserts an
    underscore before any capital letter preceded by a lowercase letter or a number (s2).
    Finally, the entire string is converted to lowercase.

    Examples:
        - "LidarPolar2DDetection" -> "lidar_polar_2d_detection"
        - "URLConverter" -> "url_converter"
        - "GPS3DPosition" -> "gps3d_position" (Note: may not handle all acronyms ideally)

    Args:
        name: The input string in CamelCase or PascalCase format.

    Returns:
        The converted string in snake_case format.
    """
    # Insert an underscore between a lowercase character/digit and an
    # uppercase character, but only if the uppercase character is followed
    # by one or more lowercase characters.
    # This primarily handles transitions like 'Message' -> 'Sensor_Factory'
    s1 = re.sub(r"(.)([A-Z][a-z]+)", r"\1_\2", name)

    # Insert an underscore between a lowercase character/digit and an
    # uppercase character (handles trailing acronyms like 'URL' or 'ID').
    # 'Sensor_Factory' -> 'sensor_factory'
    # 'GPS3DPosition' -> 'GPS3D_Position'
    s2 = re.sub(r"([A-z]+)([0-9]+)([A-z]+)", r"\1_\2_\3", s1)

    # Convert the entire resulting string to lowercase.
    return s2.lower()


def flatten_dict(
    d: dict[str, Any], parent_key: str = "", sep: str = "."
) -> dict[str, str]:
    """
    Recursively flattens a nested dictionary into a single-level dictionary.

    :param d: The dictionary to flatten.
    :param parent_key: The base key to prepend to new keys (used in recursion).
    :param sep: The separator to use between keys.
    :return: A new flattened dictionary.
    """
    items = {}
    for k, v in d.items():
        new_key = parent_key + k

        if isinstance(v, dict):
            # Recursively call the function for nested dictionaries
            items.update(flatten_dict(v, new_key + sep, sep=sep))
        else:
            # Add non-dict values to the result
            items[new_key] = str(v)

    return items


def unflatten_dict(d: dict[str, Any], sep: str = ".") -> dict[str, Any]:
    """
    Converts a flattened dictionary back into a nested dictionary structure,
    decoding Python-style literals (None, True, False, numbers, lists, dicts) along the way.

    :param d: The flattened dictionary (e.g., {'a.b': '1', 'a.c': '[1,2]'}).
    :param sep: The separator used to join the keys (default is '.').
    :return: A new, nested dictionary with decoded values.
    """

    def decode_value(value: Any) -> Any:
        # Only attempt decoding if it's a string
        if isinstance(value, str):
            try:
                # Safely evaluate Python literals
                return ast.literal_eval(value)
            except (ValueError, SyntaxError):
                # Fallback: return original string
                return value
        return value

    unflattened_dict = {}

    for compound_key, value in d.items():
        # Decode value
        value = decode_value(value)

        # Split the compound key into individual keys
        keys = compound_key.split(sep)

        # Traverse the nested dict structure
        current_dict = unflattened_dict
        for i, key in enumerate(keys):
            is_last_key = i == len(keys) - 1
            if is_last_key:
                current_dict[key] = value
            else:
                if key not in current_dict:
                    current_dict[key] = {}
                current_dict = current_dict[key]

    return unflattened_dict


def encode_to_dict(obj: Any, exclude_none: bool = False) -> Any:
    """
    Recursively converts a Pydantic model, dataclass, or nested structures (lists, tuples)
    into a standard Python dictionary representation.

    Args:
        obj: The input object to encode. Can be a Pydantic model, dataclass, list, tuple, or primitive.
        skip_none (bool): If True, omit fields with None values from the resulting dictionary.

    Returns:
        Any: A dictionary (for models/dataclasses), a list/tuple (for iterables),
             or the original primitive value if not a supported structure.
    """

    # Handle explicit None values
    if obj is None:
        return None

    # --- Handle Pydantic model instances ---
    # Pydantic models provide a built-in method `.model_dump()` which converts the model
    # (and all nested models) into a plain Python dictionary recursively.
    if isinstance(obj, BaseModel):
        return obj.model_dump(exclude_none=exclude_none)

    # --- Handle dataclass instances ---
    # Convert dataclasses into dictionaries, recursively encoding their attributes.
    # We skip private fields (those starting with '_') and optionally skip None values.
    if is_dataclass(obj) and not isinstance(obj, type):
        return {
            key: encode_to_dict(value, exclude_none=exclude_none)
            for key, value in obj.__dict__.items()
            if not key.startswith("_") and (value is not None or not exclude_none)
        }

    # --- Handle iterable types (lists and tuples) ---
    # Recursively apply encoding to each element in the collection.
    if isinstance(obj, (list, tuple)):
        # Preserve the original container type (list or tuple)
        return type(obj)(
            encode_to_dict(item, exclude_none=exclude_none) for item in obj
        )

    # --- Base case: primitive or non-special object ---
    # Return primitive types (int, str, float, datetime, etc.) as-is.
    return obj


def truncate_long_strings(data, max_length=100):
    """
    Recursively traverse nested structures (dict, list, tuple, set)
    and truncate any string values longer than `max_length`.
    """

    # --- Handle strings ---
    if isinstance(data, str):
        # Truncate strings longer than allowed length
        return data if len(data) <= max_length else data[:max_length] + "..."

    # --- Handle dictionaries ---
    if isinstance(data, dict):
        # Recursively process keys and values
        return {
            key: truncate_long_strings(value, max_length) for key, value in data.items()
        }

    # --- Handle lists ---
    if isinstance(data, Iterable):
        # Rebuild using the same container type
        return type(data)(
            truncate_long_strings(data=item, max_length=max_length) for item in data
        )

    # --- Base case: return the value unchanged ---
    return data


def sanitize_topic_name(name: str):
    name = name.strip("/")
    if not name.startswith("/"):
        return "/" + name
    return name


def sanitize_sequence_name(name: str):
    name = name.strip("/")
    if name.startswith("/"):
        return name[1:]
    return name


def pack_topic_resource_name(sequence_name: str, topic_name: str) -> str:
    """
    Constructs the full resource path for a topic.

    Args:
        sequence_name (str): The parent sequence name.
        topic_name (str): The topic name.

    Returns:
        str: A combined path string (e.g., "seq_1/topic_name").
    """
    rbase = Path(sequence_name)
    if rbase.is_absolute():
        rbase = rbase.relative_to("/")  # strip leading '/'
    sub = Path(topic_name)
    if sub.is_absolute():
        sub = sub.relative_to("/")  # strip leading '/'
    return str(rbase / sub)


def unpack_topic_full_path(topic_path: str) -> Optional[tuple[str, str]]:
    """
    Splits a full resource path back into sequence and topic names.

    Args:
        topic_path (str): The full path (e.g., "seq_1/sensor_A").

    Returns:
        Optional[tuple[str, str]]: A tuple (sequence_name, topic_name), or None if invalid.
    """
    # topic may come as '/sequence_name/the/topic/name' or as 'sequence_name/the/topic/name'
    tick_parts = topic_path.strip("/").split("/")
    if not tick_parts or len(tick_parts) < 2:
        return None
    sname = tick_parts[0]
    tname = sanitize_topic_name("/".join(tick_parts[1:]))
    return sname, tname
