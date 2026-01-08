"""
Metadata Handling Module.

This module defines data structures (`SequenceMetadata`, `TopicMetadata`) and
utility functions for encoding/decoding metadata to be compatible with the
PyArrow Flight protocol. It also handles Mosaico-specific namespacing.
"""

import json
from dataclasses import dataclass
from typing import Any, Dict

from mosaicolabs.enum import SerializationFormat

UserMetadata = Dict[str, Any]

# Prefix for internal ROS keys that for now are filtered out of user metadata
_ROS_KEY_PREFIX = "ros:"


@dataclass
class SequenceMetadata:
    """
    Represents metadata specific to a Sequence.

    Attributes:
        context (str): The context type (must be "sequence").
        user_metadata (dict): A dictionary of user-provided metadata keys/values.
    """

    context: str
    user_metadata: UserMetadata

    @classmethod
    def from_dict(cls, mdata: Dict[str, Any]):
        """
        Factory method to create a SequenceMetadata instance from a dictionary.

        Args:
            mdata (Dict[str, Any]): The decoded metadata dictionary received from the server.

        Returns:
            SequenceMetadata: An initialized instance of this class.

        Raises:
            ValueError: If the "context" key is missing or is not "sequence".
        """
        context = _get_value(mdata, "context")
        if context != "sequence":
            raise ValueError("expected a sequence context")
        user_metadata = _get_value(mdata, "user_metadata")

        # Filter out internal ROS keys before presenting to the user
        return SequenceMetadata(
            context=context,
            user_metadata={
                key: val
                for key, val in user_metadata.items()
                if _ROS_KEY_PREFIX not in key
            },
        )


@dataclass
class TopicMetadata:
    """
    Represents metadata specific to a Topic.

    Attributes:
        context (str): The context type (must be "topic").
        properties (Properties): System-level properties (ontology tag, format).
        user_metadata (dict): A dictionary of user-provided metadata keys/values.
    """

    @dataclass
    class Properties:
        ontology_tag: str
        serialization_format: SerializationFormat

    context: str
    properties: Properties
    user_metadata: UserMetadata

    @classmethod
    def from_dict(cls, mdata: Dict[str, Any]):
        """
        Factory method to create a TopicMetadata instance from a dictionary.

        Args:
            mdata (Dict[str, Any]): The decoded metadata dictionary received from the server.

        Returns:
            TopicMetadata: An initialized instance of this class.

        Raises:
            ValueError: If the "context" key is missing or is not "topic".
        """
        context = _get_value(mdata, "context")
        if context != "topic":
            raise ValueError(f"expected a 'topic' context, got '{context}'")
        properties = _get_value(mdata, "properties")
        user_metadata = _get_value(mdata, "user_metadata")

        # Filter out internal ROS keys before presenting to the user
        return TopicMetadata(
            context=context,
            properties=TopicMetadata.Properties(**properties),
            user_metadata={
                key: val
                for key, val in user_metadata.items()
                if _ROS_KEY_PREFIX not in key
            },
        )


def _decode_metadata(bmdata: dict[bytes, bytes], enc: str = "utf-8") -> dict[str, Any]:
    """
    Decodes a bytes-only dictionary back into a Python dictionary.

    This function attempts to detect and parse JSON values automatically.
    If JSON parsing fails, the value is returned as a plain string.

    Args:
        bmdata (dict[bytes, bytes]): The raw metadata dictionary from Flight.
        enc (str): The encoding to use (default "utf-8").

    Returns:
        dict[str, Any]: The decoded Python dictionary.
    """
    result = {}
    for k, v in bmdata.items():
        key = k.decode(encoding=enc) if isinstance(k, bytes) else k
        value = v.decode(encoding=enc) if isinstance(v, bytes) else v

        # Try to parse JSON values automatically
        try:
            parsed = json.loads(value)
            result[key] = parsed
        except (json.JSONDecodeError, TypeError):
            # Fallback: keep the value as a string
            result[key] = value
    return result


def _get_value(metadata: Dict[str, Any], key: str) -> Any:
    """
    Helper to retrieve namespaced values from a decoded metadata dictionary.

    Mosaico server keys are often prefixed with "mosaico:". This function
    abstracts that prefix away from the caller.

    Args:
        metadata (Dict[str, Any]): The *already decoded* metadata dictionary.
        key (str): The logical key name (e.g., "context").

    Returns:
        Any: The value associated with the namespaced key.

    Raises:
        KeyError: If the prefixed key is missing from the dictionary.
    """
    # This prefix is an internal contract with the mosaico server.
    SERVER_MOSAICO_PREFIX: str = "mosaico:"

    full_key = SERVER_MOSAICO_PREFIX + key
    value = metadata[full_key]
    return value
