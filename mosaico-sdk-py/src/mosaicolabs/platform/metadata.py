"""
Metadata Handling Module.

This module defines data structures (`SequenceMetadata`, `TopicMetadata`) and
utility functions for encoding/decoding metadata to be compatible with the
PyArrow Flight protocol. It also handles Mosaico-specific namespacing.
"""

import json
from dataclasses import dataclass
from typing import Any, ClassVar, Dict, Literal

from typing_extensions import Self

from mosaicolabs.enum import SerializationFormat

UserMetadata = Dict[str, Any]

# Prefix for internal ROS keys that for now are filtered out of user metadata
_ROS_KEY_PREFIX = "ros:"

PlatformContext = Literal["sequence", "topic"]


@dataclass(slots=True)
class PlatformMetadata:
    """
    Represents the schema metadata specific to a platform resource.

    Attributes:
        context (str): The context type (must be "sequence" or "topic").
    """

    context: PlatformContext

    # Must be defined by subclasses
    _EXPECTED_CONTEXT: ClassVar[PlatformContext]

    @classmethod
    def _from_decoded_schema_metadata(cls, schema_metadata: Dict[str, Any]) -> Self:
        """
        Factory method to create a PlatformMetadata-derived instance from a dictionary.

        Args:
            schema_metadata (Dict[str, Any]): The decoded metadata dictionary received from the server.

        Returns:
            Self: An initialized instance of this class.

        Raises:
            ValueError: If the "context" key is missing.
        """
        context = _get_value(schema_metadata, "context")
        if context != cls._EXPECTED_CONTEXT:
            raise ValueError(
                f"expected '{cls._EXPECTED_CONTEXT}' context, got '{context}'"
            )
        return cls._from_metadata(schema_metadata)

    @classmethod
    def _from_metadata(cls, schema_metadata: Dict[str, Any]) -> Self:
        """Subclass-specific construction hook."""
        raise NotImplementedError(
            "Subclasses must define `_from_metadata` decoding classmethod"
        )

    @staticmethod
    def _filter_user_metadata(user_metadata: Dict[str, Any]) -> Dict[str, Any]:
        return {
            key: val for key, val in user_metadata.items() if _ROS_KEY_PREFIX not in key
        }


@dataclass(slots=True)
class SequenceMetadata(PlatformMetadata):
    """
    Represents metadata specific to a Sequence.

    Attributes:
        context (str): The context type (must be "sequence").
        user_metadata (dict): A dictionary of user-provided metadata keys/values.
    """

    _EXPECTED_CONTEXT: ClassVar[PlatformContext] = "sequence"

    user_metadata: UserMetadata

    @classmethod
    def _from_metadata(cls, schema_metadata: Dict[str, Any]) -> Self:
        """
        Subclass-specific construction hook.

        Args:
            schema_metadata (Dict[str, Any]): The decoded metadata dictionary received from the server.

        Returns:
            Self: An initialized instance of this class.
        """
        user_metadata = _get_value(schema_metadata, "user_metadata")

        return cls(
            context=cls._EXPECTED_CONTEXT,
            user_metadata=cls._filter_user_metadata(user_metadata),
        )


@dataclass(slots=True)
class TopicMetadata(PlatformMetadata):
    """
    Represents metadata specific to a Topic.

    Attributes:
        context (str): The context type (must be "topic").
        properties (Properties): System-level properties (ontology tag, format).
        user_metadata (dict): A dictionary of user-provided metadata keys/values.
    """

    _EXPECTED_CONTEXT: ClassVar[PlatformContext] = "topic"

    @dataclass(slots=True)
    class Properties:
        ontology_tag: str
        serialization_format: SerializationFormat

        @classmethod
        def _from_dict(cls, data: dict):
            return cls(
                ontology_tag=data["ontology_tag"],
                serialization_format=SerializationFormat(data["serialization_format"]),
            )

    properties: Properties
    user_metadata: UserMetadata

    @classmethod
    def _from_metadata(cls, schema_metadata: Dict[str, Any]) -> Self:
        """
        Subclass-specific construction hook.

        Args:
            schema_metadata (Dict[str, Any]): The decoded metadata dictionary received from the server.

        Returns:
            Self: An initialized instance of this class.
        """
        properties = _get_value(schema_metadata, "properties")
        user_metadata = _get_value(schema_metadata, "user_metadata")

        return cls(
            context=cls._EXPECTED_CONTEXT,
            properties=cls.Properties._from_dict(properties),
            user_metadata=cls._filter_user_metadata(user_metadata),
        )


def _decode_schema_metadata(
    bmdata: dict[bytes, bytes], enc: str = "utf-8"
) -> dict[str, Any]:
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
