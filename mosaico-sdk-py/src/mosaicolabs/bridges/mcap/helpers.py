from typing import Dict, List, Optional

from mcap.records import Channel, Schema

from mosaicolabs import TopicHandler

from ..helpers import _filter_from_list
from .adapter_base import MCAPSchemaMetadata


def _extract_mcap_metadata(t_handler: TopicHandler) -> MCAPSchemaMetadata:
    """
    Reads and validates the ``_mcap_`` metadata field.

    Each of:
        - ``schema_name``
        - ``schema_encoding``
        - ``schema_def``
        - ``channel_name``
        - ``channel_encoding``

    is required (see `MCAPSchemaMetadata.REQUIRED_KEYS`) and must hold a string.

    Args:
        t_handler (TopicHandler): The topic handler whose metadata should be inspected.

    Returns:
        MCAPSchemaMetadata: The topic's validated MCAP metadata.

    Raises:
        ValueError: When the topic carries no ``_mcap_`` metadata, or any required field is
            missing from it.
        TypeError: When a required field of the ``_mcap_`` metadata has an unexpected type.
    """
    return MCAPSchemaMetadata(**MCAPSchemaMetadata.extract(t_handler.user_metadata))


def _filter_channels_from_dict(
    available_channels: Dict[str, Channel], requested_channles: Optional[List[str]]
) -> Dict[str, Channel]:
    """
    Resolve the set of channels to be processed based on user-provided glob patterns.

    This method filters `available_channels` according to the patterns defined in
    `self._requested_channels`, using ORDER-DEPENDENT (gitignore-like) semantics.
    Pattern semantics:
        - Patterns use standard shell-style wildcards (via `fnmatch`):
            * "*" matches any sequence of characters
            * "?" matches any single character
        - Patterns NOT starting with "!" are treated as inclusion patterns.
        - Patterns starting with "!" are treated as exclusion patterns.

    Patterns are evaluated sequentially, and each pattern modifies the current
    selection of channels. Evaluation rules:
        - Patterns are processed in the order they appear.
        - Each non-"!" pattern adds matching channels to the result set.
        - Each "!" pattern removes matching channels from the result set.
        - Later patterns override earlier ones.
        - If no inclusion pattern is present, the initial set is ALL available channels,
          which are then filtered by subsequent exclusion patterns.

    Args:
        available_channels (Dict[str, Channel]):
            Mapping of channel names to their associated metadata.
        requested_channles (Optional[List[str]]):
            Optional list of channel names or patterns to filter results.
            Only channels matching any of the provided values will be returned.

    Examples:
        ["gps*", "!gps_leica.time_reference"]
            → include all gps* channels except the Leica time_reference channel

        ["!gps*", "gps_leica.time_reference"]
            → exclude all gps* channels, then re-include the specific channel

        ["foo*"]
            → include only channels starting with "foo"

        ["!foo*"]
            → include all channels except those starting with "foo"

        []
            → include all available channels

    Warnings:
        - A warning is logged if a pattern matches no channels.
    """

    if not requested_channles:
        return available_channels

    resolved_keys = _filter_from_list(available_channels.keys(), requested_channles)

    return {key: val for key, val in available_channels.items() if key in resolved_keys}


def _sanitize_mcap_name(channel_name: str) -> str:
    """Turns an MCAP name into a Mosaico topic name: dots become slashes, with a leading
    `/` (e.g. `front_car.imu` -> `/front_car/imu`)."""

    prefix = "" if channel_name.startswith("/") else "/"
    return prefix + channel_name.replace(".", "/")


def _class_name_from_mcap_schema(schema: Schema) -> str:
    """Returns the ontology tag of the Unmodeled ontology created for `schema`: the last
    component of its name, without package or encoding (e.g. `sensor_msgs.Imu` -> `Imu`)."""

    out = _sanitize_mcap_name(schema.name)
    return out.split("/")[-1]
