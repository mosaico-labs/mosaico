from typing import Dict, List, Optional

from mcap.decoder import DecoderFactory
from mcap.records import Channel, Schema

from ..helpers import _filter_from_list


class JsonDecoderFactory(DecoderFactory):
    """Decodes `json`-message-encoded MCAP records.

    Keys purely off `message_encoding`, matching `channel.message_encoding` (NOT
    `schema.encoding`, which is `"jsonschema"` for this same channel kind). Passes the raw
    bytes through unchanged; `MCAPJsonschemaMsgDecoder._to_dict` does the actual `json.loads`.
    """

    def decoder_for(self, message_encoding: str, schema: Optional[Schema]):
        if message_encoding != "json":
            return None
        return lambda data: data


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

    Side Effects:
        - Returns a filtered dictionary of channels (no longer sets internal state).
    """

    if not requested_channles:
        return available_channels

    resolved_keys = _filter_from_list(available_channels.keys(), requested_channles)

    return {key: val for key, val in available_channels.items() if key in resolved_keys}


def _class_name_from_mcap_channel(channel: Channel):
    return channel.topic.split(".")[-1]
