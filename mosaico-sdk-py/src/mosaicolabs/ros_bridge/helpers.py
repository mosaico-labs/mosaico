import fnmatch
from typing import Any, Dict, List, Optional

import numpy as np
from rosbags.interfaces import TopicInfo

from mosaicolabs.logging_config import get_logger

# Set the hierarchical logger
logger = get_logger(__name__)


def _to_dict(message: Any) -> Any:
    """
    Recursively converts a rosbags message object and its nested fields
    to a standard Python dictionary or a list/primitive type if encountered
    during recursion.
    """
    if hasattr(message, "__msgtype__"):
        data_dict = {}
        fields = getattr(
            message,
            "__slots__",
            [k for k in dir(message) if not k.startswith("_") and k != "__msgtype__"],
        )
        for field_name in fields:
            if field_name.startswith("_") or field_name == "__msgtype__":
                continue
            try:
                field_value = getattr(message, field_name)
                data_dict[field_name] = _to_dict(field_value)
            except AttributeError:
                continue
        return data_dict
    elif isinstance(message, (list, tuple)):
        return [_to_dict(item) for item in message]
    elif isinstance(message, np.ndarray):
        return message.tolist()
    elif hasattr(message, "sec") and hasattr(message, "nanosec"):
        try:
            # Convert ROS time structure to a single float timestamp (seconds)
            return message.sec + message.nanosec * 1e-9
        except Exception:
            return message
    return message


def _filter_topics_from_list(
    available_topics: List[str], requested_topics: Optional[List[str]]
) -> List[str]:
    """
    Resolve the set of topics to be processed based on user-provided glob patterns.

    This method filters `available_topics` according to the patterns defined in
    `requested_topics`, using ORDER-DEPENDENT (gitignore-like) semantics.
    Pattern semantics:
        - Patterns use standard shell-style wildcards (via `fnmatch`):
            * "*" matches any sequence of characters
            * "?" matches any single character
        - Patterns NOT starting with "!" are treated as inclusion patterns.
        - Patterns starting with "!" are treated as exclusion patterns.

    Patterns are evaluated sequentially, and each pattern modifies the current
    selection of topics. Evaluation rules:
        - Patterns are processed in the order they appear.
        - Each non-"!" pattern adds matching topics to the result set.
        - Each "!" pattern removes matching topics from the result set.
        - Later patterns override earlier ones.
        - If no inclusion pattern is present, the initial set is ALL available topics,
          which are then filtered by subsequent exclusion patterns.

    Args:
        available_topics (list[str]):
            List of topic names.
        requested_topics (Optional[List[str]]):
            Optional list of topic names or patterns to filter results.
            Only topics matching any of the provided values will be returned.

    Examples:
        ["/gps/*", "!/gps/leica/time_reference"]
            → include all /gps/* topics except the Leica time_reference topic

        ["!/gps/*", "/gps/leica/time_reference"]
            → exclude all /gps/* topics, then re-include the specific topic

        ["foo*"]
            → include only topics starting with "foo"

        ["!foo*"]
            → include all topics except those starting with "foo"

        []
            → include all available topics

    Warnings:
        - A warning is logged if a pattern matches no topics.

    Side Effects:
        - Returns a filtered list of topics.
    """

    if not requested_topics:
        return available_topics

    # # If there is at least one include pattern, we start empty.
    # # Otherwise we start from all topics (implicit include-all).
    has_include = any(not p.startswith("!") for p in requested_topics)

    if has_include:
        resolved_topics = set()
    else:
        resolved_topics = set(available_topics)

    for pattern in requested_topics:
        exclude_me = pattern.startswith("!")
        raw_pattern = pattern[1:] if exclude_me else pattern

        matches = fnmatch.filter(available_topics, raw_pattern)

        if not matches:
            logger.warning(f"Topic pattern '{pattern}' matched nothing in this bag.")
            continue

        match_set = set(matches)

        if exclude_me:
            resolved_topics -= match_set
        else:
            resolved_topics |= match_set

    return list(resolved_topics)


def _filter_topics_from_dict(
    available_topics: Dict[str, TopicInfo], requested_topics: Optional[List[str]]
) -> Dict[str, TopicInfo]:
    """
    Resolve the set of topics to be processed based on user-provided glob patterns.

    This method filters `available_topics` according to the patterns defined in
    `self._requested_topics`, using ORDER-DEPENDENT (gitignore-like) semantics.
    Pattern semantics:
        - Patterns use standard shell-style wildcards (via `fnmatch`):
            * "*" matches any sequence of characters
            * "?" matches any single character
        - Patterns NOT starting with "!" are treated as inclusion patterns.
        - Patterns starting with "!" are treated as exclusion patterns.

    Patterns are evaluated sequentially, and each pattern modifies the current
    selection of topics. Evaluation rules:
        - Patterns are processed in the order they appear.
        - Each non-"!" pattern adds matching topics to the result set.
        - Each "!" pattern removes matching topics from the result set.
        - Later patterns override earlier ones.
        - If no inclusion pattern is present, the initial set is ALL available topics,
          which are then filtered by subsequent exclusion patterns.

    Args:
        available_topics (Dict[str, TopicInfo]):
            Mapping of topic names to their associated metadata.
        requested_topics (Optional[List[str]]):
            Optional list of topic names or patterns to filter results.
            Only topics matching any of the provided values will be returned.

    Examples:
        ["/gps/*", "!/gps/leica/time_reference"]
            → include all /gps/* topics except the Leica time_reference topic

        ["!/gps/*", "/gps/leica/time_reference"]
            → exclude all /gps/* topics, then re-include the specific topic

        ["foo*"]
            → include only topics starting with "foo"

        ["!foo*"]
            → include all topics except those starting with "foo"

        []
            → include all available topics

    Warnings:
        - A warning is logged if a pattern matches no topics.

    Side Effects:
        - Returns a filtered dictionary of topics (no longer sets internal state).
    """

    if not requested_topics:
        return available_topics

    resolved_keys = _filter_topics_from_list(available_topics.keys(), requested_topics)

    return {key: val for key, val in available_topics.items() if key in resolved_keys}
