import fnmatch
from typing import Any, Dict, List, Optional, TypeGuard

import numpy as np
from rosbags.interfaces import TopicInfo

from mosaicolabs import SequenceHandler, TopicHandler
from mosaicolabs.logging_config import get_logger

# Set the hierarchical logger
logger = get_logger(__name__)


def _validate_sequence(
    seq_handler: Optional[SequenceHandler],
) -> TypeGuard[SequenceHandler]:
    if seq_handler is None:
        return False
    return True


def _clip_timestamp(
    start_ns: Optional[int],
    end_ns: Optional[int],
    min_ns: Optional[int],
    max_ns: Optional[int],
) -> tuple[Optional[int], Optional[int]]:

    if start_ns is not None and min_ns is not None and start_ns < min_ns:
        logger.warning(
            f"Provided start_timestamp_ns is lower than sequence timestamp_ns_min: {start_ns} < {min_ns}. Clipping start_timestamp_ns to sequence timestamp_ns_min"
        )
        start_ns = max(start_ns, min_ns)

    if end_ns is not None and max_ns is not None and end_ns > max_ns:
        logger.warning(
            f"Provided end_timestamp_ns is higher than sequence timestamp_ns_max: {end_ns} > {max_ns}. Clipping end_timestamp_ns to sequence timestamp_ns_max"
        )
        end_ns = min(end_ns, max_ns)

    return start_ns, end_ns


def _extract_ros_metadata(t_handler: TopicHandler) -> Dict[str, Any]:
    """
    Reads and validates the ``_ros_`` metadata field, if present.

    Each of ``msgtype``, ``msgdef`` and ``enums`` is validated independently:
    a field that's simply absent is left out of the returned metadata (it's
    the caller's responsibility to decide whether it needed that field), while
    a field that's present but holds an unexpected type raises immediately.

    Args:
        t_handler (TopicHandler): The topic handler whose metadata should be inspected.

    Returns:
        Dict[str, Any]: The declared ROS metadata, or an empty dict if the topic carries no
            ``_ros_`` metadata at all.

    Raises:
        TypeError: When the topic's ``_ros_`` metadata carries malformed metadata.
    """
    ros_metadata = t_handler.user_metadata.get("_ros_")

    if not ros_metadata:
        return {}

    msgtype = ros_metadata.get("msgtype")
    msgdef = ros_metadata.get("msgdef")
    msgconst = ros_metadata.get("enums", {})

    if msgtype is not None and not isinstance(msgtype, str):
        raise TypeError(
            f"Topic {t_handler.name} contains msgtype within metadata but it has unexpected type. Expected {str.__name__} but got {type(msgtype).__name__}"
        )

    if msgdef is not None and not isinstance(msgdef, str):
        raise TypeError(
            f"Topic {t_handler.name} contains msgdef within metadata but it has unexpected type. Expected {str.__name__} but got {type(msgdef).__name__}"
        )

    if not isinstance(msgconst, dict):
        raise TypeError(
            f"Topic {t_handler.name} contains enums within metadata but it has unexpected type. Expected {dict.__name__} but got {type(msgconst).__name__}"
        )

    return ros_metadata


def _to_dict(message: Any) -> tuple[Any, Dict[str, Any]]:
    """
    Recursively converts a rosbags message object and its nested fields to a standard
    Python dictionary (or list/primitive type if encountered during recursion), splitting
    out message constants (`UPPER_CASE` attributes, as opposed to `snake_case` fields)
    along the way.

    Returns:
        tuple[Any, Dict[str, Any]]: A `(value, const_dict)` tuple. `const_dict` holds the `UPPER_CASE` constants
            declared directly on `message` and is empty for lists/tuples/arrays/time values,
            which never carry constants of their own. Callers recursing into nested fields
            should keep only `value` and discard `const_dict`, so constants nested below the
            top level of the original call are dropped rather than collected.
    """
    if hasattr(message, "__msgtype__"):
        data_dict: Dict[str, Any] = {}
        const_dict: Dict[str, Any] = {}
        # rosbags messages are dataclasses without __slots__. Iterating the
        # declared dataclass fields is ~3.5x faster than scanning dir(message)
        # (which enumerates and filters the whole attribute space on every
        # nested object) and yields identical data.
        fields = getattr(message, "__slots__", None)
        if fields is None:
            dataclass_fields = getattr(message, "__dataclass_fields__", None)
            if dataclass_fields is not None:
                fields = list(dataclass_fields.keys())
            else:
                # dunders and __msgtype__ are filtered by the loop below.
                fields = dir(message)
        for field_name in fields:
            if field_name.startswith("_") or field_name == "__msgtype__":
                continue
            try:
                field_value = getattr(message, field_name)
            except AttributeError:
                continue
            if field_name.isupper():
                const_dict[field_name] = field_value
                continue
            data_dict[field_name], _ = _to_dict(field_value)
        return data_dict, const_dict
    elif isinstance(message, (list, tuple)):
        return [_to_dict(item)[0] for item in message], {}
    elif isinstance(message, np.ndarray):
        return message.tolist(), {}
    elif hasattr(message, "sec") and hasattr(message, "nanosec"):
        try:
            # Convert ROS time structure to a single float timestamp (seconds)
            return message.sec + message.nanosec * 1e-9, {}
        except Exception:
            return message, {}
    return message, {}


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


def _class_name_from_ros_msgtype(ros_msgtype: str):
    return ros_msgtype.split("/")[-1]
