import fnmatch
from typing import (
    List,
    Optional,
    TypeGuard,
)

from mosaicolabs import SequenceHandler
from mosaicolabs.logging_config import get_logger

# Set the hierarchical logger
logger = get_logger(__name__)


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


def _validate_sequence(
    seq_handler: Optional[SequenceHandler],
) -> TypeGuard[SequenceHandler]:
    if seq_handler is None:
        return False
    return True


def _filter_from_list(
    input_list: List[str], glob_stype_patterns: Optional[List[str]]
) -> List[str]:
    """
    This method filters `input_list` according to the patterns defined in
    `glob_stype_patterns`, using ORDER-DEPENDENT (gitignore-like) semantics.
    Pattern semantics:
        - Patterns use standard shell-style wildcards (via `fnmatch`):
            * "*" matches any sequence of characters
            * "?" matches any single character
        - Patterns NOT starting with "!" are treated as inclusion patterns.
        - Patterns starting with "!" are treated as exclusion patterns.

    Patterns are evaluated sequentially, and each pattern modifies the current
    selection of items. Evaluation rules:
        - Patterns are processed in the order they appear.
        - Each non-"!" pattern adds matching items to the result set.
        - Each "!" pattern removes matching items from the result set.
        - Later patterns override earlier ones.
        - If no inclusion pattern is present, the initial set is ALL available items,
          which are then filtered by subsequent exclusion patterns.

    Args:
        input_list (list[str]):
            List of topic names.
        glob_stype_patterns (Optional[List[str]]):
            Optional list of topic names or patterns to filter results.
            Only items matching any of the provided values will be returned.

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

    if not glob_stype_patterns:
        return input_list

    # # If there is at least one include pattern, we start empty.
    # # Otherwise we start from all topics (implicit include-all).
    has_include = any(not p.startswith("!") for p in glob_stype_patterns)

    if has_include:
        resolved_items = set()
    else:
        resolved_items = set(input_list)

    for pattern in glob_stype_patterns:
        exclude_me = pattern.startswith("!")
        raw_pattern = pattern[1:] if exclude_me else pattern

        matches = fnmatch.filter(input_list, raw_pattern)

        if not matches:
            logger.warning(f"Topic pattern '{pattern}' matched nothing in this bag.")
            continue

        match_set = set(matches)

        if exclude_me:
            resolved_items -= match_set
        else:
            resolved_items |= match_set

    return list(resolved_items)
