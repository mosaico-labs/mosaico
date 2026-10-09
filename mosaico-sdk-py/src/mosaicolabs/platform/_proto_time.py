"""
Conversions between `mosaico.v1.time.TimestampRange` (protobuf) and plain,
independently-optional `(start_ns, end_ns)` Python integers.

`TimestampRange.start_ns`/`end_ns` are each `optional int64`: an absent bound
means unbounded on that side, not zero. Mirrors `mosaicod-marshal`'s
`timestamp_range_to_proto`/`timestamp_range_from_proto` (Rust) exactly.
"""

from typing import Optional, Tuple

from mosaicolabs.proto.v1 import time_pb2


def to_proto(
    start_ns: Optional[int], end_ns: Optional[int]
) -> Optional[time_pb2.TimestampRange]:
    """
    Builds a `TimestampRange` message from independently-optional bounds.

    Args:
        start_ns (Optional[int]): The inclusive lower bound, or None for unbounded.
        end_ns (Optional[int]): The exclusive upper bound, or None for unbounded.

    Returns:
        Optional[time_pb2.TimestampRange]: None if both bounds are None (i.e. no
            range should be sent at all), otherwise a message with only the
            provided bound(s) set.
    """
    if start_ns is None and end_ns is None:
        return None

    msg = time_pb2.TimestampRange()
    if start_ns is not None:
        msg.start_ns = start_ns
    if end_ns is not None:
        msg.end_ns = end_ns
    return msg


def from_proto(
    msg: Optional[time_pb2.TimestampRange],
) -> Tuple[Optional[int], Optional[int]]:
    """
    Extracts independently-optional bounds from a `TimestampRange` message.

    Args:
        msg (Optional[time_pb2.TimestampRange]): The message, or None if absent.

    Returns:
        Tuple[Optional[int], Optional[int]]: The (start_ns, end_ns) bounds; a
            missing message or a missing side both resolve to None (unbounded).
    """
    if msg is None:
        return None, None

    start_ns = msg.start_ns if msg.HasField("start_ns") else None
    end_ns = msg.end_ns if msg.HasField("end_ns") else None
    return start_ns, end_ns
