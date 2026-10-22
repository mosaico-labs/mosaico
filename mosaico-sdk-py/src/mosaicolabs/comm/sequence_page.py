"""
Paged listing of sequences, built on the `list_flights` endpoint.
"""

from dataclasses import dataclass

import pyarrow.flight as fl

from ..enum import SequenceOrder
from ..proto.v1 import flight_pb2


@dataclass(frozen=True)
class SequencePage:
    """
    A page of sequences.

    Attributes:
        names (list[str]): The sequence names in this page, in the requested order.
        next_cursor (bytes | None): Pass it as `cursor` to get the next page.
            `None` when there are no more pages. The cursor is meant to be stored
            and sent back unchanged. It carries the page's order, so `order_by` can
            be left out.
    """

    names: list[str]
    next_cursor: bytes | None


_ORDER_TO_PROTO = {
    SequenceOrder.CreatedAt: flight_pb2.SEQUENCE_ORDER_CREATED_AT,
    SequenceOrder.Name: flight_pb2.SEQUENCE_ORDER_NAME,
}


def _list_flights_criteria(
    limit: int,
    cursor: bytes | None,
    order_by: SequenceOrder | None,
) -> bytes:
    """Builds the `Criteria.expression` of a `list_flights` call."""
    if limit < 0:
        raise ValueError(f"limit must be >= 0, got {limit}")

    cmd = flight_pb2.ListFlightsCmd(limit=limit)

    if order_by is not None:
        cmd.order_by = _ORDER_TO_PROTO[order_by]

    if cursor is not None:
        cmd.cursor.CopyFrom(flight_pb2.SequenceCursor.FromString(cursor))

    return cmd.SerializeToString()


def _page_from_flight_infos(infos: list[fl.FlightInfo], limit: int) -> SequencePage:
    """
    Builds a [`SequencePage`][mosaicolabs.comm.SequencePage] from the response
    to a `list_flights` call made with `limit`.

    A page is the last when it is unlimited (`limit=0`) or shorter than `limit`.
    """
    is_last = limit == 0 or len(infos) < limit

    next_cursor = None
    if not is_last:
        metadata = flight_pb2.SequenceListItemAppMetadata.FromString(
            infos[-1].app_metadata
        )
        next_cursor = metadata.cursor.SerializeToString()

    # Each sequence name is carried in the descriptor path.
    names = [info.descriptor.path[0].decode("utf-8") for info in infos]

    return SequencePage(names=names, next_cursor=next_cursor)
