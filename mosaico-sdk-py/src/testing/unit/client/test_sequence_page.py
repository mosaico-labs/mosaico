import pyarrow as pa
import pyarrow.flight as fl
import pytest

from mosaicolabs.comm.sequence_page import (
    SequencePage,
    _list_flights_criteria,
    _page_from_flight_infos,
)
from mosaicolabs.enum import SequenceOrder
from mosaicolabs.proto.v1 import flight_pb2


def _name_cursor(name: str) -> flight_pb2.SequenceCursor:
    return flight_pb2.SequenceCursor(name=name)


def _flight_info(name: str) -> fl.FlightInfo:
    """A `list_flights` item, shaped like the server's."""
    app_metadata = flight_pb2.SequenceListItemAppMetadata(cursor=_name_cursor(name))
    return fl.FlightInfo(
        pa.schema([]),
        fl.FlightDescriptor.for_path(name),
        [fl.FlightEndpoint(name, [])],
        -1,
        -1,
        app_metadata=app_metadata.SerializeToString(),
    )


def test_criteria_rejects_negative_limit():
    with pytest.raises(ValueError):
        _list_flights_criteria(-1, None, SequenceOrder.CreatedAt)


def test_full_page_has_next_cursor():
    page = _page_from_flight_infos([_flight_info("a"), _flight_info("b")], limit=2)

    assert page.names == ["a", "b"]
    assert page.next_cursor == _name_cursor("b").SerializeToString()


def test_short_page_is_the_last():
    page = _page_from_flight_infos([_flight_info("a")], limit=2)

    assert page == SequencePage(names=["a"], next_cursor=None)


def test_unlimited_page_is_the_last():
    page = _page_from_flight_infos([_flight_info("a"), _flight_info("b")], limit=0)

    assert page == SequencePage(names=["a", "b"], next_cursor=None)


def test_server_default_limit_page_has_next_cursor():
    """Without an explicit limit, a non-empty page may not be the last."""
    page = _page_from_flight_infos([_flight_info("a")], limit=None)

    assert page.next_cursor == _name_cursor("a").SerializeToString()


def test_empty_page_is_the_last():
    page = _page_from_flight_infos([], limit=None)

    assert page == SequencePage(names=[], next_cursor=None)
