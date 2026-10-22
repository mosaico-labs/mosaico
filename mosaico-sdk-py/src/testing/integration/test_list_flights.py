import pytest

from mosaicolabs.comm import MosaicoClient
from mosaicolabs.enum import SequenceOrder

from .config import (
    QUERY_SEQUENCES_MOCKUP,
    UPLOADED_MCAP_MIXED_SEQUENCE_NAME,
    UPLOADED_MCAP_PROTOBUF_SEQUENCE_NAME,
    UPLOADED_SEQUENCE_NAME,
)


def test_list_sequences(
    mosaico_client: MosaicoClient,
    inject_mockup_sequences,
    inject_synthetic_sequence,
    # we do not know when the test is executed,
    # so we ensure all the sequences are available on the server
):
    """Test the retrieval of sequences from the Mosaico server."""

    # We expect to retrieve all the sequences correctly pushed on server (and only them)
    expected_sequences_list = (
        list(QUERY_SEQUENCES_MOCKUP.keys())
        + [UPLOADED_SEQUENCE_NAME]
        + [UPLOADED_MCAP_PROTOBUF_SEQUENCE_NAME]
        + [UPLOADED_MCAP_MIXED_SEQUENCE_NAME]
    )
    slist = mosaico_client.list_sequences()

    assert len(slist) == len(expected_sequences_list)
    assert all([sname in expected_sequences_list for sname in slist])


def test_list_sequences_page_walk(
    mosaico_client: MosaicoClient,
    inject_mockup_sequences,
    inject_synthetic_sequence,
):
    """Walking page by page returns every sequence once, in both orders."""
    all_by_created_at = mosaico_client.list_sequences()

    for order_by, expected in [
        (SequenceOrder.CreatedAt, all_by_created_at),
        (SequenceOrder.Name, sorted(all_by_created_at)),
    ]:
        listed = []
        cursor = None
        while True:
            page = mosaico_client.list_sequences_page(
                limit=2, cursor=cursor, order_by=order_by
            )
            listed.extend(page.names)
            if page.next_cursor is None:
                break
            cursor = page.next_cursor

        assert listed == expected, order_by


def test_iter_sequences(
    mosaico_client: MosaicoClient,
    inject_mockup_sequences,
    inject_synthetic_sequence,
):
    """Iterating returns every sequence, whatever the page size."""
    assert list(mosaico_client.iter_sequences(page_size=2)) == (
        mosaico_client.list_sequences()
    )
    assert list(
        mosaico_client.iter_sequences(page_size=1, order_by=SequenceOrder.Name)
    ) == sorted(mosaico_client.list_sequences())


def test_iter_sequences_rejects_negative_page_size(mosaico_client: MosaicoClient):
    with pytest.raises(ValueError):
        next(mosaico_client.iter_sequences(page_size=-1))
