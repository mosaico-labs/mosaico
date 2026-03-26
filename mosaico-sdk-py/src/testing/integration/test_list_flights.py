from mosaicolabs.comm import MosaicoClient

from .config import QUERY_SEQUENCES_MOCKUP, UPLOADED_SEQUENCE_NAME


def test_list_sequences(
    mosaico_client: MosaicoClient,
    inject_mockup_sequences,
    inject_synthetic_sequence,
    # we do not know when the test is executed,
    # so we ensure all the sequences are available on the server
):
    """Test the retrieval of sequences from the Mosaico server."""

    # We expect to retrieve all the sequences correctly pushed on server (and only them)
    expected_sequences_list = list(QUERY_SEQUENCES_MOCKUP.keys()) + [
        UPLOADED_SEQUENCE_NAME
    ]
    slist = mosaico_client.list_sequences()

    assert len(slist) == len(expected_sequences_list)
    assert all([sname in expected_sequences_list for sname in slist])

    # free resources
    mosaico_client.close()
