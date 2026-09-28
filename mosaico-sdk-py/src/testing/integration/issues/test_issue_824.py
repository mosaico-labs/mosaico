"""
Regression tests for issue #824:
`DataFrameExtractor.to_pandas_chunks()` omits the message at exactly `timestamp_ns_max`.
"""

from typing import List, Optional

import pytest

from mosaicolabs import Message, MosaicoClient, Temperature
from mosaicolabs.ml import DataFrameExtractor

SEQUENCE_NAME = "test_issue_824"
SINGLE_MSG_SEQUENCE_NAME = "test_issue_824_single"
TOPIC_A = "/temp_a"
TOPIC_B = "/temp_b"

T0_NS = 1_000_000_000
# Both topics end at the same (maximum) timestamp
TOPIC_A_TSTAMPS = [T0_NS + i * 10_000_000 for i in range(11)]  # 0 -> 100 ms, 10 ms step
TOPIC_B_TSTAMPS = [T0_NS + i * 20_000_000 for i in range(6)]  # 0 -> 100 ms, 20 ms step
TSTAMP_NS_MAX = T0_NS + 100_000_000
ALL_TSTAMPS = sorted(TOPIC_A_TSTAMPS + TOPIC_B_TSTAMPS)

# Chunked (non-dividing), chunked (dividing the range: the last message lies
# exactly on a window boundary) and full load
WINDOWS_SEC = [0.03, 0.05, 10.0]


def _push(client: MosaicoClient, sequence_name: str, topics: dict):
    with client.sequence_create(sequence_name, metadata={}) as swriter:
        for topic, tstamps in topics.items():
            twriter = swriter.topic_create(topic, metadata={}, ontology_type=Temperature)
            assert twriter is not None
            for ts in tstamps:
                twriter.push(
                    message=Message(timestamp_ns=ts, data=Temperature(value=300.0))
                )


def _delete_sequences(client: MosaicoClient):
    for sname in (SEQUENCE_NAME, SINGLE_MSG_SEQUENCE_NAME):
        try:
            client.sequence_delete(sname)
        except Exception:
            pass  # the sequence does not exist


@pytest.fixture(scope="module")
def issue_824_sequences(host, port, tls_cert_path, api_key_manage, compression):
    """
    Creates the sequences used by this module and ALWAYS deletes them at the end,
    so that they do not interfere with the other integration tests.
    """
    client = MosaicoClient.connect(
        host=host,
        port=port,
        tls_cert_path=tls_cert_path,
        api_key=api_key_manage,
        compression=compression,
    )
    # Remove leftovers of a previously interrupted run
    _delete_sequences(client)
    try:
        _push(
            client, SEQUENCE_NAME, {TOPIC_A: TOPIC_A_TSTAMPS, TOPIC_B: TOPIC_B_TSTAMPS}
        )
        _push(client, SINGLE_MSG_SEQUENCE_NAME, {TOPIC_A: [T0_NS]})

        yield
    finally:
        _delete_sequences(client)
        client.close()


def _extract_timestamps(
    client: MosaicoClient,
    sequence_name: str,
    window_sec: float,
    topics: Optional[List[str]] = None,
    timestamp_ns_start: Optional[int] = None,
    timestamp_ns_end: Optional[int] = None,
) -> List[int]:
    seqhandler = client.sequence_handler(sequence_name)
    assert seqhandler is not None

    tstamps = []
    for df in DataFrameExtractor(seqhandler).to_pandas_chunks(
        topics=topics,
        window_sec=window_sec,
        timestamp_ns_start=timestamp_ns_start,
        timestamp_ns_end=timestamp_ns_end,
    ):
        tstamps.extend(df["timestamp_ns"].tolist())
    return tstamps


def test_sequence_timestamp_ns_max(mosaico_client: MosaicoClient, issue_824_sequences):
    """Sanity check: the last pushed message is the sequence 'timestamp_ns_max'"""
    seqhandler = mosaico_client.sequence_handler(SEQUENCE_NAME)
    assert seqhandler is not None
    assert seqhandler.timestamp_ns_max == TSTAMP_NS_MAX
    mosaico_client.close()


@pytest.mark.parametrize("window_sec", WINDOWS_SEC)
@pytest.mark.parametrize(
    "timestamp_ns_end",
    [None, TSTAMP_NS_MAX + 1, TSTAMP_NS_MAX * 2],
    ids=["unbounded", "max_plus_one", "beyond_max"],
)
def test_end_at_or_beyond_max_includes_last_message(
    mosaico_client: MosaicoClient,
    issue_824_sequences,
    window_sec: float,
    timestamp_ns_end: Optional[int],
):
    """
    Extracting up to (or beyond) the end of the sequence must include the messages
    at `timestamp_ns_max` (of every topic ending there): this covers the clamping path.
    """
    tstamps = _extract_timestamps(
        mosaico_client,
        SEQUENCE_NAME,
        window_sec=window_sec,
        timestamp_ns_end=timestamp_ns_end,
    )

    assert tstamps == ALL_TSTAMPS
    assert tstamps.count(TSTAMP_NS_MAX) == 2
    mosaico_client.close()


@pytest.mark.parametrize("window_sec", WINDOWS_SEC)
@pytest.mark.parametrize("topic", [TOPIC_A, TOPIC_B])
def test_single_topic_beyond_max_includes_last_message(
    mosaico_client: MosaicoClient,
    issue_824_sequences,
    window_sec: float,
    topic: str,
):
    """Same as above, with a topic selection"""
    tstamps = _extract_timestamps(
        mosaico_client,
        SEQUENCE_NAME,
        window_sec=window_sec,
        topics=[topic],
        timestamp_ns_start=0,
        timestamp_ns_end=TSTAMP_NS_MAX * 2,
    )

    expected = TOPIC_A_TSTAMPS if topic == TOPIC_A else TOPIC_B_TSTAMPS
    assert tstamps == expected
    mosaico_client.close()


@pytest.mark.parametrize("window_sec", WINDOWS_SEC)
def test_end_equal_to_max_is_exclusive(
    mosaico_client: MosaicoClient,
    issue_824_sequences,
    window_sec: float,
):
    """An explicit end within the sequence range keeps the exclusive semantics (t < end)"""
    tstamps = _extract_timestamps(
        mosaico_client,
        SEQUENCE_NAME,
        window_sec=window_sec,
        timestamp_ns_end=TSTAMP_NS_MAX,
    )

    assert tstamps == [ts for ts in ALL_TSTAMPS if ts < TSTAMP_NS_MAX]
    mosaico_client.close()


@pytest.mark.parametrize("window_sec", WINDOWS_SEC)
def test_start_at_max_returns_last_messages(
    mosaico_client: MosaicoClient,
    issue_824_sequences,
    window_sec: float,
):
    """A zero-length range starting at `timestamp_ns_max` returns the last messages"""
    tstamps = _extract_timestamps(
        mosaico_client,
        SEQUENCE_NAME,
        window_sec=window_sec,
        timestamp_ns_start=TSTAMP_NS_MAX,
    )

    assert tstamps == [TSTAMP_NS_MAX, TSTAMP_NS_MAX]
    mosaico_client.close()


@pytest.mark.parametrize("window_sec", WINDOWS_SEC)
def test_single_message_sequence(
    mosaico_client: MosaicoClient,
    issue_824_sequences,
    window_sec: float,
):
    """A sequence with `timestamp_ns_min == timestamp_ns_max` must return its only message"""
    tstamps = _extract_timestamps(
        mosaico_client,
        SINGLE_MSG_SEQUENCE_NAME,
        window_sec=window_sec,
        timestamp_ns_end=T0_NS * 2,
    )

    assert tstamps == [T0_NS]
    mosaico_client.close()
