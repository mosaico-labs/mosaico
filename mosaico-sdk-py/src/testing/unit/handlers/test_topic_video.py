import pyarrow as pa
import pytest

from mosaicolabs.handlers.topic_video import (
    INIT_SEGMENT_NAME,
    MANIFEST_NAME,
    _assemble,
    _grpc_status,
    _video_request,
)

SCHEMA = pa.schema(
    [
        ("name", pa.utf8()),
        ("data", pa.binary()),
        ("last", pa.bool_()),
        ("timestamp_ns", pa.list_(pa.int64())),
        ("capture_timestamp_ns", pa.list_(pa.int64())),
        ("video_timestamp_us", pa.list_(pa.int64())),
    ],
    metadata={
        "mosaico:content_type": "video/mp4",
        "mosaico:codec": "ffv1",
        "mosaico:frames": "3",
        "mosaico:capture_start_ns": "1000",
    },
)


def _row(name, data, last, lists=None):
    ts, capture, video = lists or (None, None, None)
    return {
        "name": name,
        "data": data,
        "last": last,
        "timestamp_ns": ts,
        "capture_timestamp_ns": capture,
        "video_timestamp_us": video,
    }


def test_assemble_joins_the_pieces_of_each_file():
    table = pa.Table.from_pylist(
        [
            _row(MANIFEST_NAME, b"<MPD", False),
            _row(MANIFEST_NAME, b"/>", True),
            _row(INIT_SEGMENT_NAME, b"init", True),
            _row("segment-0.m4s", b"s0a", False, ([10, 20], [1000, 1100], [0, 100])),
            _row("segment-0.m4s", b"s0b", True),
            _row("segment-1000000.m4s", b"s1", True, ([30], [2000], [1000000])),
        ],
        schema=SCHEMA,
    )

    video = _assemble(table)

    assert video.codec == "ffv1"
    assert video.content_type == "video/mp4"
    assert video.frames == 3
    assert video.capture_start_ns == 1000
    assert video.manifest == b"<MPD/>"
    assert video.init_segment == b"init"
    assert [s.name for s in video.segments] == ["segment-0.m4s", "segment-1000000.m4s"]
    assert video.segments[0].data == b"s0as0b"
    assert video.segments[0].timestamp_ns == [10, 20]
    assert video.segments[0].capture_timestamp_ns == [1000, 1100]
    assert video.segments[1].video_timestamp_us == [1000000]
    assert video.to_mp4() == b"inits0as0bs1"


def test_save_writes_the_files_in_order(tmp_path):
    table = pa.Table.from_pylist(
        [
            _row(MANIFEST_NAME, b"mpd", True),
            _row(INIT_SEGMENT_NAME, b"init", True),
            _row("segment-0.m4s", b"s0", True, ([10], [1000], [0])),
        ],
        schema=SCHEMA,
    )

    paths = _assemble(table).save(tmp_path / "dash")

    assert [p.name for p in paths] == [
        MANIFEST_NAME,
        INIT_SEGMENT_NAME,
        "segment-0.m4s",
    ]
    assert (tmp_path / "dash" / "segment-0.m4s").read_bytes() == b"s0"


def test_video_request_without_interval_asks_for_the_whole_video():
    request = _video_request(None, None)
    assert request.WhichOneof("view") == "video"
    assert not request.video.HasField("interval")


def test_video_request_with_interval():
    request = _video_request(1, 3)
    assert request.video.interval.start_s == 1
    assert request.video.interval.end_s == 3


@pytest.mark.parametrize(
    "start_s, end_s", [(1, None), (None, 1), (2, 2), (3, 1), (-1, 1)]
)
def test_video_request_rejects_wrong_intervals(start_s, end_s):
    with pytest.raises(ValueError):
        _video_request(start_s, end_s)


@pytest.mark.parametrize(
    "message, status",
    [
        (
            "gRPC returned precondition failed error, with message: Unsupported retrieval",
            "failed_precondition",
        ),
        ("FAILED_PRECONDITION: Unsupported retrieval", "failed_precondition"),
        (
            "Flight returned invalid argument error, with message: Bad request",
            "invalid_argument",
        ),
        ("Flight returned unavailable error", ""),
    ],
)
def test_grpc_status(message, status):
    assert _grpc_status(Exception(message)) == status
