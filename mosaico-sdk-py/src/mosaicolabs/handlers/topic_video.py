"""
Topic Video Module.

The store optimizer turns the images of an image sequence topic (the
`ImageSequence` and `ImageSequenceRaw` serialization formats) into a lossless
FFV1 video. This module reads that video back as MPEG-DASH: a manifest, an
initialization segment and a media segment per second of video.

Use [`TopicHandler.get_video()`][mosaicolabs.handlers.TopicHandler.get_video]
to retrieve it.
"""

from dataclasses import dataclass, field
from pathlib import Path
from typing import Dict, List, Optional, Union

import pyarrow as pa
import pyarrow.flight as fl

from mosaicolabs.proto.v1 import flight_pb2

from ..comm.connection import ConnectionContext
from ..helpers import pack_topic_resource_name

MANIFEST_NAME = "manifest.mpd"
"""The name of the MPEG-DASH manifest."""
INIT_SEGMENT_NAME = "init.mp4"
"""The name of the initialization segment."""


class VideoUnavailableError(Exception):
    """
    Raised when the video of a topic can't be retrieved: the topic is not an
    image sequence, or the store optimizer has not turned its images into a
    video yet. Its images can still be read with
    [`TopicHandler.get_data_streamer()`][mosaicolabs.handlers.TopicHandler.get_data_streamer].
    """


@dataclass(frozen=True)
class VideoSegment:
    """
    A media segment of a [`TopicVideo`][mosaicolabs.handlers.TopicVideo]: the
    frames of a second of video, every one a keyframe.
    """

    name: str
    """The file name, `segment-<t>.m4s`, `<t>` the time of its first frame in microseconds."""
    data: bytes
    """The segment, to append to the initialization segment to decode it."""
    timestamp_ns: List[int]
    """The `timestamp_ns` of each frame, as its message was recorded."""
    capture_timestamp_ns: List[int]
    """The capture time of each frame."""
    video_timestamp_us: List[int]
    """The time of each frame in the video, in microseconds from the first frame."""


@dataclass(frozen=True)
class TopicVideo:
    """
    The video of an image sequence topic, as MPEG-DASH.

    Decode it with FFmpeg or a tool built on it, such as PyAV: browsers and most
    DASH players do not support FFV1.

    Example:
        ```python
        from mosaicolabs import MosaicoClient

        with MosaicoClient.connect("localhost", 6726) as client:
            top_handler = client.topic_handler("mission_alpha", "/camera/image_raw")
            video = top_handler.get_video()
            print(f"{video.frames} frames in {len(video.segments)} segments")

            # The manifest, the initialization segment and the media segments
            video.save("video_dash")
            # A single MP4 file
            Path("video.mp4").write_bytes(video.to_mp4())
        ```
    """

    codec: str
    """The video codec, `ffv1`."""
    content_type: str
    """The content type of the segments, `video/mp4`."""
    frames: int
    """The number of frames of the whole video."""
    capture_start_ns: int
    """The capture time of the first frame, which the video's timestamps start from."""
    manifest: bytes
    """The MPEG-DASH manifest (`manifest.mpd`), with a timeline of the media segments."""
    init_segment: bytes
    """The initialization segment (`init.mp4`)."""
    segments: List[VideoSegment] = field(default_factory=list)
    """The media segments, in order."""

    def to_mp4(self) -> bytes:
        """
        Returns the frames of the segments as a single MP4 file: the
        initialization segment followed by the media segments.
        """
        return self.init_segment + b"".join(s.data for s in self.segments)

    def save(self, directory: Union[str, Path]) -> List[Path]:
        """
        Writes the manifest, the initialization segment and the media segments
        into `directory`, creating it if needed.

        Returns:
            List[Path]: The written files, in order.
        """
        directory = Path(directory)
        directory.mkdir(parents=True, exist_ok=True)
        files = [(MANIFEST_NAME, self.manifest), (INIT_SEGMENT_NAME, self.init_segment)]
        files += [(s.name, s.data) for s in self.segments]
        paths = []
        for name, data in files:
            path = directory / name
            path.write_bytes(data)
            paths.append(path)
        return paths


def _video_request(
    start_s: Optional[int], end_s: Optional[int]
) -> flight_pb2.ImageSequenceRequest:
    if (start_s is None) != (end_s is None):
        raise ValueError("Set both 'start_s' and 'end_s', or neither of them.")
    video = flight_pb2.VideoView()
    if start_s is not None and end_s is not None:
        if not 0 <= start_s < end_s:
            raise ValueError(f"Invalid interval of seconds [{start_s}, {end_s}).")
        video.interval.CopyFrom(flight_pb2.VideoInterval(start_s=start_s, end_s=end_s))
    return flight_pb2.ImageSequenceRequest(video=video)


def _grpc_status(error: Exception) -> str:
    """The gRPC status of a Flight error, as PyArrow words it in its message."""
    message = str(error).lower()
    if "precondition failed" in message or "failed_precondition" in message:
        return "failed_precondition"
    if "invalid argument" in message or "invalid_argument" in message:
        return "invalid_argument"
    return ""


def _read_video(
    connection: ConnectionContext,
    sequence_name: str,
    topic_name: str,
    start_s: Optional[int],
    end_s: Optional[int],
) -> TopicVideo:
    """Retrieves the video of a topic. See `TopicHandler.get_video()`."""
    cmd = flight_pb2.GetFlightInfoCmd(
        locator=pack_topic_resource_name(sequence_name, topic_name),
        image_sequence=_video_request(start_s, end_s),
    )
    client = connection.flight_client
    try:
        flight_info = client.get_flight_info(
            fl.FlightDescriptor.for_command(cmd.SerializeToString())
        )
        if not flight_info.endpoints:
            raise ValueError(f"No endpoint for topic '{topic_name}'.")
        table = client.do_get(flight_info.endpoints[0].ticket).read_all()
    except (fl.FlightError, pa.ArrowException) as e:
        status = _grpc_status(e)
        if status == "invalid_argument":
            raise ValueError(
                f"Invalid request for the video of topic '{topic_name}': {e}"
            ) from e
        if status == "failed_precondition":
            raise VideoUnavailableError(
                f"The video of topic '{topic_name}' of sequence '{sequence_name}' "
                f"can't be retrieved: {e}"
            ) from e
        raise ConnectionError(
            f"Server error while retrieving the video of topic '{topic_name}', '{e}'"
        ) from e

    return _assemble(table)


def _assemble(table: pa.Table) -> TopicVideo:
    """Joins the pieces of each file of the video, in the order they came."""
    metadata = {
        k.decode(): v.decode() for k, v in (table.schema.metadata or {}).items()
    }
    files: Dict[str, bytearray] = {}
    lists: Dict[str, Dict[str, List[int]]] = {}
    order: List[str] = []
    for row in table.to_pylist():
        name = row["name"]
        if name not in files:
            files[name] = bytearray()
            order.append(name)
        files[name] += row["data"]
        if row["timestamp_ns"] is not None:
            lists[name] = {
                "timestamp_ns": row["timestamp_ns"],
                "capture_timestamp_ns": row["capture_timestamp_ns"],
                "video_timestamp_us": row["video_timestamp_us"],
            }

    segments = [
        VideoSegment(name=name, data=bytes(files[name]), **lists.get(name, _no_lists()))
        for name in order
        if name not in (MANIFEST_NAME, INIT_SEGMENT_NAME)
    ]
    return TopicVideo(
        codec=metadata.get("mosaico:codec", ""),
        content_type=metadata.get("mosaico:content_type", ""),
        frames=int(metadata.get("mosaico:frames", 0)),
        capture_start_ns=int(metadata.get("mosaico:capture_start_ns", 0)),
        manifest=bytes(files.get(MANIFEST_NAME, b"")),
        init_segment=bytes(files.get(INIT_SEGMENT_NAME, b"")),
        segments=segments,
    )


def _no_lists() -> Dict[str, List[int]]:
    return {"timestamp_ns": [], "capture_timestamp_ns": [], "video_timestamp_us": []}
