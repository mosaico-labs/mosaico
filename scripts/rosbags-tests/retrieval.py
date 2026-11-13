"""
Retrieves the image sequences of the rosbags with raw images, through the SDK.

Only the rosbags with `sensor_msgs/Image` topics are considered: the SDK stores
them as `ImageSequenceRaw` topics, which the store optimizer turns into
videos. Each such topic is read back as its images, saved as PNGs, and asked
for its video:

- optimized: the rosbags are ingested and optimized first, as the optimize
  script does. The video is saved as MPEG-DASH files and as a single MP4, and
  its frames must be the images, with the same timestamps.
- not optimized: the rosbags are only ingested, as if the store optimizer had
  failed. The images must still be readable, while the video must be refused.

The files are saved in `{output}/<sequence>/<topic>/`, replaced at each run.
"""

# Run through `filesystem/sequence_optimized.py`, `filesystem/sequence_not_optimized.py`
# or their `s3/` counterparts, which select the store.

import logging
import shutil
import time
from dataclasses import dataclass, field
from pathlib import Path

from mcap.reader import make_reader
from mosaicolabs import ImageSequenceRaw
from mosaicolabs.enum import SerializationFormat
from mosaicolabs.handlers import TopicHandler, VideoUnavailableError
from rich import box
from rich.table import Table

from common import (
    Environment,
    WIPE_HINT,
    Store,
    console,
    die,
    environment,
    error,
    find_rosbags,
    info,
    parse_arguments,
    sequence_name,
    success,
)
from ingestion import ingest_rosbags
from optimization import ingest_and_optimize, topics

OUTPUT_DIR = Path("/tmp/mosaico-rosbags-tests-retrieval")

# The SDK logs each closed topic reader, which would break the tables.
logging.getLogger("mosaicolabs.handlers.topic_reader").setLevel(logging.WARNING)

# The ROS message types the SDK stores as `ImageSequenceRaw`.
RAW_IMAGE_TYPES = {"sensor_msgs/msg/Image", "sensor_msgs/Image"}


def output_dir(store: Store, optimized: bool) -> Path:
    return OUTPUT_DIR / ("optimized" if optimized else "not-optimized") / store.value


def has_raw_images(bag: Path) -> bool:
    """Whether the rosbag has `sensor_msgs/Image` topics."""
    if bag.suffix != ".mcap":
        from rosbags.highlevel import AnyReader

        with AnyReader([bag]) as reader:
            return any(c.msgtype in RAW_IMAGE_TYPES for c in reader.connections)
    with bag.open("rb") as f:
        summary = make_reader(f).get_summary()
    return summary is not None and any(
        s.name in RAW_IMAGE_TYPES for s in summary.schemas.values()
    )


def rosbags_with_raw_images(bag: Path | None) -> list[Path]:
    bags = [b for b in find_rosbags(bag) if has_raw_images(b)]
    if not bags:
        die(f"No rosbag with {', '.join(sorted(RAW_IMAGE_TYPES))} topics.")
    return bags


def image_sequence_topics(env: Environment, sequence: str) -> list[TopicHandler]:
    """The `ImageSequenceRaw` topics of a sequence."""
    handler = env.client.sequence_handler(sequence)
    if handler is None:
        die(f"Sequence '{sequence}' not found.")
    result = []
    for name in sorted(handler.topics):
        topic = handler.get_topic_handler(name)
        if topic and topic.serialization_format is SerializationFormat.ImageSequenceRaw:
            result.append(topic)
    return result


@dataclass
class Images:
    count: int = 0
    # The size of the raw images received.
    size: int = 0
    seconds: float = 0.0
    timestamp_ns: list[int] = field(default_factory=list)
    capture_timestamp_ns: list[int] = field(default_factory=list)


def save_images(topic: TopicHandler, directory: Path) -> Images:
    """Reads the images of a topic and saves them as PNGs."""
    directory.mkdir(parents=True)
    images = Images()
    start = time.monotonic()
    for message in topic.get_data_streamer():
        image = message.get_data(ImageSequenceRaw)
        image.to_pillow().save(directory / f"{images.count:06d}.png", compress_level=1)
        images.count += 1
        images.size += len(image.data)
        images.timestamp_ns.append(message.timestamp_ns)
        images.capture_timestamp_ns.append(image.capture_timestamp_ns)
    topic.close()
    images.seconds = time.monotonic() - start
    return images


@dataclass
class Video:
    frames: int = 0
    segments: int = 0
    # The size of the MP4 file: the initialization segment and the media segments.
    size: int = 0
    seconds: float = 0.0
    # Why the server refused the video.
    refused: str | None = None
    # What differs between the frames of the video and the images.
    mismatches: list[str] = field(default_factory=list)


def save_video(topic: TopicHandler, directory: Path, images: Images) -> Video:
    """Retrieves the video of a topic, saves it and compares it to the images."""
    start = time.monotonic()
    try:
        video = topic.get_video()
    except VideoUnavailableError as e:
        return Video(refused=str(e), seconds=time.monotonic() - start)
    video.save(directory / "dash")
    mp4 = video.to_mp4()
    (directory / "video.mp4").write_bytes(mp4)
    result = Video(
        frames=video.frames,
        segments=len(video.segments),
        size=len(mp4),
        seconds=time.monotonic() - start,
    )

    timestamp_ns = [t for s in video.segments for t in s.timestamp_ns]
    capture_ns = [t for s in video.segments for t in s.capture_timestamp_ns]
    if video.frames != images.count:
        result.mismatches.append(f"{video.frames} frames, {images.count} images")
    if timestamp_ns != images.timestamp_ns:
        result.mismatches.append("the timestamps differ")
    if capture_ns != images.capture_timestamp_ns:
        result.mismatches.append("the capture timestamps differ")
    return result


@dataclass
class TopicReport:
    name: str
    images: Images
    video: Video


def mib(size: int) -> str:
    return f"{size / 2**20:,.1f}"


def print_images(sequence: str, reports: list[TopicReport]):
    table = Table(title=f"{sequence}: image sequences", box=box.SIMPLE_HEAD)
    table.add_column("Topic", no_wrap=True)
    for column in ("Images", "Size (MiB)", "Time (s)"):
        table.add_column(column, justify="right", no_wrap=True)
    for r in reports:
        table.add_row(
            r.name, str(r.images.count), mib(r.images.size), f"{r.images.seconds:.1f}"
        )
    console.print()
    console.print(table)


def print_videos(sequence: str, reports: list[TopicReport], optimized: bool):
    table = Table(
        title=f"{sequence}: videos",
        caption="The frames of a video must be the images, with the same timestamps"
        if optimized
        else "The video of a topic not optimized must be refused",
        box=box.SIMPLE_HEAD,
    )
    table.add_column("Topic", no_wrap=True)
    if optimized:
        for column in ("Frames", "Segments", "Size (MiB)", "Time (s)"):
            table.add_column(column, justify="right", no_wrap=True)
    table.add_column("Check", no_wrap=True)

    for r in reports:
        v = r.video
        if not optimized:
            check = "[green]refused[/]" if v.refused else "[red]retrieved[/]"
            table.add_row(r.name, check)
        elif v.refused:
            table.add_row(r.name, "-", "-", "-", "-", "[red]refused[/]")
        else:
            check = (
                f"[red]{', '.join(v.mismatches)}[/]"
                if v.mismatches
                else "[green]matches the images[/]"
            )
            table.add_row(
                r.name,
                str(v.frames),
                str(v.segments),
                mib(v.size),
                f"{v.seconds:.1f}",
                check,
            )
    console.print()
    console.print(table)


def retrieve(env: Environment, sequences: list[str], optimized: bool) -> bool:
    """Retrieves the image sequences and their videos. Returns whether all checks passed."""
    root = output_dir(env.store, optimized)
    ok = True
    for sequence in sequences:
        reports = []
        for topic in image_sequence_topics(env, sequence):
            directory = root / sequence / topic.name.strip("/")
            shutil.rmtree(directory, ignore_errors=True)
            images = save_images(topic, directory / "images")
            video = save_video(topic, directory, images)
            reports.append(TopicReport(topic.name, images, video))

        print_images(sequence, reports)
        print_videos(sequence, reports, optimized)

        for r in reports:
            if r.images.count == 0:
                error(f"{sequence}{r.name}: no image retrieved.")
                ok = False
            if optimized and r.video.refused:
                error(f"{sequence}{r.name}: {r.video.refused}")
                ok = False
            if optimized and r.video.mismatches:
                ok = False
            if not optimized and not r.video.refused:
                error(
                    f"{sequence}{r.name}: the video of a topic not optimized was retrieved."
                )
                ok = False
        if not reports:
            error(f"Sequence '{sequence}' has no ImageSequenceRaw topic.")
            ok = False

    console.print()
    info(f"Retrieved files: {root}")
    return ok


def check_not_optimized(env: Environment, sequences: list[str]) -> bool:
    """Checks that the store optimizer has not run on the sequences yet."""
    optimized = sorted(
        locator
        for locator, t in topics(env.store).items()
        if t.optimized_ns and locator.split("/", 1)[0] in sequences
    )
    for locator in optimized:
        error(f"Topic '{locator}' is already optimized.")
    if optimized:
        error(
            f"Wipe the store to test sequences not optimized: {WIPE_HINT}."
        )
    return not optimized


def main_optimized(store: Store) -> int:
    bags = rosbags_with_raw_images(
        parse_arguments(f"{__doc__}\nStore: {store.location}")
    )
    names = [sequence_name(bag) for bag in bags]
    with environment(store) as env:
        ok = ingest_and_optimize(env, bags)
        ok = retrieve(env, names, optimized=True) and ok

    if ok:
        success("Image sequences and videos retrieved.")
        return 0
    return 1


def main_not_optimized(store: Store) -> int:
    bags = rosbags_with_raw_images(
        parse_arguments(f"{__doc__}\nStore: {store.location}")
    )
    names = [sequence_name(bag) for bag in bags]
    with environment(store) as env:
        ok = ingest_rosbags(env, bags) and check_not_optimized(env, names)
        if ok:
            ok = retrieve(env, names, optimized=False)

    if ok:
        success("Image sequences retrieved, videos refused.")
        return 0
    return 1
