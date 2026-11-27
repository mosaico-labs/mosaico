from pathlib import Path
from typing import List, Optional, Sequence, cast

import pyarrow as pa
import pyarrow.parquet as pq
import pytest
from PIL import Image

from mosaicolabs import CompressedImage
from mosaicolabs.bridges.lerobot.metadata import (
    LeRobotEpisode,
    LeRobotFeature,
    LeRobotInfo,
)
from mosaicolabs.bridges.lerobot.mosaico_loader import LeRobotEpisodeStreamer
from mosaicolabs.bridges.lerobot.writer import (
    _TMP_SUFFIX,
    LeRobotWriter,
    _next_file_index,
    _tmp_path,
    _to_image_column,
    _to_task_index_column,
    _to_timeseries_column,
)
from mosaicolabs.bridges.protocols.lerobot import LeRobotSchemaConverter
from mosaicolabs.models.sensors.image import ImageFormat
from testing.unit.bridges.lerobot.episode_streamer_fakes import episode_streamer

TASKS = ["Pick up the cube", "Place the cube"]


def _timeseries(task: pa.Array) -> pa.RecordBatch:
    """A batch of the timeseries topic, with its `task` field."""

    data = pa.array([1.0] * len(task))

    observation = pa.StructArray.from_arrays([data, data], names=["state", "velocity"])

    return pa.RecordBatch.from_pydict(
        {
            "observation": observation,
            "action": data,
            "task": task,
        }
    )


# --- _to_timeseries_column ---


def test_timeseries_column_flattens_nested_struct():

    task = pa.array([TASKS[1], TASKS[0], TASKS[1]], pa.string())
    data = pa.array([1.0] * len(task))

    assert _to_timeseries_column(_timeseries(task), "observation.state").equals(data)
    assert _to_timeseries_column(_timeseries(task), "observation.velocity").equals(data)
    assert _to_timeseries_column(_timeseries(task), "action").equals(data)


def test_timeseries_column_missing_key():

    task = pa.array([TASKS[1], TASKS[0], TASKS[1]], pa.string())

    with pytest.raises(ValueError, match="has no field 'not existing'"):
        _to_timeseries_column(_timeseries(task), "not existing")


# --- _to_task_index_column ---


@pytest.mark.parametrize("task_type", [pa.string(), pa.string_view()])
def test_task_index_column_is_the_position_of_each_task(task_type):
    """As LeRobot reads it: the row of the task in `meta/tasks.parquet`. Mosaico can stream
    the tasks as a `string_view`."""
    task = pa.array([TASKS[1], TASKS[0], TASKS[1]], task_type)

    task_index = _to_task_index_column(_timeseries(task), TASKS)

    assert task_index.to_pylist() == [1, 0, 1]


@pytest.mark.parametrize("unknown_task", ["Push the cube", None])
def test_task_index_column_rejects_a_task_not_in_the_tasks(unknown_task):
    task = pa.array([TASKS[0], unknown_task], pa.string())

    with pytest.raises(ValueError, match="not defined in the exported dataset"):
        _to_task_index_column(_timeseries(task), TASKS)


def test_task_index_column_requires_the_task_field():
    timeseries = pa.record_batch({"action": pa.array([1.0])})

    with pytest.raises(ValueError, match="has no field 'task'"):
        _to_task_index_column(timeseries, TASKS)


# --- _to_image_column ---


def _compressed_image(image_format: ImageFormat) -> CompressedImage:
    """A 2x2 image, encoded in `image_format`."""
    return CompressedImage.from_image(Image.new("RGB", (2, 2)), format=image_format)


def _camera_batch(
    images: List[CompressedImage],
    data_type: pa.DataType = pa.binary(),
    format_type: pa.DataType = pa.string(),
) -> pa.RecordBatch:
    """A batch of a camera topic, as streamed from Mosaico (without `timestamp_ns`): the
    images serialized as the `CompressedImage` ontology. Mosaico can stream `data` and
    `format` as views (`data_type`, `format_type`)."""
    batch = pa.RecordBatch.from_pylist(
        [image._encode() for image in images],
        schema=pa.schema(CompressedImage.__msco_pyarrow_struct__),
    )
    return batch.cast(
        pa.schema(
            [
                batch.schema.field("header"),
                batch.schema.field("data").with_type(data_type),
                batch.schema.field("format").with_type(format_type),
            ]
        )
    )


@pytest.mark.parametrize(
    "data_type, format_type",
    [(pa.binary(), pa.string()), (pa.binary_view(), pa.string_view())],
)
def test_image_column_rebuilds_the_path_from_the_frame_index(data_type, format_type):
    """`data` is kept as it is, and `path` is rebuilt as LeRobot names the images it embeds."""
    images = [_compressed_image(ImageFormat.PNG), _compressed_image(ImageFormat.JPEG)]

    column = _to_image_column(
        _camera_batch(images, data_type, format_type), pa.array([0, 12])
    )

    assert column.type.equals(LeRobotSchemaConverter.IMAGE_ARROW_TYPE)
    assert column.to_pylist() == [
        {"bytes": images[0].data, "path": "frame-000000.png"},
        {"bytes": images[1].data, "path": "frame-000012.jpeg"},
    ]


def test_image_column_is_null_for_a_frame_without_an_image():
    """A frame without an image is ingested with an empty `data`."""
    image = _compressed_image(ImageFormat.PNG)
    no_image = CompressedImage(data=b"", format=ImageFormat.PNG)

    column = _to_image_column(_camera_batch([image, no_image]), pa.array([0, 1]))

    assert column.to_pylist() == [
        {"bytes": image.data, "path": "frame-000000.png"},
        None,
    ]


# --- _to_lerobot_rb and write_episode ---

FEATURES = [
    LeRobotFeature("timestamp", "float32", (1,)),
    LeRobotFeature("frame_index", "int64", (1,)),
    LeRobotFeature("episode_index", "int64", (1,)),
    LeRobotFeature("index", "int64", (1,)),
    LeRobotFeature("task_index", "int64", (1,)),
    LeRobotFeature("action", "float32", (2,)),
]
"""The features of the exported dataset: the generated columns, `task_index` and a
timeseries."""

TIMESERIES_TOPIC = "/timeseries"

DATA_PATH = "data/chunk-{chunk_index:03d}/file-{file_index:03d}.parquet"
"""The `data_path` template of LeRobot v3.0."""


def _info(
    chunks_size: Optional[int] = None, data_files_size_in_mb: Optional[int] = None
) -> LeRobotInfo:
    """The `info` of the exported dataset, with `FEATURES`."""
    return LeRobotInfo(
        codebase_version="v3.0",
        fps=30,
        features=FEATURES,
        total_episodes=1,
        data_path=DATA_PATH,
        chunks_size=chunks_size,
        data_files_size_in_mb=data_files_size_in_mb,
    )


def _episode(
    episode_index: int, dataset_from_index: int, length: int
) -> LeRobotEpisode:
    return LeRobotEpisode(
        episode_index=episode_index,
        tasks=[TASKS[0]],
        length=length,
        data_chunk_index=0,
        data_file_index=0,
        dataset_from_index=dataset_from_index,
        dataset_to_index=dataset_from_index + length,
    )


def _timeseries_slice(timestamps_ns: Sequence[int]) -> pa.RecordBatch:
    """The rows of the timeseries topic at `timestamps_ns`, with `timestamp_ns`, as streamed
    from Mosaico."""
    return pa.record_batch(
        {
            "timestamp_ns": pa.array(timestamps_ns, pa.int64()),
            "action": pa.array([[float(t), 0.0] for t in timestamps_ns]),
            "task": pa.array([TASKS[0]] * len(timestamps_ns)),
        }
    )


def _timeseries_slices(n_rows: int, sizes: List[int]) -> List[pa.RecordBatch]:
    """`n_rows` rows of the timeseries topic, one per 1 ms, split into slices of `sizes`."""
    assert sum(sizes) == n_rows
    slices, start = [], 0
    for size in sizes:
        slices.append(
            _timeseries_slice(range(start * 10**6, (start + size) * 10**6, 10**6))
        )
        start += size
    return slices


def _convert(
    writer: LeRobotWriter, episode: LeRobotEpisode, rows: int
) -> pa.RecordBatch:
    """Converts a slice of `rows` rows of `episode`, as `LeRobotEpisodeStreamer` yields it."""
    batch = _timeseries_slice(range(rows))
    return writer._to_lerobot_rb(
        episode,
        batch.column("timestamp_ns"),
        {TIMESERIES_TOPIC: batch.drop_columns(["timestamp_ns"])},
    )


def test_lerobot_rb_generates_the_indexes_of_the_exported_dataset():
    """`index` follows the rows of the dataset, `frame_index` restarts at each episode, and
    `episode_index` is the one of the episode, renumbered."""
    writer = LeRobotWriter(_info(), Path("unused"), TASKS)
    first, second = _episode(0, 0, 3), _episode(1, 3, 2)

    slices = [
        _convert(writer, first, 2),
        _convert(writer, first, 1),
        _convert(writer, second, 2),
    ]

    columns = pa.Table.from_batches(slices).to_pydict()
    assert columns["index"] == [0, 1, 2, 3, 4]
    assert columns["frame_index"] == [0, 1, 2, 0, 1]
    assert columns["episode_index"] == [0, 0, 0, 1, 1]
    assert columns["task_index"] == [0] * 5


@pytest.mark.parametrize(
    "episode, rows",
    [
        (_episode(0, 0, 3), 4),  # More rows than the episode length
        (_episode(1, 3, 2), 2),  # An episode converted before the previous one
    ],
    ids=["past-the-end", "out-of-order"],
)
def test_lerobot_rb_rejects_rows_outside_the_episode(episode, rows):
    writer = LeRobotWriter(_info(), Path("unused"), TASKS)

    with pytest.raises(
        ValueError, match=f"do not fall within episode {episode.episode_index}"
    ):
        _convert(writer, episode, rows)


def test_write_episode_rejects_an_episode_ending_before_its_dataset_to_index(tmp_path):
    """An episode of 5 rows whose metadata end it at row 6: its rows are written, then it is
    rejected, and no data file is left half-written."""
    data_path = tmp_path / "data" / "chunk-000" / "file-000.parquet"
    streamer = episode_streamer(
        {TIMESERIES_TOPIC: _timeseries_slices(5, [5])}, length=5, dataset_to_index=6
    )

    with pytest.raises(ValueError, match="ends at row 5, while it should end at row 6"):
        with LeRobotWriter(_info(), tmp_path, TASKS) as writer:
            writer.write_episode(streamer, None)

    assert not data_path.exists()
    assert not _tmp_path(data_path).exists()


class _FailingStreamer:
    """Streams the slices of an episode, then fails, as a stream from Mosaico breaking
    halfway: records whether the temporary data file held the rows written so far."""

    def __init__(self, streamer: LeRobotEpisodeStreamer, data_path: Path):
        self._streamer = streamer
        self._data_path = data_path
        self.episode_metadata = streamer.episode_metadata
        self.sequence_name = streamer.sequence_name
        self.tmp_file_written = False
        """Whether the temporary data file, and not the data file, existed when failing."""

    def __iter__(self):
        yield from self._streamer
        self.tmp_file_written = (
            _tmp_path(self._data_path).is_file() and not self._data_path.exists()
        )
        raise RuntimeError("Injected failure")


def test_write_episode_failing_leaves_no_data_file(tmp_path, monkeypatch):
    """The rows are written to `file-000.parquet.tmp`, renamed to `file-000.parquet` only when
    the `with` block exits without errors: an episode failing halfway leaves neither file."""
    # Each slice is written as soon as converted: the temporary file holds rows when failing
    monkeypatch.setattr("mosaicolabs.bridges.lerobot.writer._ROW_GROUP_MAX_BYTES", 1)
    data_path = tmp_path / "data" / "chunk-000" / "file-000.parquet"
    streamer = _FailingStreamer(
        episode_streamer({TIMESERIES_TOPIC: _timeseries_slices(5, [2, 3])}, length=5),
        data_path,
    )

    with pytest.raises(RuntimeError, match="Injected failure"):
        with LeRobotWriter(_info(), tmp_path, TASKS) as writer:
            # Duck-typed: `write_episode` only uses what `_FailingStreamer` exposes
            writer.write_episode(cast(LeRobotEpisodeStreamer, streamer), None)

    assert streamer.tmp_file_written
    assert not data_path.exists()
    assert not _tmp_path(data_path).exists()


# --- Data files ---


@pytest.mark.parametrize(
    "chunk_index, file_index, chunks_size, expected",
    [
        (0, 0, 1000, (0, 1)),  # The next file of the chunk
        (0, 999, 1000, (1, 0)),  # The chunk is full: the first file of the next chunk
        (3, 1, 2, (4, 0)),
    ],
)
def test_next_file_index(chunk_index, file_index, chunks_size, expected):
    assert _next_file_index(chunk_index, file_index, chunks_size) == expected


def _streamers(lengths: List[int]) -> List[LeRobotEpisodeStreamer]:
    """The episodes of a dataset, of `lengths` rows, each one starting where the previous one
    ends."""
    streamers, start = [], 0
    for episode_index, length in enumerate(lengths):
        slices = _timeseries_slices(length, [length])
        streamers.append(
            episode_streamer(
                {TIMESERIES_TOPIC: slices},
                length=length,
                episode_index=episode_index,
                dataset_from_index=start,
            )
        )
        start += length
    return streamers


def _data_file(root: Path, chunk_index: int, file_index: int) -> Path:
    return root / DATA_PATH.format(chunk_index=chunk_index, file_index=file_index)


def _episode_indexes(data_path: Path) -> List[int]:
    """The `episode_index` of the rows of a data file."""
    return pq.read_table(data_path).column("episode_index").to_pylist()


def test_write_episode_keeps_the_episodes_in_one_file_below_the_size_limit(tmp_path):
    """With LeRobot's default limits, small episodes all go to `file-000.parquet`."""
    with LeRobotWriter(_info(), tmp_path, TASKS) as writer:
        episodes = [writer.write_episode(s, None) for s in _streamers([3, 2, 4])]

    assert [(e.data_chunk_index, e.data_file_index) for e in episodes] == [(0, 0)] * 3
    assert _episode_indexes(_data_file(tmp_path, 0, 0)) == [0] * 3 + [1] * 2 + [2] * 4
    assert not _data_file(tmp_path, 0, 1).exists()


def test_write_episode_starts_a_new_file_at_the_size_limit(tmp_path):
    """With a limit of 0 MB, every episode after the first one reaches it and starts a new
    data file, and a new chunk once the chunk holds `chunks_size` files. Each data file is
    renamed as soon as the next episode goes to another one."""
    with LeRobotWriter(
        _info(chunks_size=2, data_files_size_in_mb=0), tmp_path, TASKS
    ) as writer:
        first, *others = _streamers([3, 2, 4])
        episodes = [writer.write_episode(first, None)]
        assert _tmp_path(_data_file(tmp_path, 0, 0)).is_file()

        for streamer in others:
            episodes.append(writer.write_episode(streamer, None))
            # The previous data file is complete
            assert _data_file(tmp_path, 0, 0).is_file()

    assert [(e.data_chunk_index, e.data_file_index) for e in episodes] == [
        (0, 0),
        (0, 1),
        (1, 0),
    ]
    # Each episode is in the data file of its metadata, whole
    assert _episode_indexes(_data_file(tmp_path, 0, 0)) == [0] * 3
    assert _episode_indexes(_data_file(tmp_path, 0, 1)) == [1] * 2
    assert _episode_indexes(_data_file(tmp_path, 1, 0)) == [2] * 4
    assert not list(tmp_path.rglob(f"*{_TMP_SUFFIX}"))


def test_write_episode_returns_the_episode_with_its_data_file(tmp_path):
    """The episode of the streamer, shared with the loader, is left unchanged."""
    first, second = _streamers([3, 2])

    with LeRobotWriter(_info(data_files_size_in_mb=0), tmp_path, TASKS) as writer:
        writer.write_episode(first, None)
        episode = writer.write_episode(second, None)

    assert (episode.data_chunk_index, episode.data_file_index) == (0, 1)
    assert episode.episode_index == second.episode_metadata.episode_index
    assert second.episode_metadata.data_file_index == 0


def test_write_episode_failing_deletes_the_complete_data_files_too(tmp_path):
    """An episode failing after a new data file was started: the complete data file of the
    previous episodes is deleted too, and no data file is left."""
    first, second = _streamers([3, 2])
    failing = _FailingStreamer(second, _data_file(tmp_path, 0, 1))

    with pytest.raises(RuntimeError, match="Injected failure"):
        with LeRobotWriter(_info(data_files_size_in_mb=0), tmp_path, TASKS) as writer:
            writer.write_episode(first, None)
            writer.write_episode(cast(LeRobotEpisodeStreamer, failing), None)

    assert not list(tmp_path.rglob("*.parquet*"))
