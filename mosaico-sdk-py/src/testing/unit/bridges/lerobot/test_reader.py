from dataclasses import replace
from pathlib import Path
from typing import Callable, List

import pyarrow as pa
import pyarrow.ipc as pa_ipc
import pyarrow.parquet as pq
import pytest

from mosaicolabs import CompressedImage
from mosaicolabs.bridges.lerobot.helpers import _max_rows_within, _to_timestamp_ns
from mosaicolabs.bridges.lerobot.lerobot_message import LeRobotMessage
from mosaicolabs.bridges.lerobot.metadata import LeRobotDatasetMeta
from mosaicolabs.bridges.lerobot.reader import (
    LeRobotReader,
    _adapt_image_rb_to_schema,
    _adapt_timeseries_rb_to_schema,
    _task_indexes_to_tasks,
    _timeseries_fields_to_msco_schema,
)

from .config import (
    ACTION_COLUMN_NAME,
    CAMERA_COLUMN_NAMES,
    DATA_SCHEMA,
    EPISODE_LENGTHS,
    EPISODES_PATH,
    INDEX_COLUMN_NAMES,
    STATE_COLUMN_NAME,
    TASK,
    TIMESERIES_COLUMN_NAMES,
    TIMESTAMP_COLUMN_NAME,
    action_value,
    episode_frames,
    image_value,
    state_value,
    timestamp_ns_value,
)

MAX_BATCH_SIZE_BYTES = 64 * 1024 * 1024
"""Large enough for every episode of the example dataset to fit in a single batch."""

TIMESERIES_TOPIC_NAME = "/timeseries"
CAMERA_TOPIC_NAMES = {
    "observation.images.front": "/observation/images/front",
    "observation.images.wrist": "/observation/images/wrist",
}

VECTOR_TYPE = DATA_SCHEMA.field(ACTION_COLUMN_NAME).type
IMAGE_TYPE = DATA_SCHEMA.field(CAMERA_COLUMN_NAMES[0]).type
COMPRESSED_IMAGE_SCHEMA = pa.schema(list(CompressedImage.__msco_pyarrow_struct__))


@pytest.fixture
def reader(lerobot_dataset) -> LeRobotReader:
    return LeRobotReader(lerobot_dataset, MAX_BATCH_SIZE_BYTES)


def _wire_batch(messages: List[LeRobotMessage]) -> pa.RecordBatch:
    """Concatenates the batches `TopicWriter.push_batch` sends for `messages`: the
    `timestamp_ns` column, then the payload columns."""
    return pa.concat_batches(
        [msg.data.add_column(0, "timestamp_ns", msg.timestamp_ns) for msg in messages]
    )


def _batch_size(batch: pa.RecordBatch, offset: int, length: int) -> int:
    """The size of the rows `[offset, offset + length)` of `batch`, as the reader measures it."""
    return pa_ipc.get_record_batch_size(batch.slice(offset, length))


def _rewrite_parquet(path: Path, transform: Callable[[pa.Table], pa.Table]):
    pq.write_table(transform(pq.read_table(path)), path)


# --- _timeseries_fields_to_msco_schema ---


def test_timeseries_schema_keeps_undotted_fields():
    fields = [pa.field("action", VECTOR_TYPE), pa.field("index", pa.int64())]

    assert _timeseries_fields_to_msco_schema(fields) == pa.schema(fields)


def test_timeseries_schema_nests_dotted_fields_into_structs():
    """The server rejects field names containing `.`."""
    schema = _timeseries_fields_to_msco_schema(
        [
            pa.field("action", VECTOR_TYPE),
            pa.field("observation.state", VECTOR_TYPE),
            pa.field("observation.force", pa.float32()),
        ]
    )

    assert schema == pa.schema(
        [
            pa.field("action", VECTOR_TYPE),
            pa.field(
                "observation",
                pa.struct(
                    [pa.field("state", VECTOR_TYPE), pa.field("force", pa.float32())]
                ),
            ),
        ]
    )


def test_timeseries_schema_nests_dotted_fields_at_any_depth():
    schema = _timeseries_fields_to_msco_schema([pa.field("a.b.c", pa.int64())])

    assert schema == pa.schema(
        [pa.field("a", pa.struct([pa.field("b", pa.struct([("c", pa.int64())]))]))]
    )


@pytest.mark.parametrize(
    ("names", "conflict"),
    [
        (["observation", "observation.state"], "observation"),
        (["observation.state", "observation"], "observation"),
        (["a.b", "a.b.c"], "a.b"),
        (["a.b.c", "a.b"], "a.b"),
    ],
)
def test_timeseries_schema_rejects_a_name_both_field_and_struct(names, conflict):
    fields = [pa.field(name, pa.int64()) for name in names]

    with pytest.raises(ValueError, match=f"'{conflict}' cannot be both"):
        _timeseries_fields_to_msco_schema(fields)


# --- _adapt_timeseries_rb_to_schema ---


def test_adapt_timeseries_batch_nests_the_dotted_columns():
    batch = pa.RecordBatch.from_pydict(
        {
            "action": pa.array([1.0, 2.0]),
            "observation.state": pa.array([3.0, 4.0]),
            "observation.force": pa.array([5.0, 6.0]),
        }
    )
    schema = _timeseries_fields_to_msco_schema(list(batch.schema))

    adapted = _adapt_timeseries_rb_to_schema(batch, schema)

    assert adapted.schema == schema
    assert adapted.to_pylist() == [
        {"action": 1.0, "observation": {"state": 3.0, "force": 5.0}},
        {"action": 2.0, "observation": {"state": 4.0, "force": 6.0}},
    ]


# --- _adapt_image_rb_to_schema ---


def test_adapt_image_batch_to_compressed_image():
    image = pa.array(
        [
            {"bytes": b"first", "path": "frame-000000.png"},
            {"bytes": b"second", "path": "frame-000001.png"},
        ],
        type=IMAGE_TYPE,
    )

    adapted = _adapt_image_rb_to_schema(image, COMPRESSED_IMAGE_SCHEMA)

    assert adapted.schema == COMPRESSED_IMAGE_SCHEMA
    assert adapted.column("header").null_count == 2
    assert adapted.column("data").to_pylist() == [b"first", b"second"]
    assert adapted.column("format").to_pylist() == ["png", "png"]


@pytest.mark.parametrize(
    ("path", "image_format"),
    [
        ("frame-000000.png", "png"),
        ("frame-000000.JPEG", "jpeg"),
        ("images.v1/frame-000000.webp", "webp"),
        ("frame-000000.tar.gz", "gz"),
        ("frame-000000", ""),
        ("images.v1/frame-000000", ""),
        (None, ""),
    ],
)
def test_adapt_image_batch_format_is_the_lowercase_file_extension(path, image_format):
    image = pa.array([{"bytes": b"image", "path": path}], type=IMAGE_TYPE)

    adapted = _adapt_image_rb_to_schema(image, COMPRESSED_IMAGE_SCHEMA)

    assert adapted.column("format").to_pylist() == [image_format]


def test_adapt_image_batch_frame_without_image_is_empty():
    image = pa.array(
        [None, {"bytes": None, "path": "frame-000001.png"}],
        type=IMAGE_TYPE,
    )

    adapted = _adapt_image_rb_to_schema(image, COMPRESSED_IMAGE_SCHEMA)

    assert adapted.column("data").to_pylist() == [b"", b""]
    assert adapted.column("format").to_pylist() == ["", "png"]


# --- helpers ---


def test_to_timestamp_ns_converts_seconds_into_int64_nanoseconds():
    timestamp_ns = _to_timestamp_ns(pa.array([0.0, 0.0625, 1.5], pa.float32()))

    assert timestamp_ns.type == pa.int64()
    assert timestamp_ns.to_pylist() == [0, 62_500_000, 1_500_000_000]


def test_to_timestamp_ns_rounds_to_the_nearest_nanosecond():
    timestamp_ns = _to_timestamp_ns(pa.array([1.4e-9, 1.6e-9], pa.float64()))

    assert timestamp_ns.to_pylist() == [1, 2]


@pytest.fixture
def int64_batch() -> pa.RecordBatch:
    return pa.RecordBatch.from_pydict({"value": pa.array(range(1000), pa.int64())})


@pytest.mark.parametrize("rows", [1, 9, 500, 990])
def test_max_rows_within_returns_the_most_rows_within_max_size(int64_batch, rows):
    max_size = _batch_size(int64_batch, 0, rows)

    max_rows = _max_rows_within(int64_batch, max_size)

    assert max_rows >= rows
    assert _batch_size(int64_batch, 0, max_rows) <= max_size
    assert _batch_size(int64_batch, 0, max_rows + 1) > max_size


def test_max_rows_within_returns_zero_if_no_row_fits(int64_batch):
    max_size = _batch_size(int64_batch, 0, 1) - 1

    assert _max_rows_within(int64_batch, max_size) == 0


# --- LeRobotReader: initialization and properties ---


@pytest.mark.parametrize("with_meta", [True, False])
def test_reader_episodes(lerobot_dataset, with_meta):
    meta = LeRobotDatasetMeta.load(lerobot_dataset) if with_meta else None

    reader = LeRobotReader(lerobot_dataset, MAX_BATCH_SIZE_BYTES, meta)

    assert reader.n_episodes == len(EPISODE_LENGTHS)
    assert reader.episode_indexes == list(range(len(EPISODE_LENGTHS)))


@pytest.mark.parametrize(
    ("column_names", "cameras", "timeseries"),
    [
        (None, CAMERA_COLUMN_NAMES, TIMESERIES_COLUMN_NAMES),
        ([], [], []),
        (
            [STATE_COLUMN_NAME, CAMERA_COLUMN_NAMES[1]],
            [CAMERA_COLUMN_NAMES[1]],
            [STATE_COLUMN_NAME],
        ),
        # The timestamp is the time of the messages, not a timeseries
        (
            ["unknown", TIMESTAMP_COLUMN_NAME, ACTION_COLUMN_NAME],
            [],
            [ACTION_COLUMN_NAME],
        ),
    ],
)
def test_reader_selects_the_requested_columns(
    lerobot_dataset, column_names, cameras, timeseries
):
    reader = LeRobotReader(
        lerobot_dataset, MAX_BATCH_SIZE_BYTES, column_names=column_names
    )

    assert reader.cameras_column_names == cameras
    assert reader.timeseries_column_names == timeseries


def test_reader_timeseries_struct_nests_the_dotted_columns(reader):
    assert reader.timeseries_struct == pa.struct(
        [
            pa.field("action", VECTOR_TYPE),
            pa.field("observation", pa.struct([pa.field("state", VECTOR_TYPE)])),
            *[pa.field(name, pa.int64()) for name in INDEX_COLUMN_NAMES[:-1]],
            pa.field("task_index", pa.string()),
        ]
    )


def test_reader_timeseries_struct_holds_the_selected_columns_only(lerobot_dataset):
    reader = LeRobotReader(
        lerobot_dataset, MAX_BATCH_SIZE_BYTES, column_names=[STATE_COLUMN_NAME]
    )

    assert reader.timeseries_struct == pa.struct(
        [pa.field("observation", pa.struct([pa.field("state", VECTOR_TYPE)]))]
    )


def test_reader_msco_topic_name_of_a_column_not_selected_is_none(lerobot_dataset):
    reader = LeRobotReader(
        lerobot_dataset, MAX_BATCH_SIZE_BYTES, column_names=[ACTION_COLUMN_NAME]
    )

    assert reader.get_msco_topic_name(STATE_COLUMN_NAME) is None
    assert reader.get_msco_topic_name(CAMERA_COLUMN_NAMES[0]) is None


def test_reader_requires_an_episode(lerobot_dataset):
    meta = replace(LeRobotDatasetMeta.load(lerobot_dataset), episodes_map={})

    with pytest.raises(ValueError, match="has no episode"):
        LeRobotReader(lerobot_dataset, MAX_BATCH_SIZE_BYTES, meta)


# --- LeRobotReader: episode batches ---


def test_iter_episode_batches_reads_the_rows_of_the_episode_only(reader):
    """Episode 1 shares its data file with episode 0."""
    batches = list(
        reader._iter_episode_batches(1, ["index", "frame_index"], batch_size=2)
    )

    assert all(0 < batch.num_rows <= 2 for batch in batches)
    table = pa.Table.from_batches(batches)
    # `episode_index`, read to filter the rows, is not returned if not requested
    assert table.column_names == ["index", "frame_index"]
    assert table.column("index").to_pylist() == list(episode_frames(1))


def test_iter_episode_batches_reads_every_column_by_default(reader):
    batches = list(reader._iter_episode_batches(2))

    assert batches[0].schema.names == DATA_SCHEMA.names


def test_fit_batch_keeps_a_batch_within_max_size_whole(reader, int64_batch):
    full_batches, pending = reader._fit_batch(int64_batch)

    assert full_batches == []
    assert pending.num_rows == int64_batch.num_rows


def test_fit_batch_cuts_slices_within_max_size(lerobot_dataset, int64_batch):
    max_size = _batch_size(int64_batch, 0, 300)
    reader = LeRobotReader(lerobot_dataset, max_size)

    full_batches, pending = reader._fit_batch(int64_batch)

    assert len(full_batches) >= 3
    batches = [*full_batches, pending]
    assert all(pa_ipc.get_record_batch_size(b) <= max_size for b in batches)
    assert pa.concat_batches(batches).equals(int64_batch)


# --- LeRobotReader.iter_timeseries_batches ---


@pytest.mark.parametrize("episode_index", range(len(EPISODE_LENGTHS)))
def test_timeseries_batches_hold_the_rows_of_the_episode(reader, episode_index):
    frames = episode_frames(episode_index)
    length = len(frames)

    batch = _wire_batch(list(reader.iter_timeseries_batches(episode_index)))

    assert batch.column("timestamp_ns").to_pylist() == [
        timestamp_ns_value(frame_index) for frame_index in range(length)
    ]
    assert batch.column("action").to_pylist() == [action_value(i) for i in frames]
    assert batch.column("observation").to_pylist() == [
        {"state": state_value(i)} for i in frames
    ]
    assert batch.column("frame_index").to_pylist() == list(range(length))
    assert batch.column("episode_index").to_pylist() == [episode_index] * length
    assert batch.column("index").to_pylist() == list(frames)
    assert batch.column("task_index").to_pylist() == [TASK] * length


def test_task_index_not_in_tasks_raises():
    batch = pa.record_batch({"task_index": [0, 1]})

    with pytest.raises(ValueError, match="meta/tasks.parquet"):
        _task_indexes_to_tasks(batch, [TASK])


def test_timeseries_batches_are_messages_of_the_timeseries_topic(reader):
    messages = list(reader.iter_timeseries_batches(0))

    for msg in messages:
        assert msg.msco_topic_name == TIMESERIES_TOPIC_NAME
        assert msg.column_names == TIMESERIES_COLUMN_NAMES
        assert msg.msco_ontology is reader.timeseries_ontology
        assert msg.timestamp_ns.type == pa.int64()
        assert msg.data.schema == pa.schema(list(reader.timeseries_struct))
        assert msg.n_rows == msg.data.num_rows == len(msg.timestamp_ns)


def test_timeseries_batches_without_timeseries_column_yield_nothing(lerobot_dataset):
    reader = LeRobotReader(
        lerobot_dataset, MAX_BATCH_SIZE_BYTES, column_names=CAMERA_COLUMN_NAMES
    )

    assert list(reader.iter_timeseries_batches(0)) == []


# --- LeRobotReader.iter_image_by_name_batches / iter_image_batches ---


@pytest.mark.parametrize("camera_column_name", CAMERA_COLUMN_NAMES)
@pytest.mark.parametrize("episode_index", range(len(EPISODE_LENGTHS)))
def test_image_batches_hold_the_images_of_the_episode(
    reader, episode_index, camera_column_name
):
    frames = episode_frames(episode_index)
    length = len(frames)

    batch = _wire_batch(
        list(reader.iter_image_by_name_batches(episode_index, camera_column_name))
    )

    assert batch.column("timestamp_ns").to_pylist() == [
        timestamp_ns_value(frame_index) for frame_index in range(length)
    ]
    assert batch.column("data").to_pylist() == [
        image_value(i, camera_column_name) for i in frames
    ]
    assert batch.column("format").to_pylist() == ["png"] * length


@pytest.mark.parametrize("camera_column_name", CAMERA_COLUMN_NAMES)
def test_image_batches_are_messages_of_the_camera_topic(reader, camera_column_name):
    messages = list(reader.iter_image_by_name_batches(0, camera_column_name))

    for msg in messages:
        assert msg.msco_topic_name == CAMERA_TOPIC_NAMES[camera_column_name]
        assert msg.column_names == [camera_column_name]
        assert msg.msco_ontology is CompressedImage
        assert msg.timestamp_ns.type == pa.int64()
        assert msg.data.schema == COMPRESSED_IMAGE_SCHEMA
        assert msg.n_rows == msg.data.num_rows == len(msg.timestamp_ns)


@pytest.mark.parametrize(
    "column_name", ["unknown", ACTION_COLUMN_NAME, CAMERA_COLUMN_NAMES[1]]
)
def test_image_batches_reject_a_column_not_selected_as_camera(
    lerobot_dataset, column_name
):
    reader = LeRobotReader(
        lerobot_dataset, MAX_BATCH_SIZE_BYTES, column_names=[CAMERA_COLUMN_NAMES[0]]
    )

    with pytest.raises(ValueError, match="camera column name"):
        list(reader.iter_image_by_name_batches(0, column_name))


def test_iter_image_batches_reads_every_camera_in_order(reader):
    messages = list(reader.iter_image_batches(1))

    assert [msg.msco_topic_name for msg in messages] == list(
        CAMERA_TOPIC_NAMES.values()
    )
    assert all(msg.n_rows == EPISODE_LENGTHS[1] for msg in messages)


def test_iter_image_batches_reads_the_selected_cameras_only(lerobot_dataset):
    reader = LeRobotReader(
        lerobot_dataset, MAX_BATCH_SIZE_BYTES, column_names=[CAMERA_COLUMN_NAMES[1]]
    )

    messages = list(reader.iter_image_batches(0))

    assert [msg.column_names for msg in messages] == [[CAMERA_COLUMN_NAMES[1]]]


# --- LeRobotReader: errors while reading an episode ---

READ_EPISODE = [
    pytest.param(
        lambda reader, episode_index: list(
            reader.iter_timeseries_batches(episode_index)
        ),
        id="timeseries",
    ),
    pytest.param(
        lambda reader, episode_index: list(
            reader.iter_image_by_name_batches(episode_index, CAMERA_COLUMN_NAMES[0])
        ),
        id="images",
    ),
]


@pytest.mark.parametrize("read_episode", READ_EPISODE)
def test_reading_an_unknown_episode_raises(reader, read_episode):
    with pytest.raises(ValueError, match="Episode 99 is not defined"):
        read_episode(reader, 99)


@pytest.mark.parametrize("read_episode", READ_EPISODE)
def test_reading_an_episode_whose_rows_differ_from_its_length_raises(
    lerobot_dataset_factory, read_episode
):
    root = lerobot_dataset_factory()
    # The first episodes file holds the rows of episodes 0 and 1
    _rewrite_parquet(
        root / EPISODES_PATH.format(chunk_index=0, file_index=0),
        lambda table: table.set_column(
            table.schema.get_field_index("length"),
            "length",
            pa.array([EPISODE_LENGTHS[0] + 1, EPISODE_LENGTHS[1]], pa.int64()),
        ),
    )
    reader = LeRobotReader(root, MAX_BATCH_SIZE_BYTES)

    with pytest.raises(ValueError, match=f"Episode 0 has {EPISODE_LENGTHS[0]} rows"):
        read_episode(reader, 0)
