import io
from collections import defaultdict

import numpy as np
import pyarrow as pa
import pyarrow.compute as pc
import pyarrow.parquet as pq
import pytest
from PIL import Image as PILImage

from mosaicolabs.bridges.lerobot.metadata import LeRobotDatasetMeta
from mosaicolabs.bridges.lerobot.writer import (
    LeRobotWriter,
    _decode_stats_image,
    _sample_indices,
    _to_task_index_column,
)

from .config import (
    CAMERA_COLUMN_NAMES,
    EPISODE_LENGTHS,
    IMAGE_HEIGHT,
    IMAGE_WIDTH,
    STATE_COLUMN_NAME,
)

TASKS = ["Pick up the cube", "Place the cube"]


def _timeseries(task: pa.Array) -> pa.RecordBatch:
    """A batch of the timeseries topic, with its `task` field."""
    return pa.record_batch({"action": pa.array([1.0] * len(task)), "task": task})


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


# --- Stats rows ---


@pytest.mark.parametrize("length, num_samples", [(50, 50), (1000, 177), (20_000, 1681)])
def test_sample_indices_as_lerobot(length, num_samples):
    """The numbers of samples of LeRobot's `estimate_num_samples`, from the first frame to
    the last."""
    sampled = _sample_indices(length).to_pylist()

    assert len(sampled) == num_samples
    assert sampled[0] == 0 and sampled[-1] == length - 1


@pytest.mark.parametrize(
    "size, shape", [((4, 4), (3, 4, 4)), ((640, 480), (3, 120, 160))]
)
def test_decode_stats_image_downsamples_large_images(size, shape):
    buffer = io.BytesIO()
    PILImage.new("RGB", size, (10, 20, 30)).save(buffer, format="PNG")

    image = _decode_stats_image(buffer.getvalue(), is_depth_map=False)

    assert image.shape == shape
    assert image[:, 0, 0].tolist() == [10, 20, 30]


def test_decode_stats_image_keeps_the_dtype_of_a_depth_map():
    """As LeRobot writes a depth map: a 16-bit TIFF, with a single channel."""
    buffer = io.BytesIO()
    PILImage.fromarray(np.full((4, 4), 1000, np.uint16)).save(buffer, format="TIFF")

    image = _decode_stats_image(buffer.getvalue(), is_depth_map=True)

    assert image.dtype == np.uint16
    assert image.shape == (1, 4, 4)
    assert image[0, 0, 0] == 1000


@pytest.mark.parametrize("frames_without_image", [1, EPISODE_LENGTHS[0]])
def test_collect_stats_rows_keeps_the_numeric_columns_and_the_sampled_images(
    lerobot_dataset, frames_without_image
):
    """Slice by slice; a frame without an image is skipped. A camera without any image
    still gets its key, with no images."""
    meta = LeRobotDatasetMeta.load(lerobot_dataset)
    episode = meta.episodes_map[0]
    data_path = meta.root / meta.info.data_path.format(
        chunk_index=episode.data_chunk_index, file_index=episode.data_file_index
    )
    rows = pq.read_table(data_path).slice(0, EPISODE_LENGTHS[0]).combine_chunks()
    front = rows.column(CAMERA_COLUMN_NAMES[0]).chunk(0)
    is_missing = pa.array([i < frames_without_image for i in range(len(front))])
    without_images = pc.if_else(is_missing, pa.scalar(None, front.type), front)
    rows = rows.set_column(
        rows.schema.get_field_index(CAMERA_COLUMN_NAMES[0]),
        CAMERA_COLUMN_NAMES[0],
        without_images,
    )

    columns = defaultdict(list)
    images = defaultdict(list)
    writer = LeRobotWriter(meta)
    for batch in (rows.slice(0, 2), rows.slice(2)):
        writer._collect_stats_rows(
            batch.to_batches()[0], _sample_indices(EPISODE_LENGTHS[0]), columns, images
        )

    assert (
        pa.concat_arrays(columns[STATE_COLUMN_NAME])
        == rows.column(STATE_COLUMN_NAME).combine_chunks()
    )
    # Before any `images[...]`, which would add the key
    assert set(images) == set(CAMERA_COLUMN_NAMES)
    assert (
        len(images[CAMERA_COLUMN_NAMES[0]]) == EPISODE_LENGTHS[0] - frames_without_image
    )
    assert len(images[CAMERA_COLUMN_NAMES[1]]) == EPISODE_LENGTHS[0]
    assert images[CAMERA_COLUMN_NAMES[1]][0].shape == (3, IMAGE_HEIGHT, IMAGE_WIDTH)
