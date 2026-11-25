from typing import List

import pyarrow as pa
import pytest
from PIL import Image

from mosaicolabs import CompressedImage
from mosaicolabs.bridges.lerobot.writer import (
    _to_image_column,
    _to_task_index_column,
    _to_timeseries_column,
)
from mosaicolabs.bridges.protocols.lerobot import LeRobotSchemaConverter
from mosaicolabs.models.sensors.image import ImageFormat

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
