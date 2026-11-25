"""
Tests of `LeRobotSchemaConverter.to_lerobot_pyarrow`: the schema of the data parquet files of an
exported dataset, with the types LeRobot writes each feature with.
"""

from typing import Tuple

import pyarrow as pa
import pytest

from mosaicolabs.bridges.lerobot.metadata import LeRobotFeature
from mosaicolabs.bridges.protocols.lerobot import LeRobotSchemaConverter

IMAGE_SHAPE = (480, 640, 3)


def _list_of(item_type: pa.DataType, size: int = -1) -> pa.DataType:
    """A list of `item_type`, with the item name of the parquet files written by LeRobot."""
    return pa.list_(pa.field("element", item_type), size)


@pytest.mark.parametrize(
    "dtype, shape, expected",
    [
        ("float32", (1,), pa.float32()),
        ("int64", (1,), pa.int64()),
        ("float32", (6,), _list_of(pa.float32(), 6)),
        ("float64", (2, 3), _list_of(_list_of(pa.float64()))),
        (
            "uint8",
            (1, 2, 3, 4, 5),
            _list_of(_list_of(_list_of(_list_of(_list_of(pa.uint8()))))),
        ),
        ("image", IMAGE_SHAPE, LeRobotSchemaConverter.IMAGE_ARROW_TYPE),
    ],
    ids=["scalar", "scalar-int", "vector", "array-2d", "array-5d", "image"],
)
def test_to_lerobot_pyarrow_types_each_feature_as_lerobot_writes_it(
    dtype: str, shape: Tuple[int, ...], expected: pa.DataType
):
    """A scalar is a single value, a vector a fixed size list, an array a nested list per
    dimension, and an image a struct of its file content and name."""
    schema = LeRobotSchemaConverter.to_lerobot_pyarrow(
        [LeRobotFeature("feature", dtype, shape)]
    )

    actual = schema.field("feature").type
    assert actual.equals(expected)
    # `equals` ignores the names of the list items, which the string holds
    assert str(actual) == str(expected)


def test_to_lerobot_pyarrow_keeps_the_order_of_the_features_without_the_videos():
    """The videos are stored in separate video files, not in the data files."""
    features = [
        LeRobotFeature("action", "float32", (6,)),
        LeRobotFeature("observation.images.front", "video", IMAGE_SHAPE),
        LeRobotFeature("timestamp", "float32", (1,)),
    ]

    schema = LeRobotSchemaConverter.to_lerobot_pyarrow(features)

    assert schema.names == ["action", "timestamp"]


def test_to_lerobot_pyarrow_rejects_an_unsupported_dtype():
    feature = LeRobotFeature("action", "not_a_dtype", (1,))

    with pytest.raises(
        ValueError, match="'action' has an unsupported dtype 'not_a_dtype'"
    ):
        LeRobotSchemaConverter.to_lerobot_pyarrow([feature])
