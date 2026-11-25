from typing import Optional

import pytest

from mosaicolabs.bridges.lerobot.metadata import LeRobotFeature

IMAGE_SHAPE = (96, 96, 3)

_KIND_PREDICATES = [
    "is_image",
    "is_video",
    "is_timestamp",
    "is_index",
    "is_frame_index",
    "is_episode_index",
    "is_task_index",
]
"""The predicates telling what a feature is: at most one of them is true."""

_SHAPE_PREDICATES = ["is_scalar", "is_list", "is_nested_list"]
"""The predicates telling the shape of a feature: exactly one of them is true, except for the
images and videos."""


def _true_predicates(feature: LeRobotFeature, predicates):
    return [name for name in predicates if getattr(feature, name)()]


# --- from_dict / to_dict ---


def test_feature_to_dict():
    feature = LeRobotFeature.from_dict(
        "action", {"dtype": "float32", "shape": [2], "names": ["x", "y"]}
    )

    assert feature.shape == (2,)
    # As in `meta/info.json`: no `key`, no `info` if not set
    assert feature.to_dict() == {
        "dtype": "float32",
        "shape": [2],
        "names": ["x", "y"],
    }


def test_feature_to_dict_keeps_its_info():
    description = {
        "dtype": "video",
        "shape": list(IMAGE_SHAPE),
        "names": ["height", "width", "channels"],
        "info": {"video.codec": "av1"},
    }

    feature = LeRobotFeature.from_dict("observation.images.side", description)

    assert feature.to_dict() == description


@pytest.mark.parametrize(
    "description, error",
    [
        ({"shape": [2]}, "has no 'dtype'"),
        ({"dtype": "float32"}, "has no 'shape'"),
        ({"dtype": "float32", "shape": None}, "unsupported"),
        ({"dtype": "float32", "shape": []}, "unsupported"),
        ({"dtype": "float32", "shape": [0]}, "unsupported"),
        ({"dtype": "float32", "shape": [1, 2, 3, 4, 5, 6]}, "unsupported"),
    ],
)
def test_feature_from_dict_rejects_an_invalid_description(description, error):
    with pytest.raises(ValueError, match=error):
        LeRobotFeature.from_dict("action", description)


@pytest.mark.parametrize("dtype", ["image", "video"])
def test_image_and_video_accept_any_shape(dtype):
    feature = LeRobotFeature("observation.images.front", dtype, IMAGE_SHAPE)

    assert _true_predicates(feature, _SHAPE_PREDICATES) == []


# --- What the feature is ---


@pytest.mark.parametrize(
    "key, dtype, shape, expected",
    [
        ("observation.images.front", "image", IMAGE_SHAPE, "is_image"),
        ("observation.images.side", "video", IMAGE_SHAPE, "is_video"),
        ("timestamp", "float32", (1,), "is_timestamp"),
        ("index", "int64", (1,), "is_index"),
        ("frame_index", "int64", (1,), "is_frame_index"),
        ("episode_index", "int64", (1,), "is_episode_index"),
        ("task_index", "int64", (1,), "is_task_index"),
        ("action", "float32", (2,), None),
    ],
)
def test_feature_kind(key, dtype, shape, expected: Optional[str]):
    feature = LeRobotFeature(key, dtype, shape)

    assert _true_predicates(feature, _KIND_PREDICATES) == (
        [expected] if expected else []
    )


@pytest.mark.parametrize(
    "key, dtype, shape, expected",
    [
        ("action", "float32", (2,), True),
        ("observation.state", "float32", (2, 3), True),
        ("task_index", "int64", (1,), True),
        ("timestamp", "float32", (1,), False),
        ("observation.images.front", "image", IMAGE_SHAPE, False),
        ("observation.images.side", "video", IMAGE_SHAPE, False),
    ],
)
def test_feature_is_timeseries(key, dtype, shape, expected):
    assert LeRobotFeature(key, dtype, shape).is_timeseries() is expected


# --- The shape of the feature ---


@pytest.mark.parametrize(
    "shape, expected",
    [
        ((1,), "is_scalar"),
        ((2,), "is_list"),
        ((2, 3), "is_nested_list"),
        ((1, 2, 3, 4, 5), "is_nested_list"),
    ],
)
def test_feature_shape_kind(shape, expected):
    feature = LeRobotFeature("observation.state", "float32", shape)

    assert _true_predicates(feature, _SHAPE_PREDICATES) == [expected]
