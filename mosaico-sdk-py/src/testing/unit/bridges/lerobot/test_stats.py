"""
Tests of the stats of a LeRobot dataset (`metadata/stats.py`). The expected values are worked
out by hand on small inputs, or are those over every frame at once.
"""

import io
from pathlib import Path
from typing import Any, Dict, Optional, Tuple

import numpy as np
import pyarrow as pa
import pyarrow.compute as pc
import pyarrow.parquet as pq
import pytest
from PIL import Image as PILImage

from mosaicolabs.bridges.lerobot.metadata import LeRobotDatasetMeta, LeRobotFeature
from mosaicolabs.bridges.lerobot.metadata.stats import (
    _STATS_QUANTILES,
    LeRobotStats,
    _compute_stats,
    _decode_image,
    _sample_indices,
    _to_matrix,
    compute_episode_stats,
)

from .config import (
    CAMERA_COLUMN_NAMES,
    DATA_PATH,
    DATA_SCHEMA,
    EPISODE_LENGTHS,
    STATE_COLUMN_NAME,
    episode_frames,
    state_value,
)

_IMAGE_TYPE = pa.struct([("bytes", pa.binary()), ("path", pa.string())])

_VECTOR_FEATURE = LeRobotFeature.from_dict(
    "observation.state", {"dtype": "float32", "shape": [2]}
)


def _png(color: Tuple[int, int, int], size: Tuple[int, int] = (4, 4)) -> bytes:
    """A PNG image of a plain RGB color."""
    buffer = io.BytesIO()
    PILImage.new("RGB", size, color).save(buffer, format="PNG")
    return buffer.getvalue()


def _depth_map(value: int) -> bytes:
    """A 4 x 4 depth map of a single 16-bit value, as LeRobot writes them: a TIFF file."""
    buffer = io.BytesIO()
    PILImage.fromarray(np.full((4, 4), value, np.uint16)).save(buffer, format="TIFF")
    return buffer.getvalue()


def _images(*images: bytes) -> pa.ChunkedArray:
    """An image column, as LeRobot writes it: structs of the content and path of the file."""
    rows = [{"bytes": image, "path": None} for image in images]
    return pa.chunked_array([pa.array(rows, _IMAGE_TYPE)])


def _image_feature(info: Optional[Dict[str, Any]] = None) -> LeRobotFeature:
    return LeRobotFeature.from_dict(
        CAMERA_COLUMN_NAMES[0], {"dtype": "image", "shape": [4, 4, 3], "info": info}
    )


def _stats(values: np.ndarray) -> LeRobotStats:
    """The stats of a matrix with a row per frame, as `LeRobotStats.from_column` computes
    them."""
    return LeRobotStats.from_dict(
        {**_compute_stats(values, (-1,)), "count": len(values)}
    )


def _feasible_stats(**changes: Any) -> Dict[str, Any]:
    """Feasible stats of a feature of 2 values, as `LeRobotEpisode.stats` holds them, with
    `changes`."""
    return {
        "min": [0.0, 1.0],
        "max": [2.0, 3.0],
        "mean": [1.0, 2.0],
        "std": [0.5, 0.5],
        "q50": [1.0, 2.0],
        "count": [4],
        **changes,
    }


def _remove_images(root: Path, camera: str, frames: int) -> None:
    """Makes null the images of `camera` in the first `frames` frames of episode 0, in its
    data file."""
    path = root / DATA_PATH.format(chunk_index=0, file_index=0)
    table = pq.read_table(path)
    is_removed = pc.and_(
        pc.equal(table["episode_index"], 0), pc.less(table["frame_index"], frames)
    )
    images = pc.if_else(is_removed, pa.scalar(None, table[camera].type), table[camera])
    column_index = table.schema.get_field_index(camera)
    pq.write_table(table.set_column(column_index, camera, images), path)


# --- _sample_indices ---


@pytest.mark.parametrize("length, num_samples", [(50, 50), (1000, 177), (20_000, 1681)])
def test_sample_indices_as_lerobot(length, num_samples):
    """The numbers of samples of LeRobot's `estimate_num_samples`, evenly spaced from the
    first frame to the last."""
    sampled = _sample_indices(length).to_pylist()

    assert len(sampled) == num_samples
    assert sampled[0] == 0 and sampled[-1] == length - 1
    assert sampled == sorted(set(sampled))


# --- _decode_image ---


@pytest.mark.parametrize("size, rows", [((4, 4), 16), ((640, 480), 160 * 120)])
def test_decode_image_as_rgb_divided_by_255(size, rows):
    """A row per pixel. An image of 300 px or more is strided down to about 150 px
    (640 // 150 = 4)."""
    pixels = _decode_image(_images(_png((0, 51, 255), size)), is_depth_map=False)

    assert pixels.shape == (rows, 3)
    np.testing.assert_allclose(pixels, [[0, 0.2, 1]] * rows)


def test_decode_image_keeps_the_values_of_a_depth_map():
    """A single channel, not divided by 255."""
    pixels = _decode_image(_images(_depth_map(1000)), is_depth_map=True)

    assert pixels.shape == (16, 1)
    np.testing.assert_array_equal(pixels, 1000)


# --- _to_matrix ---


@pytest.mark.parametrize(
    "shape, column, matrix",
    [
        ([1], pa.array([0.0, 0.5, 1.0], pa.float32()), [[0.0], [0.5], [1.0]]),
        (
            [2],
            pa.array([[1, 10], [2, 20]], pa.list_(pa.float32(), 2)),
            [[1, 10], [2, 20]],
        ),
        (
            [2, 3],
            pa.array(
                [[[0, 1, 2], [10, 11, 12]]], pa.list_(pa.list_(pa.float32(), 3), 2)
            ),
            [[0, 1, 2], [10, 11, 12]],
        ),
    ],
    ids=["scalar", "vector", "array"],
)
def test_to_matrix_has_a_column_per_element_of_the_last_axis(shape, column, matrix):
    """The other axes are pooled into the rows, as in LeRobot."""
    feature = LeRobotFeature.from_dict("feature", {"dtype": "float32", "shape": shape})

    assert _to_matrix(feature, pa.chunked_array([column])).tolist() == matrix


# --- _compute_stats ---


def test_compute_stats_per_column():
    """The std divides by n, a quantile is the first value where the count reaches q * n."""
    values = np.array([[1, 10], [2, 20], [3, 30], [4, 40], [5, 50]])

    stats = _compute_stats(values, (-1,))

    expected = {
        "min": [1, 10],
        "max": [5, 50],
        "mean": [3, 30],
        "std": [np.sqrt(2), 10 * np.sqrt(2)],
        "q01": [1, 10],
        "q10": [1, 10],
        "q50": [3, 30],
        "q90": [5, 50],
        "q99": [5, 50],
    }
    assert set(stats) == set(expected)
    for name, value in expected.items():
        assert stats[name].dtype == np.float64
        np.testing.assert_allclose(stats[name], value)


# --- LeRobotStats: creation ---


@pytest.mark.parametrize(
    "changes, error",
    [
        ({"std": [0.5]}, "not all of the same shape"),
        ({"q50": [[1.0, 2.0]]}, "not all of the same shape"),
        ({"count": [0]}, "of 0 frames"),
        ({"min": [5.0, 1.0]}, "higher than the max"),
        ({"std": [0.5, -0.1]}, "is negative"),
    ],
)
def test_stats_reject_infeasible_ones(changes, error):
    with pytest.raises(ValueError, match=error):
        LeRobotStats.from_dict(_feasible_stats(**changes))


def test_stats_accept_nan():
    """The stats of a feature holding NaN can be NaN."""
    stats = LeRobotStats.from_dict(
        _feasible_stats(min=[np.nan, 1.0], std=[np.nan, 0.5])
    )

    assert np.isnan(stats.min[0]) and np.isnan(stats.std[0])


@pytest.mark.parametrize("name", ["min", "max", "mean", "std", "count"])
def test_from_dict_requires_the_stats(name):
    """Missing, or null (the `meta/episodes` column of an episode without them)."""
    stats = _feasible_stats()
    del stats[name]

    for invalid in (stats, _feasible_stats(**{name: None})):
        with pytest.raises(ValueError, match=rf"The stats \['{name}'\] are missing"):
            LeRobotStats.from_dict(invalid)


def test_from_dict_gives_back_to_dict():
    """The stats of an image: a value per channel. A null quantile and a key that is not a
    stat are left out."""
    image_stats = {
        "min": [[[0.1]], [[0.2]], [[0.3]]],
        "max": [[[0.5]], [[0.6]], [[0.7]]],
        "mean": [[[0.3]], [[0.4]], [[0.5]]],
        "std": [[[0.1]], [[0.1]], [[0.1]]],
        "q01": [[[0.1]], [[0.2]], [[0.3]]],
        "count": [10],
    }

    stats = LeRobotStats.from_dict({**image_stats, "q99": None, "other": [1.0]})

    assert stats.mean.shape == (3, 1, 1)
    assert list(stats.quantiles) == ["q01"]
    assert stats.to_dict() == image_stats


def test_from_dict_reads_a_count_as_a_number():
    assert LeRobotStats.from_dict(_feasible_stats(count=4)).count == 4


# --- LeRobotStats.from_column ---


def test_from_column_of_a_timeseries():
    """A value per element of the vector, over every frame."""
    column = pa.chunked_array(
        [pa.array([[1, 10], [2, 20], [3, 30]], pa.list_(pa.float32(), 2))]
    )

    stats = LeRobotStats.from_column(_VECTOR_FEATURE, column, count=3)

    np.testing.assert_array_equal(stats.min, [1, 10])
    np.testing.assert_array_equal(stats.max, [3, 30])
    np.testing.assert_array_equal(stats.mean, [2, 20])
    assert stats.count == 3
    assert list(stats.quantiles) == list(_STATS_QUANTILES)


def test_from_column_of_images():
    """A value per channel, of shape (channels, 1, 1), in [0, 1]. `count` is the number of
    frames of the episode, not of the sampled images."""
    column = _images(_png((0, 51, 255)), _png((255, 51, 0)))

    stats = LeRobotStats.from_column(_image_feature(), column, count=7)

    assert stats.min.shape == (3, 1, 1)
    np.testing.assert_allclose(stats.min.ravel(), [0, 0.2, 0])
    np.testing.assert_allclose(stats.max.ravel(), [1, 0.2, 1])
    np.testing.assert_allclose(stats.mean.ravel(), [0.5, 0.2, 0.5])
    assert stats.count == 7


@pytest.mark.parametrize("info", [{"is_depth_map": True}, {"video.is_depth_map": True}])
def test_from_column_of_depth_maps(info):
    """Not divided by 255, whether the flag is that of an image or of a video."""
    column = _images(_depth_map(1000), _depth_map(3000))

    stats = LeRobotStats.from_column(_image_feature(info), column, count=2)

    assert stats.mean.tolist() == [[[2000.0]]]


# --- LeRobotStats.aggregate ---


def test_aggregate_two_episodes():
    """The example of `LeRobotStats.aggregate`: episodes `[1, 2, 3]` and `[10, 20]`.
    Exact for min, max, mean, std and count; the quantiles keep the widest range."""
    first, second = (
        _stats(np.array([[1.0], [2], [3]])),
        _stats(np.array([[10.0], [20]])),
    )

    aggregated = LeRobotStats.aggregate([first, second])

    assert (aggregated.min.tolist(), aggregated.max.tolist()) == ([1.0], [20.0])
    np.testing.assert_allclose(aggregated.mean, [7.2])
    np.testing.assert_allclose(aggregated.std, [np.std([1, 2, 3, 10, 20])])
    assert aggregated.count == 5
    # The lowest q50 of the episodes (that of every frame is 3), the highest q90
    assert aggregated.quantiles["q50"].tolist() == [2.0]
    assert aggregated.quantiles["q90"].tolist() == [20.0]


def test_aggregate_keeps_the_quantiles_of_every_episode():
    """A quantile some episode lacks (e.g. written by a LeRobot version without it) is left
    out."""
    with_q99 = _stats(np.array([[1.0], [2.0]]))
    without_q99 = LeRobotStats.from_dict({**with_q99.to_dict(), "q99": None})

    aggregated = LeRobotStats.aggregate([with_q99, without_q99])

    assert list(aggregated.quantiles) == ["q01", "q10", "q50", "q90"]


@pytest.mark.parametrize(
    "stats",
    [[], [_stats(np.ones((2, 2))), _stats(np.ones((2, 3)))]],
    ids=["no-stats", "different-shapes"],
)
def test_aggregate_rejects(stats):
    with pytest.raises(ValueError):
        LeRobotStats.aggregate(stats)


# --- compute_episode_stats ---


def test_compute_episode_stats_of_an_episode_sharing_its_data_file(lerobot_dataset):
    """Only the rows of the episode are read (episode 1 follows episode 0 in its data file).
    `count` is its length for every feature, the images included."""
    meta = LeRobotDatasetMeta.load(lerobot_dataset)

    stats = compute_episode_stats(meta.get_parquet_file(1), 1, meta.features)

    assert set(stats) == set(DATA_SCHEMA.names)
    assert all(s["count"] == [EPISODE_LENGTHS[1]] for s in stats.values())
    expected = _stats(np.array([state_value(i) for i in episode_frames(1)]))
    for name, value in expected.to_dict().items():
        np.testing.assert_allclose(stats[STATE_COLUMN_NAME][name], value)
    # The red of the front camera is the index of the frame (see `image_value`)
    red = np.mean(episode_frames(1)) / 255
    np.testing.assert_allclose(
        stats[CAMERA_COLUMN_NAMES[0]]["mean"], [[[red]], [[0]], [[0]]]
    )


def test_compute_episode_stats_leaves_out_the_features_without_stats(lerobot_dataset):
    """The videos (stored in separate files) and the texts, as LeRobot does."""
    meta = LeRobotDatasetMeta.load(lerobot_dataset)
    features = [
        *meta.features,
        LeRobotFeature.from_dict(
            "observation.images.side", {"dtype": "video", "shape": [4, 4, 3]}
        ),
        LeRobotFeature.from_dict(
            "language_instruction", {"dtype": "string", "shape": [1]}
        ),
    ]

    stats = compute_episode_stats(meta.get_parquet_file(0), 0, features)

    assert set(stats) == set(DATA_SCHEMA.names)


def test_compute_episode_stats_leaves_out_a_camera_without_images(
    lerobot_dataset_factory,
):
    root = lerobot_dataset_factory()
    _remove_images(root, CAMERA_COLUMN_NAMES[0], frames=EPISODE_LENGTHS[0])
    meta = LeRobotDatasetMeta.load(root)

    stats = compute_episode_stats(meta.get_parquet_file(0), 0, meta.features)

    assert CAMERA_COLUMN_NAMES[0] not in stats
    assert CAMERA_COLUMN_NAMES[1] in stats


def test_compute_episode_stats_of_an_episode_not_in_the_data_file(lerobot_dataset):
    """Episode 2 is in the second data file."""
    meta = LeRobotDatasetMeta.load(lerobot_dataset)

    with pytest.raises(RuntimeError, match="Episode 2 has no row"):
        compute_episode_stats(meta.get_parquet_file(0), 2, meta.features)
