import io
from collections import defaultdict
from dataclasses import replace

import numpy as np
import pyarrow as pa
import pyarrow.compute as pc
import pyarrow.parquet as pq
import pytest
from PIL import Image as PILImage

from mosaicolabs.bridges.lerobot.metadata import LeRobotDatasetMeta
from mosaicolabs.bridges.lerobot.writer import (
    LeRobotWriter,
    _aggregate_stats,
    _check_episode_stats,
    _compute_stats,
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
    TIMESTAMP_COLUMN_NAME,
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
    "size, shape", [((4, 4), (16, 3)), ((640, 480), (120 * 160, 3))]
)
def test_decode_stats_image_downsamples_large_images(size, shape):
    buffer = io.BytesIO()
    PILImage.new("RGB", size, (10, 20, 30)).save(buffer, format="PNG")

    image = _decode_stats_image(buffer.getvalue(), is_depth_map=False)

    assert image.shape == shape
    assert image[0].tolist() == [10, 20, 30]


def test_decode_stats_image_keeps_the_dtype_of_a_depth_map():
    """As LeRobot writes a depth map: a 16-bit TIFF, with a single channel."""
    buffer = io.BytesIO()
    PILImage.fromarray(np.full((4, 4), 1000, np.uint16)).save(buffer, format="TIFF")

    image = _decode_stats_image(buffer.getvalue(), is_depth_map=True)

    assert image.dtype == np.uint16
    assert image.shape == (16, 1)
    assert image[0, 0] == 1000


def test_compute_stats_per_column():
    """The std divides by n, a quantile is the first value where the count reaches q * n."""
    values = np.array([[1, 10], [2, 20], [3, 30], [4, 40], [5, 50]])

    stats = _compute_stats(values)

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


def test_aggregate_stats_as_lerobot(lerobot_dataset):
    """Min, max, mean, std and count are those of all the frames; quantiles are the envelope."""
    meta = LeRobotDatasetMeta.load(lerobot_dataset)
    rng = np.random.default_rng(0)
    frames = {
        i: rng.normal(i, i + 1, size=(n, 2))
        for i, n in zip(meta.episode_indexes, EPISODE_LENGTHS)
    }
    for i, values in frames.items():
        stats = {**_compute_stats(values), "count": np.array([len(values)])}
        image = {
            k: v.reshape(-1, 1, 1) if k != "count" else v for k, v in stats.items()
        }
        last = i == meta.episode_indexes[-1]
        meta.set_episode_stats(
            i,
            {STATE_COLUMN_NAME: stats}
            if last
            else {STATE_COLUMN_NAME: stats, CAMERA_COLUMN_NAMES[0]: image},
        )

    stats = _aggregate_stats(meta)

    for key, episodes, shape in [
        (STATE_COLUMN_NAME, list(frames), (-1,)),
        (
            CAMERA_COLUMN_NAMES[0],
            list(frames)[:-1],
            (-1, 1, 1),
        ),  # the last episode has no image
    ]:
        expected = _compute_stats(np.concatenate([frames[i] for i in episodes]))
        for name in ("min", "max", "mean", "std"):
            np.testing.assert_allclose(stats[key][name], expected[name].reshape(shape))
        assert stats[key]["count"] == [sum(len(frames[i]) for i in episodes)]
        stored = [meta.episodes_map[i].stats[key] for i in episodes]
        np.testing.assert_allclose(
            stats[key]["q10"], np.min([s["q10"] for s in stored], axis=0)
        )
        np.testing.assert_allclose(
            stats[key]["q90"], np.max([s["q90"] for s in stored], axis=0)
        )


@pytest.mark.parametrize("frames_without_image", [1, EPISODE_LENGTHS[0]])
def test_accumulate_stats_inputs_keeps_the_timeseries_and_the_sampled_images(
    lerobot_dataset, frames_without_image
):
    """Slice by slice; a frame without an image is skipped."""
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

    inputs = defaultdict(list)
    writer = LeRobotWriter(meta)
    for batch in (rows.slice(0, 2), rows.slice(2)):
        writer._accumulate_stats_inputs(
            batch.to_batches()[0], _sample_indices(EPISODE_LENGTHS[0]), inputs
        )

    np.testing.assert_array_equal(
        np.concatenate(inputs[STATE_COLUMN_NAME]),
        rows.column(STATE_COLUMN_NAME).to_pylist(),
    )
    assert np.concatenate(inputs[TIMESTAMP_COLUMN_NAME]).shape == (
        EPISODE_LENGTHS[0],
        1,
    )
    assert (
        len(inputs[CAMERA_COLUMN_NAMES[0]]) == EPISODE_LENGTHS[0] - frames_without_image
    )
    assert len(inputs[CAMERA_COLUMN_NAMES[1]]) == EPISODE_LENGTHS[0]
    # Frame 0 of the second camera: every pixel is (0, 128, 0) (see `image_value`)
    assert inputs[CAMERA_COLUMN_NAMES[1]][0].tolist() == [[0, 128, 0]] * (
        IMAGE_HEIGHT * IMAGE_WIDTH
    )


@pytest.mark.parametrize(
    "depth_info", [{"is_depth_map": True}, {"video.is_depth_map": True}]
)
def test_compute_episode_stats_as_lerobot(lerobot_dataset, depth_info):
    """The slices joined; an image per channel, of shape (channels, 1, 1), divided by 255
    but for a depth map; `count` is the length of the episode, for every feature."""
    meta = LeRobotDatasetMeta.load(lerobot_dataset)
    depth_key = CAMERA_COLUMN_NAMES[1]
    features = [
        replace(f, info=depth_info) if f.key == depth_key else f
        for f in meta.info.features
    ]
    writer = LeRobotWriter(replace(meta, info=replace(meta.info, features=features)))
    inputs = {
        STATE_COLUMN_NAME: [
            np.array([[1.0, 10.0], [2.0, 20.0]]),
            np.array([[3.0, 30.0]]),
        ],
        # Two images of two pixels each, a row per pixel and a column per channel
        CAMERA_COLUMN_NAMES[0]: [
            np.array([[0, 51, 255]] * 2, np.uint8),
            np.array([[255, 51, 0]] * 2, np.uint8),
        ],
        depth_key: [np.array([[1000], [3000]], np.uint16)],
    }

    stats = writer._compute_episode_stats(inputs, length=7)

    assert stats[STATE_COLUMN_NAME]["min"].tolist() == [1, 10]
    assert stats[STATE_COLUMN_NAME]["max"].tolist() == [3, 30]
    rgb_stats = stats[CAMERA_COLUMN_NAMES[0]]
    assert rgb_stats["min"].shape == (3, 1, 1)
    np.testing.assert_allclose(rgb_stats["min"].ravel(), [0, 0.2, 0])
    np.testing.assert_allclose(rgb_stats["max"].ravel(), [1, 0.2, 1])
    assert stats[depth_key]["mean"].tolist() == [[[2000]]]
    assert all(s["count"].tolist() == [7] for s in stats.values())


def test_compute_episode_stats_skips_a_camera_without_images(lerobot_dataset):
    writer = LeRobotWriter(LeRobotDatasetMeta.load(lerobot_dataset))

    stats = writer._compute_episode_stats({CAMERA_COLUMN_NAMES[0]: []}, length=7)

    assert stats == {}


def test_check_episode_stats_warns_out_of_tolerance(lerobot_dataset, caplog):
    """A wider tolerance for the quantiles (0.05% of max - min); the renumbered columns and the
    stats missing from the source are skipped."""
    episode = replace(
        LeRobotDatasetMeta.load(lerobot_dataset).episodes_map[0],
        stats={
            STATE_COLUMN_NAME: {"mean": [1.0, 2.0], "q50": [1.0, 2.0], "count": [5]},
            "index": {"mean": [0.0]},
        },
    )
    stats = {
        STATE_COLUMN_NAME: {
            "min": np.array([0.0, 0.0]),
            "max": np.array([10.0, 10.0]),
            "mean": np.array([1.0, 2.1]),
            "q50": np.array([1.004, 2.0]),
            "count": np.array([5]),
        },
        "index": {
            "min": np.array([5.0]),
            "max": np.array([9.0]),
            "mean": np.array([7.0]),
        },
    }

    _check_episode_stats(episode, stats)

    assert [r.levelname for r in caplog.records] == ["WARNING"]
    assert f"the `mean` of `{STATE_COLUMN_NAME}`" in caplog.records[0].getMessage()
