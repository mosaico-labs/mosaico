"""
Writer of the LeRobot dataset extraction: writes the episodes streamed from Mosaico (see
`LeRobotEpisodeStreamer`) to the data parquet file of the exported dataset.

It is the opposite of `LeRobotReader`: the data columns are rebuilt from the topics of the
sequences, with the types LeRobot writes them with (see
`LeRobotSchemaConverter.to_lerobot_pyarrow`).
"""

import io
import logging
from collections import defaultdict
from pathlib import Path
from typing import DefaultDict, Dict, List

import numpy as np
import pyarrow as pa
import pyarrow.compute as pc
import pyarrow.parquet as pq
from PIL import Image as PILImage

from mosaicolabs.logging_config import get_logger

from ..helpers import _dotted_name_to_topic_name
from ..protocols.lerobot import LeRobotSchemaConverter
from .helpers import _from_timestamp_ns
from .metadata import LeRobotDatasetMeta, LeRobotEpisode
from .metadata.feature import (
    _EPISODE_INDEX_COLUMN,
    _FRAME_INDEX_COLUMN,
    _INDEX_COLUMN,
    _TASK_INDEX_COLUMN,
)
from .mosaico_loader import LeRobotEpisodeStreamer
from .reader import (
    _NESTED_NAME_SEP,
    _NOT_FOUND,
    _TASK_COLUMN,
    _TIMESERIES_MSCO_TOPIC_NAME,
)

_IMAGE_DATA_FIELD = "data"
"""The field of the `CompressedImage` ontology holding the content of the image file."""

_IMAGE_FORMAT_FIELD = "format"
"""The field of the `CompressedImage` ontology holding the format of the image (e.g. `png`)."""

_FRAME_INDEX_WIDTH = 6
"""The number of digits of the frame index in the name of an image (`frame-000012.png`)."""

_NO_STATS_DTYPES = ("string", "language")
"""The `dtype` of the features LeRobot computes no stats for."""

_STATS_QUANTILES = {"q01": 0.01, "q10": 0.10, "q50": 0.50, "q90": 0.90, "q99": 0.99}
"""The quantiles of the stats, by name (`DEFAULT_QUANTILES` in LeRobot)."""

_CHECK_RTOL = 1e-5
"""The relative tolerance of the debug check of the stats (see `_check_episode_stats`)."""

_CHECK_ATOL = 1e-8
"""The absolute tolerance of the debug check of the stats but the quantiles, for the values
near 0 (e.g. the std of a constant column)."""

_CHECK_QUANTILE_ATOL_SHARE = 0.02
"""The absolute tolerance of the quantiles in the debug check of the stats, as a share of
`max - min`: LeRobot estimates them from a histogram (up to 1.4% of the range apart on real
data)."""

_RENUMBERED_COLUMNS = (_INDEX_COLUMN, _EPISODE_INDEX_COLUMN, _TASK_INDEX_COLUMN)
"""The columns renumbered on export: their stats differ from the source ones."""

# Set the hierarchical logger
logger = get_logger(__name__)


def _sample_indices(length: int) -> pa.Array:
    """
    Returns the frames of an episode whose images go into the stats, as LeRobot picks them
    (`sample_indices` in `lerobot/datasets/compute_stats.py`): evenly spaced, 100 of them (all
    the frames of a shorter episode), up to 10,000 for a long one (`length ** 0.75`).

    Args:
        length (int): The number of frames of the episode.

    Returns:
        pa.Array: The `frame_index` of the sampled frames.
    """
    num_samples = max(min(100, length), min(int(length**0.75), 10_000))
    return pa.array(np.round(np.linspace(0, length - 1, num_samples)).astype(np.int64))


def _decode_stats_image(data: bytes, is_depth_map: bool) -> np.ndarray:
    """
    Decodes an image for the stats, as LeRobot does (`sample_images` in
    `lerobot/datasets/compute_stats.py`): RGB `uint8`, or a depth map in its own dtype (e.g.
    `uint16` or `float32`) and channel, and downsampled by striding to about 150 px on its
    longer side when that side is 300 px or more.

    Args:
        data (bytes): The content of the image file.
        is_depth_map (bool): Whether the image is a depth map.

    Returns:
        np.ndarray: The image, of shape `(pixels, channels)`: a row per pixel, a column per
            channel.
    """
    image = PILImage.open(io.BytesIO(data))
    if not is_depth_map:
        image = image.convert("RGB")
    # (height, width, channels): a depth map has no channel axis, it gets one
    image = np.atleast_3d(np.asarray(image))

    long_side = max(image.shape[:2])
    step = long_side // 150 if long_side >= 300 else 1
    # A copy: a strided view would keep the whole decoded image in memory
    return image[::step, ::step].reshape(-1, image.shape[-1]).copy()


def _compute_stats(values: np.ndarray) -> Dict[str, np.ndarray]:
    """
    Computes the stats of a feature in an episode, per column of `values`, as LeRobot does
    (`get_feature_stats` in `lerobot/datasets/compute_stats.py`): min, max, mean, std (divided
    by n) and the quantiles of `_STATS_QUANTILES`, in `float64`. The quantiles are exact
    (linear interpolation), where LeRobot estimates them from a histogram.

    Args:
        values (np.ndarray): The values, of shape `(rows, columns)`, with at least one row:
            e.g. a row per frame and a column per dimension of a vector.

    Returns:
        Dict[str, np.ndarray]: Each stat, of shape `(columns,)`.
    """
    values = values.astype(np.float64, copy=False)
    quantiles = np.quantile(values, list(_STATS_QUANTILES.values()), axis=0)
    return {
        "min": values.min(axis=0),
        "max": values.max(axis=0),
        "mean": values.mean(axis=0),
        "std": values.std(axis=0),
        **dict(zip(_STATS_QUANTILES, quantiles)),
    }


def _check_episode_stats(
    episode: LeRobotEpisode, stats: Dict[str, Dict[str, np.ndarray]]
) -> None:
    """
    Debug check of the stats computed on export: logs a warning for each stat out of tolerance
    with the source one the episode carries (computed by LeRobot, read from the `_lerobot_`
    metadata of its sequence). The tolerance is `_CHECK_RTOL` and `_CHECK_ATOL`, or
    `_CHECK_QUANTILE_ATOL_SHARE` of `max - min` for the quantiles. The renumbered columns
    (`_RENUMBERED_COLUMNS`) and the stats missing on either side are skipped; a non-finite
    source value, stored as `None`, is compared as NaN.

    Args:
        episode (LeRobotEpisode): The episode, with its source stats.
        stats (Dict[str, Dict[str, np.ndarray]]): By feature, its stats computed on export
            (see `LeRobotWriter._compute_episode_stats`).
    """
    for key, feature_stats in stats.items():
        source = episode.stats.get(key)
        if source is None or key in _RENUMBERED_COLUMNS:
            continue

        value_range = feature_stats["max"] - feature_stats["min"]
        for name, value in feature_stats.items():
            if name not in source:
                continue
            expected = np.asarray(source[name], dtype=np.float64)
            atol = (
                _CHECK_QUANTILE_ATOL_SHARE * value_range
                if name in _STATS_QUANTILES
                else _CHECK_ATOL
            )
            if expected.shape != value.shape or not np.allclose(
                value, expected, rtol=_CHECK_RTOL, atol=atol, equal_nan=True
            ):
                logger.warning(
                    f"Episode {episode.episode_index}: the `{name}` of `{key}` is "
                    f"{value.tolist()}, while the source one is {expected.tolist()}."
                )


def _to_timeseries_column(timeseries: pa.RecordBatch, key: str) -> pa.Array:
    """
    Returns the column of a timeseries feature from the batch of the timeseries topic, where
    the dotted names are nested into structs (see `_timeseries_fields_to_msco_schema`):
    `observation.state` is the field `state` of the column `observation`.

    Args:
        timeseries (pa.RecordBatch): The rows of the timeseries topic.
        key (str): The key of the feature (e.g. `observation.state`).

    Returns:
        pa.Array: The column of the feature.

    Raises:
        ValueError: If the field of the feature is missing from the timeseries topic.
    """
    missing = ValueError(
        f"Feature '{key}': topic '{_TIMESERIES_MSCO_TOPIC_NAME}' has no field '{key}'."
    )

    name, *children = key.split(_NESTED_NAME_SEP)
    if timeseries.schema.get_field_index(name) == _NOT_FOUND:
        raise missing
    column = timeseries.column(name)

    for child in children:
        if (
            not pa.types.is_struct(column.type)
            or column.type.get_field_index(child) == _NOT_FOUND
        ):
            raise missing
        # Unlike `StructArray.field`, `struct_field` applies the nulls of the struct
        column = pc.struct_field(column, child)
    return column


def _to_image_column(image: pa.RecordBatch, frame_index: pa.Array) -> pa.StructArray:
    """
    Converts the batch of a camera topic (the `CompressedImage` ontology) back into an image
    column: the opposite of `_adapt_image_rb_to_schema`.

    `data` becomes `bytes` as it is, and `header` is dropped. Of the original `path`, only the
    extension was kept (`format`): it is rebuilt as LeRobot names the images it embeds,
    `frame-{frame_index:06d}.{format}`, or `None` if `format` is empty (a `path` with no
    extension, or no `path`).

    The frames without an image, ingested with an empty `data` (the `CompressedImage` fields
    cannot be null), are null. An image file of 0 bytes is null too: it holds no image.

    Args:
        image (pa.RecordBatch): The rows of the camera topic.
        frame_index (pa.Array): The `frame_index` of the rows.

    Returns:
        pa.StructArray: The image column: a struct of `bytes` and `path`.
    """

    # Mosaico can stream the columns as views (`binary_view`, `string_view`), which the compute
    # functions below do not support: the casts are free on the plain types
    data = image.column(_IMAGE_DATA_FIELD).cast(pa.binary())
    image_format = image.column(_IMAGE_FORMAT_FIELD).cast(pa.string())

    # An empty `format` is no `format`: the `path` is then null (`emit_null` below)
    image_format = pc.if_else(
        pc.equal(image_format, ""), pa.scalar(None, image_format.type), image_format
    )
    frame_name = pc.utf8_lpad(
        frame_index.cast(pa.string()), width=_FRAME_INDEX_WIDTH, padding="0"
    )
    # The last argument of `binary_join_element_wise` is the separator
    path = pc.binary_join_element_wise(
        "frame-", frame_name, ".", image_format, "", null_handling="emit_null"
    )
    # A frame without an image was ingested with an empty `data`
    no_image = pc.equal(data, b"")

    return pa.StructArray.from_arrays(
        [data, path], names=LeRobotSchemaConverter.IMAGE_ARROW_TYPE.names, mask=no_image
    )


def _to_task_index_column(timeseries: pa.RecordBatch, tasks: List[str]) -> pa.Array:
    """
    Converts the `task` field of the timeseries topic (the task of each frame, ingested in
    place of `task_index`) back into the `task_index` column:
    the position of the task in `tasks`, as LeRobot reads it from `meta/tasks.parquet`.

    Args:
        timeseries (pa.RecordBatch): The rows of the timeseries topic.
        tasks (List[str]): The tasks of the exported dataset, in task index order (see
            `LeRobotDatasetMeta.tasks`).

    Returns:
        pa.Array: The `task_index` of the rows.

    Raises:
        ValueError: If the timeseries topic has no `task` field, or a frame has no task or a
            task not in `tasks`.
    """

    task = _to_timeseries_column(timeseries, _TASK_COLUMN).cast(pa.string())

    task_index = pc.index_in(task, value_set=pa.array(tasks, pa.string()))
    # `index_in` gives a null for a task not in `tasks`, and for a null task
    if task_index.null_count > 0:
        unknown_tasks = pc.unique(pc.filter(task, pc.is_null(task_index)))
        raise ValueError(
            f"Frames hold tasks not defined in the exported dataset: "
            f"{unknown_tasks.to_pylist()} (its tasks are those of its episodes in "
            "'meta/episodes')."
        )
    return task_index


class LeRobotWriter:
    """
    Writes the data of the episodes of an exported LeRobot dataset to its data parquet file.

    The schema of the file is built from the features of the dataset (see `schema`), before
    any episode is written. Each data file is opened, under the `root` of the dataset, when its
    first episode is written (see `write_episode`), and stays open until `close` (or the
    `with` block exits): a parquet file is only readable once closed, and cannot be reopened
    to append more episodes.

    ```python
    with LeRobotWriter(meta) as writer:
        for streamer in loader:
            writer.write_episode(streamer)
    ```
    """

    def __init__(self, meta: LeRobotDatasetMeta):
        """
        Args:
            meta (LeRobotDatasetMeta): The metadata of the exported dataset.

        Raises:
            ValueError: If a feature has an unsupported `dtype` or `shape` (see
                `LeRobotSchemaConverter.to_lerobot_pyarrow`).
        """
        self._meta = meta
        """The metadata of the exported dataset."""
        self._features = [f for f in meta.features if not f.is_video()]
        """The features stored in the data file: all but the videos."""
        self._schema = LeRobotSchemaConverter.to_lerobot_pyarrow(self._features)
        """The schema of the data parquet file, a field per feature of `_features`."""
        self._current_index: int = 0
        """The `index` of the next row: its position in the exported dataset."""
        self._file_writers: Dict[Path, pq.ParquetWriter] = {}
        """The writer of each data parquet file opened, keyed by its path."""

    def _get_file_writer(self, episode: LeRobotEpisode) -> pq.ParquetWriter:
        """
        Returns the writer of the data parquet file of an episode: the `data_path` template of
        `info` under `root`, filled with the `data/chunk_index` and `data/file_index` of the
        episode.

        The file is opened (creating its folders) for its first episode only, then its writer
        is reused: opening a parquet file empties it.
        """
        data_path = self._meta.root / self._meta.info.data_path.format(
            chunk_index=episode.data_chunk_index, file_index=episode.data_file_index
        )

        file_writer = self._file_writers.get(data_path)
        if file_writer is None:
            data_path.parent.mkdir(parents=True, exist_ok=True)
            file_writer = pq.ParquetWriter(data_path, self._schema)
            self._file_writers[data_path] = file_writer

        return file_writer

    def _to_lerobot_rb(
        self,
        episode: LeRobotEpisode,
        timestamps_ns: pa.Array,
        batches: Dict[str, pa.RecordBatch],
    ) -> pa.RecordBatch:
        """
        Converts a slice of an episode, as streamed from Mosaico (see
        `LeRobotEpisodeStreamer`), into rows of the data file: the opposite of `LeRobotReader`.

        The slices must be converted in order: the rows are placed at `_current_index`, the
        position of the next row in the exported dataset, which then moves past them.

        Each column is built from its feature:
        - `timestamp`: from `timestamps_ns`, back into seconds;
        - `episode_index`, `index` and `frame_index`: generated, as the episodes are
          renumbered in the exported dataset (they are not stored in Mosaico);
        - `task_index`: from the `task` field of the timeseries topic, as the position of the
          task in the tasks of the exported dataset (see `_to_task_index_column`);
        - an image: from its own topic (see `_to_image_column`);
        - a timeseries: from the timeseries topic (see `_to_timeseries_column`).

        Each column is then cast to its type in `schema`.

        Args:
            episode (LeRobotEpisode): The episode of the rows, renumbered in the exported
                dataset.
            timestamps_ns (pa.Array): The `timestamp_ns` of the rows, shared by all the topics.
            batches (Dict[str, pa.RecordBatch]): A batch per topic holding the rows, without
                `timestamp_ns`.

        Returns:
            pa.RecordBatch: The rows, with the schema of the data file.

        Raises:
            ValueError: If the rows do not fall within the episode at `_current_index` (a slice
                converted out of order, or an episode with more or fewer rows than its
                `length`), a feature is not supported, the field of a timeseries (or `task`)
                is missing from the timeseries topic, or a frame has a task not defined in the
                exported dataset.
            KeyError: If the topic of a feature is missing from `batches`.
            pa.ArrowInvalid: If a column cannot be cast to its type in `schema`.
        """
        batch_rows = len(timestamps_ns)

        # `frame_index` is the position of the row in its episode
        frame_index_start = self._current_index - episode.dataset_from_index
        if frame_index_start < 0 or frame_index_start + batch_rows > episode.length:
            raise ValueError(
                f"Rows {self._current_index} to {self._current_index + batch_rows} do not "
                f"fall within episode {episode.episode_index} (rows "
                f"{episode.dataset_from_index} to {episode.dataset_to_index})."
            )
        index = pa.array(
            np.arange(self._current_index, self._current_index + batch_rows)
        )
        frame_index = pa.array(
            np.arange(frame_index_start, frame_index_start + batch_rows)
        )

        columns: List[pa.Array] = []
        for feature, field in zip(self._features, self._schema, strict=True):
            if feature.is_timestamp():
                column = _from_timestamp_ns(timestamps_ns)
            elif feature.is_episode_index():
                column = pa.repeat(episode.episode_index, batch_rows)
            elif feature.is_index():
                column = index
            elif feature.is_frame_index():
                column = frame_index
            elif feature.is_task_index():
                column = _to_task_index_column(
                    batches[_TIMESERIES_MSCO_TOPIC_NAME], self._meta.tasks
                )
            elif feature.is_image():
                msco_topic_name = _dotted_name_to_topic_name(feature.key)
                column = _to_image_column(batches[msco_topic_name], frame_index)
            elif feature.is_timeserie():
                timeseries = batches[_TIMESERIES_MSCO_TOPIC_NAME]
                column = _to_timeseries_column(timeseries, feature.key)
            else:
                raise ValueError(
                    f"Feature '{feature.key}' is not supported (dtype '{feature.dtype}')."
                )

            columns.append(column.cast(field.type))

        # Compose the final RecordBatch to be written in the parquet file
        lerobot_batch = pa.RecordBatch.from_arrays(columns, schema=self._schema)

        # The next rows start after these ones
        self._current_index += batch_rows

        return lerobot_batch

    def _accumulate_stats_inputs(
        self,
        lerobot_rb: pa.RecordBatch,
        sampled_frames: pa.Array,
        inputs: DefaultDict[str, List[np.ndarray]],
    ) -> None:
        """
        Keeps what the stats of an episode need from rows of the episode, as written to the
        data file (see `_to_lerobot_rb`): the stats of `episode_index`, `index` and
        `task_index` are then those of the exported dataset. Only these copies are kept,
        never the slice, whose images can be very large.

        The `string` and `language` features are skipped, as in LeRobot.

        Args:
            lerobot_rb (pa.RecordBatch): Rows of the episode, with the schema of the data file.
            sampled_frames (pa.Array): The `frame_index` of the frames of the episode whose
                images go into the stats (see `_sample_indices`).
            inputs (DefaultDict[str, List[np.ndarray]]): By feature, its values in each slice
                so far, as `_compute_stats` takes them. A timeseries: a column per element of
                the last axis, and a row per frame (per frame and position on the other axes,
                for an array, as in LeRobot). An image: its sampled images, decoded (see
                `_decode_stats_image`); a frame without an image (written as null, see
                `_to_image_column`) is skipped.
        """
        is_sampled = pc.is_in(
            lerobot_rb.column(_FRAME_INDEX_COLUMN), value_set=sampled_frames
        )

        for feature in self._features:
            if feature.dtype in _NO_STATS_DTYPES:
                continue

            column = lerobot_rb.column(feature.key)
            if feature.is_image():
                sampled = pc.filter(column, pc.and_(is_sampled, pc.is_valid(column)))
                for image in sampled.to_pylist():
                    inputs[feature.key].append(
                        _decode_stats_image(image["bytes"], feature.is_depth_map())
                    )
            else:
                # Every axis but the last pooled into the rows, as in LeRobot
                values = (
                    column
                    if feature.is_scalar()
                    else pc.list_flatten(column, recursive=True)
                )
                inputs[feature.key].append(
                    values.to_numpy(zero_copy_only=False).reshape(-1, feature.shape[-1])
                )

    def _compute_episode_stats(
        self, inputs: Dict[str, List[np.ndarray]], length: int
    ) -> Dict[str, Dict[str, np.ndarray]]:
        """
        Computes the stats of an episode from its inputs (see `_accumulate_stats_inputs`), as
        LeRobot does (`compute_episode_stats` in `lerobot/datasets/compute_stats.py`). Those
        of an image are per channel, of shape `(channels, 1, 1)`, and divided by 255 but for
        a depth map. `count` is the `length` of the episode for every feature, the images
        included. A camera without any sampled image gets no stats.

        Args:
            inputs (Dict[str, List[np.ndarray]]): By feature, its values in each slice of the
                episode (its sampled images, for an image).
            length (int): The number of frames of the episode.

        Returns:
            Dict[str, Dict[str, np.ndarray]]: By feature, its stats (see `_compute_stats`)
                and `count` as `[length]`.
        """
        stats: Dict[str, Dict[str, np.ndarray]] = {}
        for feature in self._features:
            values = inputs.get(feature.key)
            if not values:
                continue
            feature_stats = _compute_stats(np.concatenate(values))
            if feature.is_image():
                scale = 1.0 if feature.is_depth_map() else 255.0
                feature_stats = {
                    name: value.reshape(-1, 1, 1) / scale
                    for name, value in feature_stats.items()
                }
            stats[feature.key] = {**feature_stats, "count": np.array([length])}
        return stats

    # --- Public API --- #

    @property
    def schema(self) -> pa.Schema:
        """The schema of the data parquet file: a column per feature, except the videos."""
        return self._schema

    def write_episode(self, streamer: LeRobotEpisodeStreamer) -> None:
        """
        Writes an episode, streamed from its Mosaico sequence, to its data parquet file: the
        `data_path` template of `info` under `root`, filled with the `data/chunk_index` and
        `data/file_index` of the episode. Its stats (see `_compute_episode_stats`) replace
        the source ones in the metadata (see `LeRobotDatasetMeta.set_episode_stats`), after
        a debug check against them when the logger is at `DEBUG` (see
        `_check_episode_stats`).

        The episode is written slice by slice, as streamed: only one slice is held in memory,
        never the whole episode, which can be very large. Each slice is a row group of the file
        (LeRobot writes a row group per episode instead). If an error is raised, the slices
        before it are already written.

        The episodes must be written in the order of the exported dataset, each one starting at
        the row the previous one ended at.

        Args:
            streamer (LeRobotEpisodeStreamer): The episode, as yielded by the loader.

        Raises:
            ValueError: If the episode does not end at its `dataset_to_index`, or a slice
                cannot be converted (see `_to_lerobot_rb`).
            KeyError: If a topic of the episode is missing (see `_to_lerobot_rb`).
        """
        episode = streamer.episode_metadata

        file_writer = self._get_file_writer(episode)

        sampled_frames = _sample_indices(episode.length)
        stats_inputs: DefaultDict[str, List[np.ndarray]] = defaultdict(list)
        for ts, b in streamer:
            lerobot_rb = self._to_lerobot_rb(episode, ts, b)
            file_writer.write_batch(lerobot_rb)
            self._accumulate_stats_inputs(lerobot_rb, sampled_frames, stats_inputs)

        # The next episode starts where this one ends in the metadata
        if self._current_index != episode.dataset_to_index:
            raise ValueError(
                f"Sequence '{streamer.sequence_name}': episode {episode.episode_index} ends at "
                f"row {self._current_index}, while it should end at row "
                f"{episode.dataset_to_index} (its `length` is {episode.length})."
            )

        stats = self._compute_episode_stats(stats_inputs, episode.length)
        if logger.isEnabledFor(logging.DEBUG):
            _check_episode_stats(episode, stats)
        self._meta.set_episode_stats(episode.episode_index, stats)

    def close(self) -> None:
        """
        Closes the data parquet files, writing their footer: a file cannot be read until then.
        """
        for file_writer in self._file_writers.values():
            file_writer.close()

    def __enter__(self) -> "LeRobotWriter":
        """Context manager support."""
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        """Ensures the data files are closed even if an error occurs in the `with` block."""
        self.close()
