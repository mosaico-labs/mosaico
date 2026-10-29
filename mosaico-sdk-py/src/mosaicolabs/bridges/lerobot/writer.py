"""
Writer of the LeRobot dataset extraction: writes the episodes streamed from Mosaico (see
`LeRobotEpisodeStreamer`) to the data parquet file of the exported dataset.

It is the opposite of `LeRobotReader`: the data columns are rebuilt from the topics of the
sequences, with the types LeRobot writes them with (see
`LeRobotSchemaConverter.to_lerobot_pyarrow`).
"""

from pathlib import Path
from typing import Dict, List

import numpy as np
import pyarrow as pa
import pyarrow.compute as pc
import pyarrow.parquet as pq

from ..helpers import _dotted_name_to_topic_name
from ..protocols.lerobot import LeRobotSchemaConverter
from .helpers import _from_timestamp_ns
from .metadata import LeRobotDatasetMeta, LeRobotEpisode
from .mosaico_loader import LeRobotEpisodeStreamer
from .reader import (
    _NESTED_NAME_SEP,
    _NOT_FOUND,
    _TIMESERIES_MSCO_TOPIC_NAME,
)

_IMAGE_DATA_FIELD = "data"
"""The field of the `CompressedImage` ontology holding the content of the image file."""

_IMAGE_FORMAT_FIELD = "format"
"""The field of the `CompressedImage` ontology holding the format of the image (e.g. `png`)."""

_FRAME_INDEX_WIDTH = 6
"""The number of digits of the frame index in the name of an image (`frame-000012.png`)."""


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
        - `episode_index`, `index`, `frame_index` and `task_index`: generated, as the episodes
          are renumbered in the exported dataset (they are not stored in Mosaico, or with the
          values of the source dataset);
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
                `length`), a feature is not supported, or the field of a timeseries is missing
                from the timeseries topic.
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
                # FIXME: the `task_index` stored in Mosaico refers to the tasks of the source
                # dataset, which are not stored. Set to 0 until Mosaico stores the task of each
                # frame (a `task` column), from which it will be rebuilt
                column = pa.repeat(0, batch_rows)
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

    # --- Public API --- #

    @property
    def schema(self) -> pa.Schema:
        """The schema of the data parquet file: a column per feature, except the videos."""
        return self._schema

    def write_episode(self, streamer: LeRobotEpisodeStreamer) -> None:
        """
        Writes an episode, streamed from its Mosaico sequence, to its data parquet file: the
        `data_path` template of `info` under `root`, filled with the `data/chunk_index` and
        `data/file_index` of the episode.

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
        """
        episode = streamer.episode_metadata

        file_writer = self._get_file_writer(episode)

        for ts, b in streamer:
            lerobot_rb = self._to_lerobot_rb(episode, ts, b)
            file_writer.write_batch(lerobot_rb)

        # The next episode starts where this one ends in the metadata
        if self._current_index != episode.dataset_to_index:
            raise ValueError(
                f"Sequence '{streamer.sequence_name}': episode {episode.episode_index} ends at "
                f"row {self._current_index}, while it should end at row "
                f"{episode.dataset_to_index} (its `length` is {episode.length})."
            )

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
