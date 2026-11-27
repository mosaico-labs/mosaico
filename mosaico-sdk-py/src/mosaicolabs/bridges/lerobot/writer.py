"""
Writer of the LeRobot dataset extraction: writes the episodes streamed from Mosaico (see
`LeRobotEpisodeStreamer`) to the data parquet file of the exported dataset.

It is the opposite of `LeRobotReader`: the data columns are rebuilt from the topics of the
sequences, with the types LeRobot writes them with (see
`LeRobotSchemaConverter.to_lerobot_pyarrow`).
"""

from dataclasses import replace
from pathlib import Path
from typing import Dict, List, Optional, Tuple

import numpy as np
import pyarrow as pa
import pyarrow.compute as pc
import pyarrow.parquet as pq

from ..helpers import _dotted_name_to_topic_name
from ..protocols.lerobot import LeRobotSchemaConverter
from .helpers import _from_timestamp_ns
from .metadata import LeRobotEpisode, LeRobotInfo
from .mosaico_loader import LeRobotEpisodeStreamer
from .reader import (
    _NESTED_NAME_SEP,
    _NOT_FOUND,
    _TASK_COLUMN,
    _TIMESERIES_MSCO_TOPIC_NAME,
)
from .ui_lerobot import LeRobotProgressManager

_IMAGE_DATA_FIELD = "data"
"""The field of the `CompressedImage` ontology holding the content of the image file."""

_IMAGE_FORMAT_FIELD = "format"
"""The field of the `CompressedImage` ontology holding the format of the image (e.g. `png`)."""

_FRAME_INDEX_WIDTH = 6
"""The number of digits of the frame index in the name of an image (`frame-000012.png`)."""

_ROW_GROUP_MAX_BYTES = 64 * 1024 * 1024
"""The size of the rows buffered before they are written as one row group of a data file: it
bounds the memory held while writing an episode, whatever its length."""

_TMP_SUFFIX = ".tmp"
"""The suffix of the file a data file is written to, until it is complete (see `_tmp_path`)."""

_DEFAULT_CHUNKS_SIZE = 1000
"""The maximum number of files of a `chunk-XXX` folder when `info` has no `chunks_size`, as
in LeRobot."""

_DEFAULT_DATA_FILES_SIZE_IN_MB = 100
"""The size, in MB, beyond which a new data file is started when `info` has no
`data_files_size_in_mb`, as in LeRobot."""


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


def _next_file_index(
    chunk_index: int, file_index: int, chunks_size: int
) -> Tuple[int, int]:
    """
    Returns the data file after the one at `chunk_index` and `file_index`, as LeRobot numbers
    them: the next file of the chunk, or the first file of the next chunk once the chunk holds
    `chunks_size` files.

    Args:
        chunk_index (int): The chunk of the current data file.
        file_index (int): The current data file in its chunk.
        chunks_size (int): The maximum number of files of a chunk.

    Returns:
        Tuple[int, int]: The `chunk_index` and `file_index` of the next data file.
    """
    if file_index == chunks_size - 1:
        return chunk_index + 1, 0
    return chunk_index, file_index + 1


def _tmp_path(data_path: Path) -> Path:
    """
    Returns the file a data file is written to (`file-000.parquet.tmp`), renamed to
    `data_path` once complete (see `LeRobotWriter._close_file`).

    Args:
        data_path (Path): The data parquet file.

    Returns:
        Path: The temporary file of `data_path`.
    """
    return data_path.with_name(data_path.name + _TMP_SUFFIX)


class LeRobotWriter:
    """
    Writes the data of the episodes of an exported LeRobot dataset to its data parquet files.

    The schema of the files is built from the features of the dataset (see `schema`), before
    any episode is written. The episodes are written one after the other to the current data
    file, and a new data file is started before an episode that would make the current one
    exceed `data_files_size_in_mb`, as LeRobot does (see `_start_episode`). An episode is never
    split across two data files.

    A data file is written to a temporary file next to it (see `_tmp_path`), renamed to the
    data file once it is complete: when the next episode goes to another data file, or when
    the writer is closed with no error. A failed episode stops the export: when the `with`
    block exits on an error, the data files are deleted, the complete ones too, and no data
    file is left.

    ```python
    with LeRobotWriter(info, root, tasks) as writer:
        episodes = [writer.write_episode(streamer, ui=None) for streamer in loader]
    ```
    """

    def __init__(self, info: LeRobotInfo, root: Path, tasks: List[str]):
        """
        Args:
            info (LeRobotInfo): The `info` of the exported dataset: its features, in the order
                of `meta/info.json` (the video features are skipped: they are not stored in the
                data files), the `data_path` template of its data files, and its
                `chunks_size` and `data_files_size_in_mb` (LeRobot's defaults if missing).
            root (Path): The root folder of the exported dataset.
            tasks (List[str]): The tasks of the exported dataset, in task index order: the
                `task_index` of a frame is the position of its task in this list.

        Raises:
            ValueError: If a feature has an unsupported `dtype` or `shape` (see
                `LeRobotSchemaConverter.to_lerobot_pyarrow`).
        """
        self._info = info
        """The `info` of the exported dataset, giving the paths of its data files."""
        self._root = root
        """The root folder of the exported dataset."""
        self._features = [f for f in info.features if not f.is_video()]
        """The features stored in the data file: all but the videos."""
        self._tasks = tasks
        """The tasks of the exported dataset, in task index order."""
        self._schema = LeRobotSchemaConverter.to_lerobot_pyarrow(self._features)
        """The schema of the data parquet file, a field per feature of `_features`."""
        self._chunks_size = (
            _DEFAULT_CHUNKS_SIZE if info.chunks_size is None else info.chunks_size
        )
        """The maximum number of data files of a chunk."""
        self._data_files_size_in_mb = (
            _DEFAULT_DATA_FILES_SIZE_IN_MB
            if info.data_files_size_in_mb is None
            else info.data_files_size_in_mb
        )
        """The size, in MB, beyond which a new data file is started."""
        self._current_index: int = 0
        """The `index` of the next row: its position in the exported dataset."""
        self._file_index: Tuple[int, int] = (0, 0)
        """The `chunk_index` and `file_index` of the current data file."""
        self._file_writer: Optional[pq.ParquetWriter] = None
        """The writer of the current data file, or `None` if it is not open yet."""
        self._file_rows: int = 0
        """The number of rows written to the current data file."""
        self._saved_paths: List[Path] = []
        """The data files complete and renamed (see `_close_file`)."""

    def _data_path(self) -> Path:
        """Returns the path of the current data file (see `LeRobotInfo.get_path_to_data`)."""
        return self._info.get_path_to_data(self._root, *self._file_index)

    def _close_file(self, save: bool) -> None:
        """
        Closes the current data file, writing its footer: a parquet file cannot be read until
        then, nor reopened to append more rows.

        Args:
            save (bool): Whether to rename the temporary file to the data file, or to delete it.
        """
        if self._file_writer is None:
            return

        self._file_writer.close()
        self._file_writer = None
        self._file_rows = 0

        data_path = self._data_path()
        if save:
            # Atomic: the data file is never seen half-written
            _tmp_path(data_path).replace(data_path)
            self._saved_paths.append(data_path)
        else:
            _tmp_path(data_path).unlink(missing_ok=True)

    def _start_episode(self, length: int) -> pq.ParquetWriter:
        """
        Returns the writer of the data file of the next episode, as LeRobot chooses it: the
        current data file, unless its size, plus the average size of its rows times `length`,
        would reach `data_files_size_in_mb`. The current data file is then closed and the next
        one is started (see `_next_file_index`). The first data file holds at least the first
        episode, whatever its size.

        Args:
            length (int): The number of rows of the next episode.

        Returns:
            pq.ParquetWriter: The writer of the data file of the episode, writing to its
                temporary file (see `_tmp_path`).
        """
        if self._file_writer is not None and self._file_rows > 0:
            size_in_mb = _tmp_path(self._data_path()).stat().st_size / 1024**2
            size_per_row = size_in_mb / self._file_rows

            if size_in_mb + size_per_row * length >= self._data_files_size_in_mb:
                self._close_file(save=True)
                self._file_index = _next_file_index(
                    *self._file_index, self._chunks_size
                )

        if self._file_writer is None:
            data_path = self._data_path()
            data_path.parent.mkdir(parents=True, exist_ok=True)
            self._file_writer = pq.ParquetWriter(_tmp_path(data_path), self._schema)

        return self._file_writer

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
                    batches[_TIMESERIES_MSCO_TOPIC_NAME], self._tasks
                )
            elif feature.is_image():
                msco_topic_name = _dotted_name_to_topic_name(feature.key)
                column = _to_image_column(batches[msco_topic_name], frame_index)
            elif feature.is_timeseries():
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

    def write_episode(
        self,
        streamer: LeRobotEpisodeStreamer,
        ui: Optional[LeRobotProgressManager],
    ) -> LeRobotEpisode:
        """
        Writes an episode, streamed from its Mosaico sequence, to its data parquet file (see
        `_start_episode`).

        The episode is converted slice by slice, as streamed, and the slices are buffered up
        to `_ROW_GROUP_MAX_BYTES`, then written as one row group: the memory held is bounded
        by that limit, never by the whole episode, which can be very large.

        The episodes must be written in the order of the exported dataset, each one starting at
        the row the previous one ended at. A failed episode stops the export: the data files
        are deleted when the `with` block exits on its error, none is left.

        Args:
            streamer (LeRobotEpisodeStreamer): The episode, as yielded by the loader.
            ui (Optional[LeRobotProgressManager]): The progress bars, whose topic bars are
                advanced by the rows of each slice converted, or `None` for no progress bars.

        Returns:
            LeRobotEpisode: The episode, with the `data/chunk_index` and `data/file_index` of
                the data file it was written to. The episode of the streamer is left unchanged.

        Raises:
            ValueError: If the episode does not end at its `dataset_to_index`, or a slice
                cannot be converted (see `_to_lerobot_rb`).
            KeyError: If a topic of the episode is missing (see `_to_lerobot_rb`). The
                `MosaicoToLeRobotLoader` drops such sequences beforehand (see `_check_topics`).
        """
        episode = streamer.episode_metadata

        file_writer = self._start_episode(episode.length)

        # The slices not written yet: kept as a list, not concatenated, to copy them only once
        buffer: List[pa.RecordBatch] = []
        buffer_bytes = 0

        for ts, b in streamer:
            lerobot_rb = self._to_lerobot_rb(episode, ts, b)
            buffer.append(lerobot_rb)
            buffer_bytes += lerobot_rb.nbytes

            if buffer_bytes >= _ROW_GROUP_MAX_BYTES:
                # The buffered slices, written as one row group
                file_writer.write_table(pa.Table.from_batches(buffer, self._schema))
                buffer, buffer_bytes = [], 0

            advance = len(ts)
            topics = [topic for topic in b]

            if ui is not None:
                ui.advance_all_topics(topics, advance)

        # The last rows of the episode: a row group never holds two episodes
        if buffer:
            file_writer.write_table(pa.Table.from_batches(buffer, self._schema))

        # The next episode starts where this one ends in the metadata
        if self._current_index != episode.dataset_to_index:
            raise ValueError(
                f"Sequence '{streamer.sequence_name}': episode {episode.episode_index} ends at "
                f"row {self._current_index}, while it should end at row "
                f"{episode.dataset_to_index} (its `length` is {episode.length})."
            )
        self._file_rows += episode.length

        chunk_index, file_index = self._file_index
        return replace(
            episode, data_chunk_index=chunk_index, data_file_index=file_index
        )

    def close(self, save_files: bool = True) -> None:
        """
        Closes the current data parquet file (see `_close_file`). Calling `close` again does
        nothing.

        Args:
            save_files (bool): Whether to keep the data files, or to delete them, the complete
                ones too (after a failed episode: the dataset would be incomplete).
        """
        self._close_file(save=save_files)

        if not save_files:
            for data_path in self._saved_paths:
                data_path.unlink(missing_ok=True)

        self._saved_paths.clear()

    def __enter__(self) -> "LeRobotWriter":
        """Context manager support."""
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        """
        Closes the data files when the `with` block exits: they are kept only if it exits
        with no error, and deleted otherwise (see `close`).
        """
        self.close(save_files=exc_type is None)
