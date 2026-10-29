"""
LeRobot dataset reader.

This module reads the `data/` folder of a LeRobot v3.0 dataset with `pyarrow`, one episode at a time.

The data parquet file holding an episode is found from its `meta/episodes` row (see
`LeRobotDatasetMeta.get_parquet_file`). Each file can hold several episodes, stored
contiguously: the rows of the other episodes are filtered out on the `episode_index` column.

The row groups are decoded a batch at a time, not as a whole as the `pyarrow` dataset scanner
does: this bounds the memory used, and a row group of high-resolution images can hold more than
the 2 GiB `pyarrow` can decode at once within a nested column (the image struct).

The timeseries (every data column except `timestamp` and the images) are rearranged into a single
struct, without copying them: the Mosaico server rejects field names containing `.`, so dotted
column names are nested (`observation.state` becomes the field `state` of the struct `observation`).

Each image column (an image embedded in the data files, as a struct of the image file content
`bytes` and its file name `path`) is converted into the fields of the `CompressedImage` ontology,
without copying nor decoding the images.

The batches are cut and merged so that each one is as large as possible within the maximum batch
size accepted by the server.
"""

from pathlib import Path
from typing import Any, Dict, Iterator, List, Optional, Tuple, Type

import pyarrow as pa
import pyarrow.compute as pc
import pyarrow.ipc as pa_ipc
import pyarrow.parquet as pq

from mosaicolabs import CompressedImage, Header, Serializable
from mosaicolabs.models.core import Message, resolve_ontology_class

from ..helpers import _dotted_name_to_topic_name
from .helpers import _max_rows_within, _to_timestamp_ns
from .lerobot_message import LeRobotMessage
from .metadata import _TIMESTAMP_COLUMN_NAME, LeRobotDatasetMeta

DEFAULT_BATCH_SIZE = 131_072
"""The default maximum number of rows of a batch (the `pyarrow` default)."""

IMAGE_BATCH_SIZE = 100
"""The maximum number of rows read at a time from an image column, to bound the memory used."""

READ_BUFFER_SIZE = 32 * 1024 * 1024
"""The size of the buffer the column chunks are read through, instead of being loaded whole."""

_EPISODE_INDEX_COLUMN = "episode_index"
_NESTED_NAME_SEP = "."
_NOT_FOUND = -1
_TIMESERIES_MSCO_TOPIC_NAME = "/timeseries"
_TIMESERIES_ONTOLOGY_TAG = "LeRobotTimeserie"


def _timeseries_fields_to_msco_schema(fields: List[pa.Field]) -> pa.Schema:
    """
    Builds the Mosaico compatible schema of the timeseries from their parquet fields.

    The Mosaico server rejects field names containing `.`, so the fields with a dotted name are
    nested into structs, at any depth: the fields `action`, `observation.state` and
    `observation.force` become the field `action` and the struct `observation` holding `state`
    and `force`. A struct takes the position of its first field.

    Raises:
        ValueError: If a name is both a field and a struct (e.g. `observation` and `observation.state`).
    """
    tree: Dict[str, Any] = {}
    for field in fields:
        *parents, leaf = field.name.split(_NESTED_NAME_SEP)
        node = tree
        for depth, parent in enumerate(parents, start=1):
            node = node.setdefault(parent, {})
            if not isinstance(node, dict):
                name = _NESTED_NAME_SEP.join(parents[:depth])
                raise ValueError(f"'{name}' cannot be both a column and a struct.")
        if leaf in node:
            raise ValueError(f"'{field.name}' cannot be both a column and a struct.")
        node[leaf] = pa.field(leaf, field.type, field.nullable)

    def to_struct(node: Dict[str, Any]) -> pa.StructType:
        return pa.struct(
            [
                pa.field(name, to_struct(child)) if isinstance(child, dict) else child
                for name, child in node.items()
            ]
        )

    return pa.schema(list(to_struct(tree)))


def _adapt_timeseries_rb_to_schema(
    batch: pa.RecordBatch, schema: pa.Schema
) -> pa.RecordBatch:
    """
    Rearranges the columns of `batch` into the fields of `schema`, as grouped by
    `_timeseries_fields_to_msco_schema`.

    The column arrays are reused as they are: no data is copied.
    """

    def build(field: pa.Field, name: str) -> pa.Array:
        # A field is either a column of the batch, or the struct of the columns `name.*`
        if batch.schema.get_field_index(name) != _NOT_FOUND:
            return batch.column(name)
        return pa.StructArray.from_arrays(
            [
                build(child, f"{name}{_NESTED_NAME_SEP}{child.name}")
                for child in field.type
            ],
            fields=list(field.type),
        )

    return pa.RecordBatch.from_arrays(
        [build(field, field.name) for field in schema], schema=schema
    )


def _adapt_image_rb_to_schema(
    image: pa.StructArray, schema: pa.Schema
) -> pa.RecordBatch:
    """
    Converts an image column into the fields of `schema`, the `CompressedImage` ontology.

    The image column is a struct of the image file content (`bytes`) and of the name of the file
    LeRobot wrote it to (`path`): `bytes` becomes `data` as it is (the image is not copied nor
    decoded), the lowercase extension of `path` becomes `format`, and `header` is null.
    """

    def fill_none(array: pa.Array) -> pa.Array:
        return pc.fill_null(array, pa.scalar(b"", type=pa.binary()))

    def to_image_format(array: pa.Array) -> pa.Array:
        ext = pc.extract_regex(array, r"\.(?P<ext>[^./\\]+)$").field("ext")
        ext = pc.utf8_lower(ext)

        return ext

    column_names_transformation_mapper = {
        "bytes": fill_none,
        "path": to_image_format,
    }

    # flatten() applies the struct's own nulls (frames without an image) to its children
    children = dict(zip(image.type.names, image.flatten()))

    def apply_transformation(col_name: str) -> pa.Array:

        transformation = column_names_transformation_mapper[col_name]
        return transformation(children[col_name])

    header = pa.nulls(len(image), Header.__msco_pyarrow_struct__)

    trans_img = [apply_transformation(col_name) for col_name in image.type.names]

    return pa.RecordBatch.from_arrays(
        [header, *trans_img],
        schema=schema,
    )


def _to_lerobot_message(
    msco_topic: str,
    column_names: List[str],
    batch: pa.RecordBatch,
    msco_ontology: Type[Serializable],
) -> LeRobotMessage:
    """
    Splits a batch with the `timestamp_ns` column first (the wire schema) into the message
    pushed with `TopicWriter.push_batch`: the `timestamp_ns` column and the payload columns.

    No data is copied: `push_batch` merges them back into the same batch.
    """
    return LeRobotMessage(
        msco_topic,
        column_names,
        batch.column(0),
        batch.remove_column(0),
        msco_ontology,
        batch.num_rows,
    )


class LeRobotReader:
    """
    Reads the data parquet files of a LeRobot v3.0 dataset, one episode at a time.

    Example:
        ```python
        reader = LeRobotReader(root, client.max_batch_size_bytes)
        for message in reader.iter_timeseries_batches(0):
            ...
        for message in reader.iter_image_batches(0):
            ...
        ```
    """

    def __init__(
        self,
        root: Path,
        max_batch_size_bytes: int,
        meta: Optional[LeRobotDatasetMeta] = None,
        column_names: Optional[List[str]] = None,
    ):
        """
        Args:
            root (Path): The root folder of the dataset.
            max_batch_size_bytes (int): The maximum size of the batches sent to the server
                (see `MosaicoClient.max_batch_size_bytes`).
            meta (Optional[LeRobotDatasetMeta]): The dataset metadata, loaded from `root`.
            column_names (Optional[List[str]]): The image and timeseries columns to read (e.g.
                `observation.images.front`, `observation.state`). Every column is read if `None`,
                none if empty.

        Raises:
            FileNotFoundError: If the data file of the first episode, holding the schema of the
                data files, is missing (see `LeRobotDatasetMeta.read_schema`).
            ValueError: If the dataset has no episode, the data files have no `timestamp`
                column, or the timeseries columns cannot be nested (a name is both a column and
                a struct).
        """
        # Get default metadata from input root
        if not meta:
            meta = LeRobotDatasetMeta.load(root)

        self._meta = meta
        self._max_batch_size_bytes = max_batch_size_bytes

        schema = self._meta.read_schema()
        if _TIMESTAMP_COLUMN_NAME not in schema.names:
            raise ValueError(
                f"The data files have no '{_TIMESTAMP_COLUMN_NAME}' column."
            )

        # `None` selects every column, an empty list none of them
        self._image_column_names: List[str] = [
            key
            for key, feature in meta.features.items()
            if feature.is_image and (column_names is None or key in column_names)
        ]
        # The order of the columns is the order of the timeseries struct fields
        self._timeseries_column_names: List[str] = [
            key
            for key, feature in meta.features.items()
            if feature.is_timeserie and (column_names is None or key in column_names)
        ]

        self._timeseries_schema: pa.Schema = _timeseries_fields_to_msco_schema(
            [schema.field(name) for name in self._timeseries_column_names]
        )

    def _iter_episode_batches(
        self,
        episode_index: int,
        columns: Optional[List[str]] = None,
        batch_size: int = DEFAULT_BATCH_SIZE,
    ) -> Iterator[pa.RecordBatch]:
        """
        Reads the rows of an episode, in file order, as record batches.

        Only the data file holding the episode is read (see `LeRobotDatasetMeta.get_parquet_file`),
        `batch_size` rows at a time with `ParquetFile.iter_batches`. The rows of the other episodes
        of the file, if any, are filtered out.

        Args:
            episode_index (int): The episode index.
            columns (Optional[List[str]]): The columns to read, in this order (all of them if
                `None`).
            batch_size (int): The maximum number of rows of each batch. Batches can be smaller
                (e.g. the last one, or those holding rows of other episodes, filtered out).

        Yields:
            pa.RecordBatch: The episode rows, with the requested columns.

        Raises:
            FileNotFoundError: If the data file of the episode is missing.
            ValueError: If the episode is not defined in `meta/episodes`, or its number of
                rows in its data file differs from its `length`.
        """
        episode_parquet_path = self._meta.get_parquet_file(episode_index)
        episode_length = self._meta.episodes_map[episode_index].length

        # `episode_index` is always read, to keep the rows of the episode only
        read_columns = (
            None
            if columns is None
            else list(dict.fromkeys([*columns, _EPISODE_INDEX_COLUMN]))
        )

        # The data file can hold other episodes too
        episode_filter = pc.field(_EPISODE_INDEX_COLUMN) == episode_index

        with pq.ParquetFile(
            episode_parquet_path,
            pre_buffer=False,
            buffer_size=READ_BUFFER_SIZE,
        ) as data_file:
            # Check that the number of rows of the episode coincides with its length in the metadata
            episode_indexes = data_file.read(columns=[_EPISODE_INDEX_COLUMN])
            num_rows = episode_indexes.filter(episode_filter).num_rows
            if num_rows != episode_length:
                raise ValueError(
                    f"Episode {episode_index} has {num_rows} rows in "
                    f"'{episode_parquet_path}', but its length in 'meta/episodes' is "
                    f"{episode_length}."
                )

            for batch in data_file.iter_batches(
                batch_size=batch_size, columns=read_columns
            ):
                batch = batch.filter(episode_filter)

                if columns is not None:
                    batch = batch.select(columns)

                if batch.num_rows > 0:
                    yield batch

    def _fit_batch(
        self, batch: pa.RecordBatch
    ) -> Tuple[List[pa.RecordBatch], pa.RecordBatch]:
        """
        Cuts `batch` into batches as large as possible within `max_batch_size_bytes`.

        Returns:
            Tuple[List[pa.RecordBatch], pa.RecordBatch]: The full batches, and the rows left
                over, which fit within `max_batch_size_bytes` and can be sent together with the
                next rows. Both are slices of `batch`: no data is copied.

        Raises:
            ValueError: If a single row exceeds `max_batch_size_bytes`.
        """
        max_size = self._max_batch_size_bytes
        full_rbs: List[pa.RecordBatch] = []
        while pa_ipc.get_record_batch_size(batch) > max_size:
            rows = _max_rows_within(batch, max_size)
            if rows == 0:
                row_size = pa_ipc.get_record_batch_size(batch.slice(0, 1))
                raise ValueError(
                    f"Single row size {row_size} bytes > {max_size} bytes maximum allowed batch size."
                )

            full_rbs.append(batch.slice(offset=0, length=rows))
            batch = batch.slice(offset=rows)
        return full_rbs, batch

    # Public API functions

    def read_episode_timestamps(self, episode_index: int) -> pa.ChunkedArray:
        """
        Reads the `timestamp` column of an episode (seconds since the episode start) as float64,
        in file order.

        Raises:
            FileNotFoundError: If the data file of the episode is missing.
            ValueError: If the episode is not defined in `meta/episodes`, or its number of
                rows in its data file differs from its `length`.
        """
        return pa.chunked_array(
            [
                batch.column(0).cast(pa.float64())
                for batch in self._iter_episode_batches(
                    episode_index, [_TIMESTAMP_COLUMN_NAME]
                )
            ],
            # Needed when the episode has no rows: there is no batch to take the type from
            type=pa.float64(),
        )

    @property
    def n_episodes(self) -> int:
        """The number of episodes of the dataset (`total_episodes` in `meta/info.json`)."""
        return self._meta.n_episodes

    @property
    def episode_indexes(self) -> List[int]:
        """The indexes of the episodes defined in `meta/episodes`, in increasing order."""
        return self._meta.episode_indexes

    @property
    def timeseries_msco_topic_name(self) -> str:
        """The Mosaico topic of the timeseries (`/timeseries`)."""
        return _TIMESERIES_MSCO_TOPIC_NAME

    @property
    def timeseries_struct(self) -> pa.StructType:
        """
        The struct of the timeseries: every data column except `timestamp` and the images, with
        dotted names nested. It is the ontology schema of the timeseries topic (e.g. for
        `make_unmodeled_ontology_class`).
        """
        return pa.struct(self._timeseries_schema)

    @property
    def timeseries_ontology(self) -> Type[Serializable]:
        """The `Unmodeled` ontology of the timeseries topic (tag `LeRobotTimeserie`), built
        from `timeseries_struct`."""
        return resolve_ontology_class(
            ontology_tag=_TIMESERIES_ONTOLOGY_TAG, schema=self.timeseries_struct
        )

    @property
    def image_ontology(self) -> Type[Serializable]:
        """The ontology of the camera topics (`CompressedImage`)."""
        return CompressedImage

    @property
    def cameras_column_names(self) -> List[str]:
        """
        Returns the names of the selected image columns (the features of type `image`, restricted
        to `column_names`), as accepted by `iter_image_by_name_batches`.

        Returns:
            List[str]: The image column names (e.g. `observation.images.front`).
        """
        return list(self._image_column_names)

    @property
    def timeseries_column_names(self) -> List[str]:
        """The names of the selected timeseries columns, in the order of the timeseries struct
        fields."""
        return self._timeseries_column_names

    def get_msco_topic_name(self, column: str) -> Optional[str]:
        """
        Returns the Mosaico topic an accepted column is pushed to: `/timeseries` for a
        timeseries column, its own topic for an image column (e.g. `observation.images.front`
        -> `/observation/images/front`).

        Args:
            column (str): The column name.

        Returns:
            Optional[str]: The Mosaico topic name, or `None` if the column is not accepted.
        """

        if column in self.cameras_column_names:
            return _dotted_name_to_topic_name(column)
        if column in self.timeseries_column_names:
            return self.timeseries_msco_topic_name
        return None

    def iter_timeseries_batches(self, episode_index: int) -> Iterator[LeRobotMessage]:
        """
        Reads the timeseries of an episode, as the messages pushed to the timeseries topic.

        Each message holds the `timestamp_ns` column (the `int64` nanoseconds since the episode
        start, from `timestamp`) and the timeseries rows, whose schema holds the fields of
        `timeseries_struct`, as `TopicWriter.push_batch` takes them. The batch `push_batch`
        sends, their combination, is as large as possible within `max_batch_size_bytes`: only
        the last one of the episode can be smaller. Only the `timestamp` and timeseries columns
        are read: the images are never loaded. Nothing is yielded if no timeseries column is
        selected (see `column_names`).

        Args:
            episode_index (int): The episode index.

        Yields:
            LeRobotMessage: The timeseries topic name, the `timestamp_ns` column and the
                timeseries rows.

        Raises:
            ValueError: If a timeseries column collides with the `timestamp_ns` envelope column,
                the episode is not defined in `meta/episodes`, its number of rows in the data
                files differs from its `length`, or a single row exceeds `max_batch_size_bytes`.
        """
        # No timeseries topic to push: an empty struct is not a valid ontology
        if not self._timeseries_column_names:
            return

        all_columns = [_TIMESTAMP_COLUMN_NAME, *self._timeseries_column_names]

        # The wire schema: the `timestamp_ns` envelope column + timeseries schema fields
        ts_timeseries_schema: pa.Schema = Message._get_schema(
            pa.struct(list(self._timeseries_schema))
        )

        # The rows left over by the previous batch, sent together with the next rows
        pending_rb: Optional[pa.RecordBatch] = None

        for batch in self._iter_episode_batches(episode_index, all_columns):
            timestamp_array = _to_timestamp_ns(batch.column(_TIMESTAMP_COLUMN_NAME))
            timeseries_rb = _adapt_timeseries_rb_to_schema(
                batch.select(self._timeseries_column_names), self._timeseries_schema
            )

            merged_rb = pa.RecordBatch.from_arrays(
                [timestamp_array, *timeseries_rb.columns],
                schema=ts_timeseries_schema,
            )

            if pending_rb is not None:
                # Copies the rows: only when the episode is read in more than one batch
                merged_rb = pa.concat_batches([pending_rb, merged_rb])

            full_rbs, pending_rb = self._fit_batch(merged_rb)
            for rb in full_rbs:
                yield _to_lerobot_message(
                    self.timeseries_msco_topic_name,
                    self.timeseries_column_names,
                    rb,
                    self.timeseries_ontology,
                )

        if pending_rb is not None:
            yield _to_lerobot_message(
                self.timeseries_msco_topic_name,
                self.timeseries_column_names,
                pending_rb,
                self.timeseries_ontology,
            )

    def iter_image_by_name_batches(
        self, episode_index: int, camera_column_name: str
    ) -> Iterator[LeRobotMessage]:
        """
        Reads the images of a camera in an episode, as the messages pushed to its topic.

        Each message holds the `timestamp_ns` column (the `int64` nanoseconds since the episode
        start, from `timestamp`) and the image rows, whose schema holds the fields of the
        `CompressedImage` ontology: `header` (null), `data` (the image file content) and
        `format` (the lowercase extension of the image file name). The batch
        `TopicWriter.push_batch` sends, their combination, is as large as possible within
        `max_batch_size_bytes`: only the last one of the episode can be smaller. Only the
        `timestamp` and `camera_column_name` columns are read, `IMAGE_BATCH_SIZE` rows at a time.

        Args:
            episode_index (int): The episode index.
            camera_column_name (str): The image column to read (see `cameras_column_names`).

        Yields:
            LeRobotMessage: The camera topic name (from `camera_column_name`), the
                `timestamp_ns` column and the image rows.

        Raises:
            ValueError: If `camera_column_name` is not a selected image column, the episode is not
                defined in `meta/episodes`, its number of rows in the data files differs from
                its `length`, or a single row exceeds `max_batch_size_bytes`.
        """

        if camera_column_name not in self._image_column_names:
            raise ValueError(
                f"Requested camera column name {camera_column_name} is not contained among batch columns. "
                f"Available camera column names are: {[c_name for c_name in self._image_column_names]}"
            )

        # The wire schema: the `timestamp_ns` envelope column, then the image struct fields
        ts_image_schema: pa.Schema = Message._get_schema(CompressedImage)

        all_columns = [_TIMESTAMP_COLUMN_NAME, camera_column_name]

        msco_topic_name = self.get_msco_topic_name(camera_column_name)

        # The rows left over by the previous batch, sent together with the next rows
        pending_rb: Optional[pa.RecordBatch] = None

        for batch in self._iter_episode_batches(
            episode_index, all_columns, IMAGE_BATCH_SIZE
        ):
            timestamp_array = _to_timestamp_ns(batch.column(_TIMESTAMP_COLUMN_NAME))
            image_rb = _adapt_image_rb_to_schema(
                batch.column(camera_column_name),
                pa.schema(list(CompressedImage.__msco_pyarrow_struct__)),
            )

            merged_rb = pa.RecordBatch.from_arrays(
                [timestamp_array, *image_rb.columns],
                schema=ts_image_schema,
            )

            if pending_rb is not None:
                # Copies the rows: only when the episode is read in more than one batch
                merged_rb = pa.concat_batches([pending_rb, merged_rb])

            full_rbs, pending_rb = self._fit_batch(merged_rb)
            for rb in full_rbs:
                yield _to_lerobot_message(
                    msco_topic_name or "?",
                    [camera_column_name],
                    rb,
                    self.image_ontology,
                )

        if pending_rb is not None:
            yield _to_lerobot_message(
                msco_topic_name or "?",
                [camera_column_name],
                pending_rb,
                self.image_ontology,
            )

    def iter_image_batches(self, episode_index: int) -> Iterator[LeRobotMessage]:
        """
        Reads the images of every camera in an episode, as the messages pushed to their topics.

        The cameras are read one after the other, in the order of `cameras_column_names`, and
        each message holds the topic name of its camera (see `iter_image_by_name_batches`).

        Args:
            episode_index (int): The episode index.

        Yields:
            LeRobotMessage: The camera topic name, the `timestamp_ns` column and the image rows.

        Raises:
            ValueError: If the episode is not defined in `meta/episodes`, its number of rows in
                the data files differs from its `length`, or a single row exceeds
                `max_batch_size_bytes`.
        """
        for camera_col_name in self.cameras_column_names:
            yield from self.iter_image_by_name_batches(episode_index, camera_col_name)
