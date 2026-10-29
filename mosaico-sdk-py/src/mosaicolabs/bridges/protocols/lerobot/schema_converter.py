"""
Conversion of the features of a LeRobot dataset into `pyarrow` schemas, in the two directions
of the LeRobot bridge:
- `to_lerobot_pyarrow`: the schema of the data parquet files, as LeRobot writes them (the
  export, see `LeRobotWriter`);
- `to_mosaico_pyarrow`: the schema the timeseries are pushed to Mosaico with (the ingestion,
  see `LeRobotReader`). Not implemented yet.
"""

from typing import ClassVar, List

import pyarrow as pa

from mosaicolabs.bridges.lerobot.metadata import LeRobotFeature


class LeRobotSchemaConverter:
    """
    Converts the features of a LeRobot dataset (see `LeRobotFeature`) into `pyarrow` schemas.

    Unlike the MCAP converters (see `McapSchemaConverter`), there is a single format to convert,
    hence no registry: the two methods are the two directions of the LeRobot bridge.
    """

    IMAGE_ARROW_TYPE: ClassVar[pa.StructType] = pa.struct(
        [pa.field("bytes", pa.binary()), pa.field("path", pa.string())]
    )
    """The type of an image column: the content of the image file (`bytes`) and its file
    name (`path`)."""

    _LIST_ITEM_NAME: ClassVar[str] = "element"
    """The name of the items of the list columns, as in the parquet files written by LeRobot."""

    @classmethod
    def to_lerobot_pyarrow(cls, features: List[LeRobotFeature]) -> pa.Schema:
        """
        Returns the schema of the data parquet files of a dataset: a column per feature, in the
        order of the features, except the videos (stored in separate video files). Each column
        has the type LeRobot writes it with (see `_feature_to_lerobot_type`).

        Args:
            features (List[LeRobotFeature]): The features of the dataset.

        Returns:
            pa.Schema: The schema of the data files.

        Raises:
            ValueError: If a feature has an unsupported `dtype` or `shape`.
        """
        return pa.schema(
            [
                pa.field(feature.key, cls._feature_to_lerobot_type(feature))
                for feature in features
                if not feature.is_video()
            ]
        )

    @classmethod
    def to_mosaico_pyarrow(cls, features: List[LeRobotFeature]) -> pa.StructType:
        """
        Returns the struct the timeseries of a dataset are pushed to Mosaico with: the ontology
        schema of the timeseries topic (the camera topics need no conversion: they use the
        `CompressedImage` ontology).

        TODO: build it from the timeseries fields of `to_lerobot_pyarrow`, nested as
        `_timeseries_fields_to_msco_schema` does in `LeRobotReader`, so that the reader no
        longer needs the schema of the data files.
        """
        raise NotImplementedError

    @classmethod
    def _feature_to_lerobot_type(cls, feature: LeRobotFeature) -> pa.DataType:
        """
        Returns the type of the data column of a feature, as LeRobot writes it (through the
        `datasets` features, see `get_hf_features_from_features` in LeRobot):
        - an image: a struct of the content of the image file (`bytes`) and its file name
          (`path`);
        - a value of shape `(1,)`: a scalar of the feature `dtype`;
        - a vector (one dimension): a fixed size list of the feature `dtype`;
        - an array of 2 to 5 dimensions: as many nested lists of the feature `dtype`.

        Args:
            feature (LeRobotFeature): A feature stored in the data files (not a video).

        Returns:
            pa.DataType: The type of the column of the feature.

        Raises:
            ValueError: If the `dtype` of the feature is not a `pyarrow` type, or its `shape`
                has no dimension or more than 5.
        """
        if feature.is_image():
            return cls.IMAGE_ARROW_TYPE

        try:
            arrow_type = pa.type_for_alias(feature.dtype)
        except ValueError as e:
            raise ValueError(
                f"Feature '{feature.key}' has an unsupported dtype '{feature.dtype}'."
            ) from e

        if feature.is_scalar():
            return arrow_type
        if feature.is_list():
            return pa.list_(pa.field(cls._LIST_ITEM_NAME, arrow_type), feature.shape[0])
        if feature.is_nested_list():
            # As `datasets` stores its `ArrayXD` features: variable size lists, one per
            # dimension
            for _ in feature.shape:
                arrow_type = pa.list_(pa.field(cls._LIST_ITEM_NAME, arrow_type))
            return arrow_type

        raise ValueError(
            f"Feature '{feature.key}' has an unsupported shape {list(feature.shape)}."
        )
