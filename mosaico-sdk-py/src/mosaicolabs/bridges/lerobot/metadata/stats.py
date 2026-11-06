"""
The stats of a LeRobot dataset, computed as LeRobot does (`lerobot/datasets/compute_stats.py`):
those of each episode (the `stats/<feature>/<stat>` columns of `meta/episodes`) from its rows in
the data files (see `compute_episode_stats`), and those of the dataset (`meta/stats.json`)
combined from those of its episodes (see `LeRobotStats.aggregate`). The stats of a feature are
held by a `LeRobotStats`.
"""

import io
from dataclasses import dataclass, field
from pathlib import Path
from typing import Any, Dict, List, Tuple

import numpy as np
import pyarrow as pa
import pyarrow.compute as pc
import pyarrow.dataset as ds
from PIL import Image as PILImage

from .feature import _EPISODE_INDEX_COLUMN, _FRAME_INDEX_COLUMN, LeRobotFeature

_NO_STATS_DTYPES = ("string", "language")
"""The `dtype` of the features LeRobot computes no stats for."""

_IMAGE_BYTES_FIELD = "bytes"
"""The field of an image column holding the content of the image file."""

_IMAGE_TARGET_SIZE = 150
"""The size an image is downsampled to on its longer side, for the stats (`target_size` of
`auto_downsample_height_width` in LeRobot)."""

_IMAGE_DOWNSAMPLE_THRESHOLD = 300
"""The size of the longer side from which an image is downsampled, for the stats
(`max_size_threshold` of `auto_downsample_height_width` in LeRobot)."""

_STATS_QUANTILES = {"q01": 0.01, "q10": 0.10, "q50": 0.50, "q90": 0.90, "q99": 0.99}
"""The quantiles of the stats, by name (`DEFAULT_QUANTILES` in LeRobot)."""


def _is_quantile(name: str) -> bool:
    """Whether a stat is one of the quantiles LeRobot computes (`_STATS_QUANTILES`)."""
    return name in _STATS_QUANTILES


def _decode_image(column: pa.ChunkedArray, is_depth_map: bool) -> np.ndarray:
    """
    Decodes the images of a column into the values their stats are computed over: a row per
    pixel of every image, and a column per channel.

    Each image is decoded as LeRobot loads it: as RGB, or in its own dtype if the feature is
    flagged as a depth map. It is then downsampled by striding, when its longer side is
    `_IMAGE_DOWNSAMPLE_THRESHOLD` pixels or more, to about `_IMAGE_TARGET_SIZE` pixels on
    that side.

    The values are then divided by 255, into `[0, 1]`, unless the feature is flagged as a
    depth map, as LeRobot does.

    Args:
        column (pa.ChunkedArray): The images, at least one and none null: structs whose
            `bytes` field holds the content of the image file (e.g. PNG).
        is_depth_map (bool): Whether the feature of the images is flagged as a depth map (see
            `LeRobotFeature.is_depth_map`).

    Returns:
        np.ndarray: The pixels of the images, of shape `(pixels, channels)`, in `float64`.

    Examples:
        ```
        2 RGB images of 4 x 4:        2 * 16 = 32 rows of 3 channels, in [0, 1]   (32, 3)
        1 RGB image of 640 x 480:     downsampled by 640 // 150 = 4,
                                      to 160 x 120 = 19,200 rows                  (19200, 3)
        1 depth map of 4 x 4, flagged: 16 rows of 1 channel, not divided          (16, 1)
        ```
    """
    pixels: List[np.ndarray] = []
    for data in pc.struct_field(column, _IMAGE_BYTES_FIELD).to_pylist():
        pil_image = PILImage.open(io.BytesIO(data))

        if not is_depth_map:
            pil_image = pil_image.convert("RGB")
            image = np.asarray(pil_image, dtype=np.float64) / 255.0
        else:
            image = np.asarray(pil_image, dtype=np.float64)

        # (height, width, channels): a depth map has no channel axis, it gets one
        image = np.atleast_3d(image)

        # Subsampling: if the longer side is 300 px or more, stride it down to about 150 px
        long_side = max([pil_image.width, pil_image.height])
        stride = (
            long_side // _IMAGE_TARGET_SIZE
            if long_side >= _IMAGE_DOWNSAMPLE_THRESHOLD
            else 1
        )
        image = image[::stride, ::stride]

        # Final shape needs to be: Rows x Channels
        channels = image.shape[-1]
        pixels.append(image.reshape(-1, channels))

    return np.concatenate(pixels)


def _to_matrix(feature: LeRobotFeature, column: pa.ChunkedArray) -> np.ndarray:
    """
    Converts the column of a timeseries feature into the matrix its stats are computed over,
    as LeRobot does: a column per element of the last axis of the feature, and a row per frame
    (per frame and position on the other axes, for an array of 2 or more dimensions). Its
    values are flattened, frame after frame, then cut into rows of `feature.shape[-1]` values.

    Args:
        feature (LeRobotFeature): The feature of the column, a timeseries.
        column (pa.ChunkedArray): The column of the feature in the episode, a value per frame.

    Returns:
        np.ndarray: The matrix, of shape `(rows, feature.shape[-1])`.

    Examples:
        How the column is reshaped, over a few frames:

        ```
        scalar, shape (1,), e.g. `timestamp`: a single column
            column     [0.0, 0.1, 0.2]
            reshaped   [[0.0],
                        [0.1],
                        [0.2]]                            (3, 1): stats of 1 value

        vector, shape (2,), e.g. 2 joints: a column per joint
            column     [[1, 10], [2, 20], [3, 30]]
            flattened  [1, 10, 2, 20, 3, 30]
            reshaped   [[1, 10],
                        [2, 20],
                        [3, 30]]                          (3, 2): stats of 2 values

        array, shape (2, 3), e.g. 2 points of (x, y, z): a row per point of each frame, a
        column per coordinate
            column     [[[0, 1, 2], [10, 11, 12]], [[100, 101, 102], [110, 111, 112]]]
            flattened  [0, 1, 2, 10, 11, 12, 100, 101, 102, 110, 111, 112]
            reshaped   [[  0,   1,   2],
                        [ 10,  11,  12],
                        [100, 101, 102],
                        [110, 111, 112]]                  (4, 3): stats of 3 values
        ```

        The stats of the array are then those of each coordinate, over every point of every
        frame: e.g. its mean is `[55, 56, 57]`.
    """
    if not feature.is_scalar():
        # Every axis but the last pooled into the rows, as in LeRobot
        column = pc.list_flatten(column, recursive=True)

    return column.to_numpy().reshape(-1, feature.shape[-1])


def _compute_stats(values: np.ndarray, shape: Tuple[int, ...]) -> Dict[str, np.ndarray]:
    """
    Computes the stats of a matrix, per column: min, max, mean, std (divided by n) and the
    quantiles of `_STATS_QUANTILES`, in `float64`. The quantiles are exact, unlike LeRobot,
    which estimates them with a histogram: the first value where the count reaches `q * n`
    (`inverted_cdf`), without interpolating between two values.

    Args:
        values (np.ndarray): The matrix, of shape `(rows, columns)`, with at least one row
            (see `_to_matrix` and `_decode_image`).
        shape (Tuple[int, ...]): The shape each stat is reshaped to, from `(columns,)`: e.g.
            `(-1,)` for a timeseries, `(-1, 1, 1)` for an image (see
            `LeRobotStats.from_column`).

    Returns:
        Dict[str, np.ndarray]: Each stat, of shape `shape`.
    """
    values = values.astype(np.float64, copy=False)
    quantiles = np.quantile(
        values, list(_STATS_QUANTILES.values()), axis=0, method="inverted_cdf"
    )
    return {
        "min": values.min(axis=0).reshape(shape),
        "max": values.max(axis=0).reshape(shape),
        "mean": values.mean(axis=0).reshape(shape),
        "std": values.std(axis=0).reshape(shape),
        **{
            q_name: value.reshape(shape)
            for q_name, value in zip(_STATS_QUANTILES, quantiles)
        },
    }


@dataclass(frozen=True, eq=False)
class LeRobotStats:
    """
    The stats of a feature, as LeRobot computes them: in an episode (its `stats/<feature>/...`
    columns of `meta/episodes`) or in the dataset (its entry of `meta/stats.json`).

    Each stat holds a value per element of the last axis of the feature, of shape
    `(shape[-1],)`, for a timeseries, or a value per channel, of shape `(channels, 1, 1)`, for
    an image.

    The quantiles are those of `_STATS_QUANTILES`, by name: the LeRobot versions before them
    wrote none.

    The stats are checked to be feasible on creation (see `__post_init__`).

    `eq=False`: the `__eq__` generated by `dataclass` cannot compare numpy arrays.
    """

    min: np.ndarray
    """The lowest value."""

    max: np.ndarray
    """The highest value."""

    mean: np.ndarray
    """The mean."""

    std: np.ndarray
    """The standard deviation (divided by n)."""

    count: int
    """The number of frames the stats are of."""

    quantiles: Dict[str, np.ndarray] = field(default_factory=dict)
    """The quantiles, by name (e.g. `q01`), possibly none."""

    def __post_init__(self):
        """
        Checks that the stats are feasible: all of the same shape, of at least one frame, with
        no min higher than its max and no negative std. A NaN value (e.g. of a feature holding
        NaN) passes these checks.

        Raises:
            ValueError: If the stats are not feasible.
        """
        stats = {
            "min": self.min,
            "max": self.max,
            "mean": self.mean,
            "std": self.std,
            **self.quantiles,
        }
        shapes = {name: value.shape for name, value in stats.items()}
        if len(set(shapes.values())) > 1:
            raise ValueError(f"The stats are not all of the same shape: {shapes}.")

        if self.count < 1:
            raise ValueError(
                f"The stats are of {self.count} frames, instead of 1 or more."
            )

        # Checked as violations, so that NaN (whose comparisons are all false) passes
        if np.any(self.min > self.max):
            raise ValueError(f"The min {self.min} is higher than the max {self.max}.")
        if np.any(self.std < 0):
            raise ValueError(f"The std {self.std} is negative.")

    @classmethod
    def from_column(
        cls, feature: LeRobotFeature, column: pa.ChunkedArray, count: int
    ) -> "LeRobotStats":
        """
        Computes the stats of a feature from its column in an episode, as LeRobot does:

        - a timeseries: over every frame, a value per element of the last axis of the
          feature (see `_to_matrix`), of shape `(shape[-1],)`;
        - an image: over every pixel of the sampled images, a value per channel (see
          `_decode_image`), of shape `(channels, 1, 1)`, as LeRobot stores them.

        Args:
            feature (LeRobotFeature): The feature of the column, a timeseries or an image (not
                a video, nor a `string` or `language`).
            column (pa.ChunkedArray): The column of the feature in the episode, with at least
                one value and none null: a value per frame for a timeseries, the sampled
                images for an image.
            count (int): The number of frames of the episode: for an image all of them, not
                only the sampled ones, as LeRobot counts them.

        Returns:
            LeRobotStats: The stats of the feature in the episode.
        """
        if feature.is_image():
            values = _decode_image(column, feature.is_depth_map())
            shape = (-1, 1, 1)
        else:
            values = _to_matrix(feature, column)
            shape = (-1,)

        stats = _compute_stats(values, shape)
        return cls.from_dict({**stats, "count": count})

    @classmethod
    def from_dict(cls, stats: Dict[str, Any]) -> "LeRobotStats":
        """
        Reads the stats of a feature from a dict, as `LeRobotEpisode.stats` holds them (see
        `to_dict`). A null quantile is skipped, and a key that is not a stat is ignored.

        Args:
            stats (Dict[str, Any]): The stats, by name, as (nested) lists or numpy arrays,
                with `count` as `[frames]` or `frames`.

        Returns:
            LeRobotStats: The stats, in `float64`.

        Raises:
            ValueError: If one of min, max, mean, std and count is missing or null, or the
                stats are not feasible (see `__post_init__`).
        """
        missing = [
            name
            for name in ("min", "max", "mean", "std", "count")
            if stats.get(name) is None
        ]
        if missing:
            raise ValueError(f"The stats {missing} are missing.")

        return cls(
            min=np.asarray(stats["min"], dtype=np.float64),
            max=np.asarray(stats["max"], dtype=np.float64),
            mean=np.asarray(stats["mean"], dtype=np.float64),
            std=np.asarray(stats["std"], dtype=np.float64),
            count=int(np.squeeze(stats["count"])),
            quantiles={
                name: np.asarray(value, dtype=np.float64)
                for name, value in stats.items()
                if _is_quantile(name) and value is not None
            },
        )

    def to_dict(self) -> Dict[str, List]:
        """
        Returns the stats as a dict, as `LeRobotEpisode.stats` holds those of a feature and
        `meta/stats.json` stores them: by name, as (nested) lists, with `count` as `[frames]`.
        """
        return {
            "min": self.min.tolist(),
            "max": self.max.tolist(),
            "mean": self.mean.tolist(),
            "std": self.std.tolist(),
            **{name: value.tolist() for name, value in self.quantiles.items()},
            "count": [self.count],
        }

    @classmethod
    def aggregate(cls, stats: List["LeRobotStats"]) -> "LeRobotStats":
        """
        Combines the stats of a feature in several episodes into those over all of them, as
        LeRobot does:

        - min, max: the lowest min, the highest max;
        - mean: the mean of the episodes, weighted by their count;
        - std: from the variance of each episode around the combined mean (its own variance,
          plus the square of the distance of its mean), weighted by its count;
        - count: the sum;
        - quantiles: they cannot be combined exactly, the widest range is kept. Up to q50 the
          lowest among the episodes, above the highest. Only those all the episodes have are
          kept.

        Args:
            stats (List[LeRobotStats]): The stats of the feature in each episode, at least
                one, all of the same shape.

        Returns:
            LeRobotStats: The stats over all the episodes.

        Raises:
            ValueError: If `stats` is empty, or its stats are not all of the same shape.

        Examples:
            A feature of a single value, in an episode `[1, 2, 3]` and in one `[10, 20]`:

            ```
            count   3 + 2                                           = 5
            mean    (3 * 2 + 2 * 15) / 5                            = 7.2
            std     sqrt((3 * (0.67 + 5.2²) + 2 * (25 + 7.8²)) / 5) = 7.14  (exact)
            q50     min(2, 10)                                      = 2     (exact: 3)
            ```
        """
        if not stats:
            raise ValueError("There are no stats to aggregate.")

        counts = np.array([s.count for s in stats])
        means = np.stack([s.mean for s in stats])
        mean = np.average(means, axis=0, weights=counts)

        # The variance of each episode around the combined mean, weighted by its frames
        stds = np.stack([s.std for s in stats])
        variance = np.average(stds**2 + (means - mean) ** 2, axis=0, weights=counts)

        quantiles: Dict[str, np.ndarray] = {}
        for name in stats[0].quantiles:
            if not all(name in s.quantiles for s in stats):
                continue
            values = np.stack([s.quantiles[name] for s in stats])
            is_low = _STATS_QUANTILES[name] <= 0.5
            quantiles[name] = values.min(axis=0) if is_low else values.max(axis=0)

        return cls(
            min=np.stack([s.min for s in stats]).min(axis=0),
            max=np.stack([s.max for s in stats]).max(axis=0),
            mean=mean,
            std=np.sqrt(variance),
            count=int(counts.sum()),
            quantiles=quantiles,
        )


def _sample_indices(length: int) -> pa.Array:
    """
    Returns the frames of an episode whose images go into its stats, as LeRobot picks them:
    evenly spaced from the first frame to the last. All the frames of an episode shorter than
    100 frames, else `length ** 0.75` of them, at least 100 and at most 10,000.

    Args:
        length (int): The number of frames of the episode.

    Returns:
        pa.Array: The `frame_index` of the sampled frames, in increasing order, as `int64`
            (the type of the `frame_index` column).
    """
    num_samples = max(min(100, length), min(int(length**0.75), 10_000))

    # Rounded to the nearest frame, as LeRobot does (`dtype=np.int64` would round down)
    return pa.array(np.round(np.linspace(0, length - 1, num_samples)).astype(np.int64))


# ----- Public API functions


def compute_episode_stats(
    data_path: Path, episode_index: int, features: List[LeRobotFeature]
) -> Dict[str, Dict[str, Any]]:
    """
    Computes the stats of an episode from its rows in its data file: those of
    each timeseries over all its frames, and those of each image over the
    sampled frames that have one.

    Args:
        data_path (Path): The data file holding the episode, possibly with other episodes.
        episode_index (int): The index of the episode.
        features (List[LeRobotFeature]): The features of the dataset.

    Returns:
        Dict[str, Dict[str, Any]]: By feature with stats, its stats as `LeRobotEpisode.stats`
            holds them (see `LeRobotStats.to_dict`).

    Raises:
        RuntimeError: If the data file holds no row of the episode.
        FileNotFoundError: If the data file is missing.
        pa.ArrowInvalid: If the data file is not a readable parquet file (e.g. still being
            written).
    """
    features = [
        f for f in features if not f.is_video() and f.dtype not in _NO_STATS_DTYPES
    ]

    data_file = ds.dataset(data_path, format="parquet")

    # The data file can hold other episodes: only the rows of this one are read
    is_considered_episode = ds.field(_EPISODE_INDEX_COLUMN) == episode_index
    num_frames = data_file.count_rows(filter=is_considered_episode)

    if num_frames <= 0:
        raise RuntimeError(
            f"Episode {episode_index} has no row in the data file '{data_path}': its stats "
            "cannot be computed."
        )

    is_sampled = is_considered_episode & ds.field(_FRAME_INDEX_COLUMN).isin(
        _sample_indices(num_frames)
    )

    stats: Dict[str, Dict[str, Any]] = {}
    for feature in features:
        # Images are read on the sampled frames only
        filters = is_sampled if feature.is_image() else is_considered_episode

        # Loading in memory only the requested feature and with the correct filters
        rows = data_file.to_table(
            columns=[feature.key],
            filter=filters,
        )

        column = rows.column(feature.key)

        if feature.is_image():
            column = column.drop_null()
            if len(column) == 0:
                # No sampled frame has an image (a frame without one is null): no stats
                continue

        stats[feature.key] = LeRobotStats.from_column(
            feature, column, num_frames
        ).to_dict()

    return stats
