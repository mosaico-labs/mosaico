"""
Example LeRobot v3.0 dataset of the LeRobot bridge tests.

`make_lerobot_dataset` writes it with `pyarrow` and `PIL` only (the `lerobot` package is not
needed), laid out as LeRobot writes it:

    <name>/
    ├── meta/
    │   ├── info.json
    │   ├── stats.json
    │   ├── tasks.parquet
    │   └── episodes/chunk-000/file-XXX.parquet
    └── data/chunk-000/file-XXX.parquet

Every value is a function of its frame (see `action_value`, `state_value`, `timestamp_value` and
`image_value`), so that the tests can check what the bridge reads.
"""

import io
import json
from pathlib import Path
from typing import Any, Dict, List, Sequence

import pandas as pd
import pyarrow as pa
import pyarrow.parquet as pq
from PIL import Image as PILImage

DATASET_NAME = "example_lerobot"
"""The default name of the dataset root folder, the default prefix of its sequence names."""

CODEBASE_VERSION = "v3.0"

FPS = 16
"""A power of two: the `float32` timestamps (`frame_index / FPS`) and their nanoseconds are exact."""

EPISODE_LENGTHS = [5, 7, 4]
"""The number of frames of each episode."""

EPISODES_PER_FILE = 2
"""The number of episodes of each data file and each episodes file: episodes 0 and 1 share their
files, so that the rows of an episode must be filtered out of them."""

ROW_GROUP_SIZE = 3
"""The number of rows of the row groups of the data files: an episode spans several of them."""

TASK = "Pick up the cube"

JOINT_NAMES = ["shoulder.pos", "gripper.pos"]
"""The names of the `action` and `observation.state` dimensions."""

IMAGE_WIDTH = 4
IMAGE_HEIGHT = 4

DATA_PATH = "data/chunk-{chunk_index:03d}/file-{file_index:03d}.parquet"
VIDEO_PATH = "videos/{video_key}/chunk-{chunk_index:03d}/file-{file_index:03d}.mp4"
EPISODES_PATH = "meta/episodes/chunk-{chunk_index:03d}/file-{file_index:03d}.parquet"

# ----- Columns -----

ACTION_COLUMN_NAME = "action"
STATE_COLUMN_NAME = "observation.state"
CAMERA_COLUMN_NAMES = ["observation.images.front", "observation.images.wrist"]
VIDEO_COLUMN_NAME = "observation.images.side"
"""The video feature declared by `with_video`, which has no data column."""
TIMESTAMP_COLUMN_NAME = "timestamp"
INDEX_COLUMN_NAMES = ["frame_index", "episode_index", "index", "task_index"]

TIMESERIES_COLUMN_NAMES = [ACTION_COLUMN_NAME, STATE_COLUMN_NAME, *INDEX_COLUMN_NAMES]
"""The timeseries columns, in the order of the features."""

COLUMN_NAMES = [
    ACTION_COLUMN_NAME,
    STATE_COLUMN_NAME,
    *CAMERA_COLUMN_NAMES,
    *INDEX_COLUMN_NAMES,
]
"""The columns read by the bridge (all but the timestamp and the video), in the order of the
features."""

_IMAGE_TYPE = pa.struct([("bytes", pa.binary()), ("path", pa.string())])
_VECTOR_TYPE = pa.list_(pa.float32(), len(JOINT_NAMES))

DATA_SCHEMA = pa.schema(
    [
        (ACTION_COLUMN_NAME, _VECTOR_TYPE),
        (STATE_COLUMN_NAME, _VECTOR_TYPE),
        *[(name, _IMAGE_TYPE) for name in CAMERA_COLUMN_NAMES],
        (TIMESTAMP_COLUMN_NAME, pa.float32()),
        *[(name, pa.int64()) for name in INDEX_COLUMN_NAMES],
    ]
)
"""The schema of the data files, as LeRobot writes them."""

# ----- Frame values -----
# `index` is the global index of a frame in the dataset, `frame_index` its index in its episode.


def episode_frames(
    episode_index: int, episode_lengths: Sequence[int] = EPISODE_LENGTHS
) -> range:
    """The global indexes of the frames of an episode."""
    start = sum(episode_lengths[:episode_index])
    return range(start, start + episode_lengths[episode_index])


def action_value(index: int) -> List[float]:
    """The `action` of the frame `index` (exact in `float32`)."""
    return [float(index), -float(index)]


def state_value(index: int) -> List[float]:
    """The `observation.state` of the frame `index` (exact in `float32`)."""
    return [index + 0.5, -index - 0.5]


def timestamp_value(frame_index: int) -> float:
    """The `timestamp` of the frame `frame_index`, in seconds since the episode start."""
    return frame_index / FPS


def timestamp_ns_value(frame_index: int) -> int:
    """The `timestamp` of the frame `frame_index`, in nanoseconds since the episode start."""
    return frame_index * 1_000_000_000 // FPS


def image_file_name(frame_index: int) -> str:
    """The name of the file LeRobot wrote the image of the frame `frame_index` to."""
    return f"frame-{frame_index:06d}.png"


def image_value(index: int, camera_column_name: str) -> bytes:
    """The PNG image of a camera in the frame `index`: a plain color, derived from both."""
    color = (index % 256, 128 * CAMERA_COLUMN_NAMES.index(camera_column_name), 0)

    buffer = io.BytesIO()
    PILImage.new("RGB", (IMAGE_WIDTH, IMAGE_HEIGHT), color).save(buffer, format="PNG")
    return buffer.getvalue()


# ----- Dataset -----


def _vector_stats(values: List[List[float]]) -> Dict[str, List[Any]]:
    """The LeRobot statistics of a vector feature over `values`, per dimension."""
    dims = list(zip(*values))
    return {
        "min": [min(dim) for dim in dims],
        "max": [max(dim) for dim in dims],
        "mean": [sum(dim) / len(dim) for dim in dims],
        "count": [len(values)],
    }


def _features(with_video: bool) -> Dict[str, Any]:
    """The `features` of `meta/info.json`, in the order LeRobot writes them."""
    vector = {"dtype": "float32", "shape": [len(JOINT_NAMES)], "names": JOINT_NAMES}
    image = {
        "dtype": "image",
        "shape": [IMAGE_HEIGHT, IMAGE_WIDTH, 3],
        "names": ["height", "width", "channels"],
        "info": {"is_depth_map": False},
    }
    scalar = {"shape": [1], "names": None}

    features: Dict[str, Any] = {
        ACTION_COLUMN_NAME: vector,
        STATE_COLUMN_NAME: vector,
        **{name: image for name in CAMERA_COLUMN_NAMES},
    }
    if with_video:
        features[VIDEO_COLUMN_NAME] = {
            "dtype": "video",
            "shape": [IMAGE_HEIGHT, IMAGE_WIDTH, 3],
            "names": ["height", "width", "channels"],
            "info": {
                "video.fps": FPS,
                "video.codec": "av1",
                "video.pix_fmt": "yuv420p",
                "video.is_depth_map": False,
                "has_audio": False,
            },
        }
    features[TIMESTAMP_COLUMN_NAME] = {"dtype": "float32", **scalar}
    features.update({name: {"dtype": "int64", **scalar} for name in INDEX_COLUMN_NAMES})
    return features


def _write_parquet(table: pa.Table, path: Path, **kwargs: Any) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    pq.write_table(table, path, **kwargs)


def _write_json(content: Dict[str, Any], path: Path) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(content, indent=4))


def make_lerobot_dataset(
    parent_dir: Path,
    name: str = DATASET_NAME,
    episode_lengths: Sequence[int] = EPISODE_LENGTHS,
    episodes_per_file: int = EPISODES_PER_FILE,
    row_group_size: int = ROW_GROUP_SIZE,
    with_stats_json: bool = True,
    with_video: bool = False,
) -> Path:
    """
    Writes an example LeRobot v3.0 dataset, with the features of `_features`, a single task and
    every file in chunk 0.

    Args:
        parent_dir (Path): The folder the dataset is written in.
        name (str): The name of the dataset root folder.
        episode_lengths (Sequence[int]): The number of frames of each episode.
        episodes_per_file (int): The number of episodes of each data file and each episodes file.
        row_group_size (int): The number of rows of the row groups of the data files.
        with_stats_json (bool): Whether to write `meta/stats.json`, optional in LeRobot.
        with_video (bool): Whether to declare the video feature `VIDEO_COLUMN_NAME`, with its
            columns in `meta/episodes`. No video file is written: the bridge does not read them.

    Returns:
        Path: The root folder of the dataset.
    """
    root = Path(parent_dir) / name

    data_rows: Dict[int, List[Dict[str, Any]]] = {}
    episode_rows: Dict[int, List[Dict[str, Any]]] = {}

    start = 0
    for episode_index, length in enumerate(episode_lengths):
        file_index = episode_index // episodes_per_file
        file_rows = data_rows.setdefault(file_index, [])
        # The episodes of a file are stored one after the other, in data and video files alike
        file_offset = len(file_rows)
        indexes = range(start, start + length)

        for frame_index, index in enumerate(indexes):
            file_rows.append(
                {
                    ACTION_COLUMN_NAME: action_value(index),
                    STATE_COLUMN_NAME: state_value(index),
                    **{
                        camera: {
                            "bytes": image_value(index, camera),
                            "path": image_file_name(frame_index),
                        }
                        for camera in CAMERA_COLUMN_NAMES
                    },
                    TIMESTAMP_COLUMN_NAME: timestamp_value(frame_index),
                    "frame_index": frame_index,
                    "episode_index": episode_index,
                    "index": index,
                    "task_index": 0,
                }
            )

        episode_row: Dict[str, Any] = {
            "episode_index": episode_index,
            "tasks": [TASK],
            "length": length,
            "data/chunk_index": 0,
            "data/file_index": file_index,
            "dataset_from_index": start,
            "dataset_to_index": start + length,
            "meta/episodes/chunk_index": 0,
            "meta/episodes/file_index": file_index,
        }
        if with_video:
            video_columns = f"videos/{VIDEO_COLUMN_NAME}"
            episode_row.update(
                {
                    f"{video_columns}/chunk_index": 0,
                    f"{video_columns}/file_index": file_index,
                    f"{video_columns}/from_timestamp": file_offset / FPS,
                    f"{video_columns}/to_timestamp": (file_offset + length) / FPS,
                }
            )
        for column_name, value in (
            (ACTION_COLUMN_NAME, action_value),
            (STATE_COLUMN_NAME, state_value),
        ):
            stats = _vector_stats([value(index) for index in indexes])
            episode_row.update(
                {f"stats/{column_name}/{stat}": val for stat, val in stats.items()}
            )
        episode_rows.setdefault(file_index, []).append(episode_row)

        start += length

    for file_index, rows in data_rows.items():
        _write_parquet(
            pa.Table.from_pylist(rows, schema=DATA_SCHEMA),
            root / DATA_PATH.format(chunk_index=0, file_index=file_index),
            row_group_size=row_group_size,
        )
    for file_index, rows in episode_rows.items():
        _write_parquet(
            pa.Table.from_pylist(rows),
            root / EPISODES_PATH.format(chunk_index=0, file_index=file_index),
        )
    pd.DataFrame({"task_index": [0]}, index=pd.Index([TASK], name="task")).to_parquet(
        root / "meta" / "tasks.parquet"
    )

    _write_json(
        {
            "codebase_version": CODEBASE_VERSION,
            "robot_type": "example_robot",
            "fps": FPS,
            "features": _features(with_video),
            "total_episodes": len(episode_lengths),
            "total_frames": start,
            "total_tasks": 1,
            "chunks_size": 1000,
            "data_files_size_in_mb": 100,
            "video_files_size_in_mb": 200,
            "data_path": DATA_PATH,
            "video_path": VIDEO_PATH if with_video else None,
            "splits": {"train": f"0:{len(episode_lengths)}"},
        },
        root / "meta" / "info.json",
    )
    if with_stats_json:
        _write_json(
            {
                ACTION_COLUMN_NAME: _vector_stats(
                    [action_value(i) for i in range(start)]
                ),
                STATE_COLUMN_NAME: _vector_stats(
                    [state_value(i) for i in range(start)]
                ),
            },
            root / "meta" / "stats.json",
        )

    return root


def lengthen_first_episode(root: Path) -> None:
    """Makes the `length` of episode 0 in `meta/episodes` exceed its rows in its data file, so
    that the episode cannot be read."""
    path = root / EPISODES_PATH.format(chunk_index=0, file_index=0)
    table = pq.read_table(path)
    lengths = table.column("length").to_pylist()
    lengths[0] += 1

    pq.write_table(
        table.set_column(
            table.schema.get_field_index("length"),
            "length",
            pa.array(lengths, pa.int64()),
        ),
        path,
    )
