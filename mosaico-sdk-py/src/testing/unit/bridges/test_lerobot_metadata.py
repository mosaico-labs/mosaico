import json

import pandas as pd
import pyarrow as pa
import pyarrow.parquet as pq

from mosaicolabs.bridges.lerobot.metadata import LeRobotDatasetMeta

FEATURES = {
    "observation.state": {"dtype": "float32", "shape": [6], "names": None},
    "observation.images.top": {"dtype": "video", "shape": [480, 640, 3]},
    "action": {"dtype": "float32", "shape": [6], "names": None},
}


def _row(episode_index, length, task="pick", **extra):
    """A `meta/episodes` row, flat as in the parquet files."""
    return {
        "episode_index": episode_index,
        "tasks": [task],
        "length": length,
        "data/chunk_index": 0,
        "data/file_index": 0,
        "dataset_from_index": 1000,
        "dataset_to_index": 1000 + length,
        "stats/observation.state/mean": [0.1] * 6,
        **extra,
    }


def _info(features=FEATURES, fps=30):
    return {
        "codebase_version": "v3.0",
        "robot_type": "so_follower",
        "total_episodes": 99,
        "total_frames": 9999,
        "total_tasks": 9,
        "chunks_size": 1000,
        "data_files_size_in_mb": 100,
        "video_files_size_in_mb": 200,
        "fps": fps,
        "splits": {"train": "0:99"},
        "data_path": "data/chunk-{chunk_index:03d}/file-{file_index:03d}.parquet",
        "video_path": "videos/{video_key}/chunk-{chunk_index:03d}/file-{file_index:03d}.mp4",
        "features": features,
    }


def test_load_reads_folder(tmp_path):
    (tmp_path / "meta" / "episodes").mkdir(parents=True)
    (tmp_path / "meta" / "info.json").write_text(json.dumps(_info()))
    rows = [_row(1, 20), _row(0, 10)]
    pq.write_table(
        pa.Table.from_pylist(rows), tmp_path / "meta" / "episodes" / "file-000.parquet"
    )
    pd.DataFrame({"task_index": [0]}, index=pd.Index(["pick"], name="task")).to_parquet(
        tmp_path / "meta" / "tasks.parquet"
    )

    meta = LeRobotDatasetMeta.load(tmp_path)

    assert meta.episode_indexes == [0, 1]
    assert meta.n_frames() == 30
    assert meta.stats is None
    assert meta.tasks == ["pick"]
