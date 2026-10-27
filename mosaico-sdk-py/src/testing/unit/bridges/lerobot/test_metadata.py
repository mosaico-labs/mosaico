import json
import math
from dataclasses import replace
from pathlib import Path
from typing import Any, Callable, Dict

import pandas as pd
import pytest

from mosaicolabs.bridges.lerobot.metadata import (
    LeRobotDatasetMeta,
    LeRobotEpisode,
    LeRobotFeature,
    LeRobotInfo,
)
from mosaicolabs.bridges.lerobot.schema_metadata import LeRobotSchemaMetadata

from .config import (
    ACTION_COLUMN_NAME,
    DATA_PATH,
    DATA_SCHEMA,
    EPISODE_LENGTHS,
    FPS,
    STATE_COLUMN_NAME,
    TASK,
)


def _episode_row() -> Dict[str, Any]:
    """A row of `meta/episodes`, as `pyarrow.Table.to_pylist()` returns it."""
    return {
        "episode_index": 3,
        "tasks": ["Pick up the cube"],
        "length": 10,
        "data/chunk_index": 0,
        "data/file_index": 1,
        "dataset_from_index": 20,
        "dataset_to_index": 30,
        "meta/episodes/chunk_index": 0,
        "meta/episodes/file_index": 2,
        "stats/observation.state/mean": [0.5, 1.5],
        "stats/observation.state/count": [10],
        "stats/action/max": [1.0, 2.0],
        "videos/observation.images.side/file_index": 4,
    }


def _edit_info_json(root: Path, edit: Callable[[Dict[str, Any]], Any]):
    info_path = root / "meta" / "info.json"
    info_path.write_text(json.dumps(edit(json.loads(info_path.read_text()))))


# --- LeRobotSchemaMetadata ---


def test_schema_metadata_stores_the_features_as_a_string():
    """They are keyed by feature names, whose `.` the server rejects in keys."""
    features = {"observation.state": {"dtype": "float32", "shape": [2]}}

    meta = LeRobotSchemaMetadata(info={"fps": 30, "features": features})

    assert meta.to_dict()["_lerobot_"]["info"] == {
        "fps": 30,
        "features": json.dumps(features),
    }


def test_schema_metadata_nests_the_episode_stats():
    """The server rejects metadata keys containing `.`, and non-finite floats."""
    meta = LeRobotSchemaMetadata(
        episode={
            "length": 10,
            "stats": {
                "observation.state": {"std": [math.nan, 1.0]},
                "observation.images.front": {"max": [[[1.0]]]},
            },
        }
    )

    assert meta.to_dict() == {
        "_lerobot_": {
            "episode": {
                "length": 10,
                "stats": {
                    "observation": {
                        "state": {"std": [None, 1.0]},
                        "images": {"front": {"max": [[[1.0]]]}},
                    }
                },
            }
        }
    }


@pytest.mark.parametrize(
    "stats",
    [
        {"a": {"mean": [1.0]}, "a.mean": {"mean": [2.0]}},
        {"a.mean": {"mean": [2.0]}, "a": {"mean": [1.0]}},
    ],
)
def test_schema_metadata_rejects_a_key_both_value_and_object(stats):
    """A feature named after a statistic of another one (`a.mean`) cannot be nested."""
    with pytest.raises(ValueError, match="Metadata key conflict on 'a.mean'"):
        LeRobotSchemaMetadata(episode={"stats": stats})


def test_schema_metadata_update_sanitizes_the_fields():
    meta = LeRobotSchemaMetadata(source_file="pusht")

    result = meta.update(episode={"stats": {"observation.state": {"std": [math.nan]}}})

    assert result is meta
    assert meta.to_dict() == {
        "_lerobot_": {
            "source_file": "pusht",
            "episode": {"stats": {"observation": {"state": {"std": [None]}}}},
        }
    }


@pytest.mark.parametrize("with_video", [False, True])
def test_schema_metadata_accepts_the_metadata_of_every_episode(
    lerobot_dataset_factory, with_video
):
    """What the injector attaches to each sequence: no key is rejected."""
    meta = LeRobotDatasetMeta.load(lerobot_dataset_factory(with_video=with_video))

    for episode in meta.episodes_map.values():
        lerobot_meta = LeRobotSchemaMetadata(
            info=meta.info.to_dict(), episode=episode.to_dict()
        )

        stats = lerobot_meta.to_dict()["_lerobot_"]["episode"]["stats"]
        assert "state" in stats["observation"]


@pytest.mark.parametrize("with_video", [False, True])
def test_schema_metadata_restore_gives_the_fields_back(
    lerobot_dataset_factory, with_video
):
    meta = LeRobotDatasetMeta.load(lerobot_dataset_factory(with_video=with_video))
    fields = {
        "info": meta.info.to_dict(),
        "episode": meta.episodes_map[0].to_dict(),
        "source_file": "example",
    }

    stored = LeRobotSchemaMetadata(**fields).merge_into({"operator": "x"})

    assert LeRobotSchemaMetadata.restore(stored) == fields


def test_schema_metadata_restores_a_feature_prefix_of_another():
    """Nesting merges the stats of `a.b` into those of `a`: restoring splits them back."""
    stats = {"a": {"mean": [1.0]}, "a.b": {"mean": [2.0]}}

    stored = LeRobotSchemaMetadata(episode={"stats": stats}).to_dict()

    assert LeRobotSchemaMetadata.restore(stored)["episode"]["stats"] == stats


# --- LeRobotEpisode ---


def test_episode_from_row():
    episode = LeRobotEpisode.from_row(_episode_row())

    assert episode.episode_index == 3
    assert episode.tasks == ["Pick up the cube"]
    assert episode.length == 10
    assert (episode.data_chunk_index, episode.data_file_index) == (0, 1)
    assert (episode.dataset_from_index, episode.dataset_to_index) == (20, 30)
    assert episode.meta_episodes_file_index == 2
    # Feature names contain `.`, not `/`: the stat name follows the last `/`
    assert episode.stats == {
        "observation.state": {"mean": [0.5, 1.5], "count": [10]},
        "action": {"max": [1.0, 2.0]},
    }


def test_episode_to_dict_nests_the_column_names():
    episode = LeRobotEpisode.from_row(_episode_row())

    # The `videos/...` column of the row is not read: video features are not supported yet
    assert episode.to_dict() == {
        "episode_index": 3,
        "tasks": ["Pick up the cube"],
        "length": 10,
        "data": {"chunk_index": 0, "file_index": 1},
        "dataset_from_index": 20,
        "dataset_to_index": 30,
        "meta": {"episodes": {"chunk_index": 0, "file_index": 2}},
        "stats": {
            "observation.state": {"mean": [0.5, 1.5], "count": [10]},
            "action": {"max": [1.0, 2.0]},
        },
    }


def test_episode_requires_its_columns():
    row = _episode_row()
    del row["length"]

    with pytest.raises(ValueError, match=r"missing the required columns \['length'\]"):
        LeRobotEpisode.from_row(row)


# --- LeRobotFeature ---


def test_feature_to_dict():
    feature = LeRobotFeature.from_dict(
        "action", {"dtype": "float32", "shape": [2], "names": ["x", "y"]}
    )

    assert feature.shape == (2,)
    # As in `meta/info.json`: no `key`, no `info` if not set
    assert feature.to_dict() == {
        "dtype": "float32",
        "shape": [2],
        "names": ["x", "y"],
    }


def test_feature_requires_a_dtype():
    with pytest.raises(ValueError, match="has no 'dtype'"):
        LeRobotFeature.from_dict("action", {"shape": [2]})


# --- LeRobotInfo ---


def test_info_round_trips_info_json(lerobot_dataset):
    info_json = json.loads((lerobot_dataset / "meta" / "info.json").read_text())

    info = LeRobotInfo.from_dict(info_json)

    assert (info.fps, info.data_path, info.video_path) == (FPS, DATA_PATH, None)
    assert [f.key for f in info.features] == DATA_SCHEMA.names
    assert info.to_dict() == info_json


def test_info_without_its_optional_keys(lerobot_dataset):
    info_json = json.loads((lerobot_dataset / "meta" / "info.json").read_text())
    del info_json["robot_type"], info_json["video_path"]

    info = LeRobotInfo.from_dict(info_json)

    assert (info.robot_type, info.video_path) == (None, None)


def test_info_requires_its_keys(lerobot_dataset):
    info_json = json.loads((lerobot_dataset / "meta" / "info.json").read_text())
    del info_json["data_path"]

    with pytest.raises(ValueError, match=r"missing the required keys \['data_path'\]"):
        LeRobotInfo.from_dict(info_json)


def test_info_rejects_an_unsupported_version():
    with pytest.raises(ValueError, match="Unsupported LeRobot dataset version 'v2.1'"):
        LeRobotInfo.from_dict({"codebase_version": "v2.1"})


# --- LeRobotDatasetMeta.load ---


def test_load(lerobot_dataset):
    meta = LeRobotDatasetMeta.load(lerobot_dataset)

    assert meta.codebase_version == "v3.0"
    assert meta.fps == FPS
    assert meta.n_episodes == len(EPISODE_LENGTHS)
    assert [f.key for f in meta.features] == DATA_SCHEMA.names
    assert meta.stats is not None
    assert set(meta.stats) == {ACTION_COLUMN_NAME, STATE_COLUMN_NAME}
    # Episodes 0 and 1 are in the first episodes file, episode 2 in the second one
    assert meta.episode_indexes == [0, 1, 2]
    assert [e.length for e in meta.episodes_map.values()] == EPISODE_LENGTHS
    assert [e.data_file_index for e in meta.episodes_map.values()] == [0, 0, 1]


def test_load_without_stats_json(lerobot_dataset_factory):
    """`meta/stats.json` is optional in LeRobot."""
    meta = LeRobotDatasetMeta.load(lerobot_dataset_factory(with_stats_json=False))

    assert meta.stats is None


# --- LeRobotDatasetMeta.dump ---


@pytest.mark.parametrize("with_stats_json", [False, True])
def test_dump_writes_what_load_reads(
    lerobot_dataset_factory, tmp_path, with_stats_json
):
    source = lerobot_dataset_factory(with_stats_json=with_stats_json)
    meta = replace(LeRobotDatasetMeta.load(source), root=tmp_path / "dumped")

    meta.dump()

    assert LeRobotDatasetMeta.load(meta.root) == meta


def test_dump_writes_the_tasks_as_lerobot_reads_them(lerobot_dataset, tmp_path):
    """LeRobot reads `meta/tasks.parquet` with pandas, indexed by task."""
    meta = replace(LeRobotDatasetMeta.load(lerobot_dataset), root=tmp_path)

    meta.dump()

    tasks = pd.read_parquet(tmp_path / "meta" / "tasks.parquet")
    assert list(tasks.index) == [TASK]
    assert tasks.loc[TASK, "task_index"] == 0
