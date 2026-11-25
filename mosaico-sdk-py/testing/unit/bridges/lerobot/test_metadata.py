import json
import math
import shutil
from dataclasses import replace
from pathlib import Path
from typing import Any, Callable, Dict, Iterable, List

import numpy as np
import pandas as pd
import pytest

from mosaicolabs.bridges.lerobot.metadata import (
    LeRobotDatasetMeta,
    LeRobotEpisode,
    LeRobotInfo,
)
from mosaicolabs.bridges.lerobot.schema_metadata import LeRobotSchemaMetadata

from .config import (
    ACTION_COLUMN_NAME,
    DATA_PATH,
    DATA_SCHEMA,
    EPISODE_LENGTHS,
    FPS,
    INDEX_COLUMN_NAMES,
    STATE_COLUMN_NAME,
    TASK,
    action_value,
    state_value,
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


def _frames_stats(
    value: Callable[[int], List[float]], frames: Iterable[int]
) -> Dict[str, Any]:
    """The stats of a vector feature of the example dataset (e.g. `state_value`) over
    `frames`, as LeRobot computes them."""
    values = np.array([value(index) for index in frames])
    return {
        "min": values.min(axis=0),
        "max": values.max(axis=0),
        "mean": values.mean(axis=0),
        "std": values.std(axis=0),
        "count": [len(values)],
    }


def _without_stats(meta: LeRobotDatasetMeta) -> LeRobotDatasetMeta:
    """`meta` with episodes without stats, as those exported from Mosaico."""
    episodes = [replace(e, stats={}) for e in meta.episodes_map.values()]
    return LeRobotDatasetMeta.from_data(meta.root, meta.info, episodes, meta.tasks)


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
            info=meta.info.to_dict(),
            episode={
                "length": episode.length,
                "tasks": episode.tasks,
                "stats": episode.stats,
            },
        )

        stats = lerobot_meta.to_dict()["_lerobot_"]["episode"]["stats"]
        assert "state" in stats["observation"]


@pytest.mark.parametrize("with_video", [False, True])
def test_schema_metadata_restore_gives_the_fields_back(
    lerobot_dataset_factory, with_video
):
    meta = LeRobotDatasetMeta.load(lerobot_dataset_factory(with_video=with_video))
    episode = meta.episodes_map[0]
    # What the injector attaches to the sequence of the episode
    fields = {
        "info": meta.info.to_dict(),
        "episode": {
            "length": episode.length,
            "tasks": episode.tasks,
            "stats": episode.stats,
        },
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


def test_episode_to_row():
    episode = LeRobotEpisode.from_row(_episode_row())

    # The `videos/...` column of the row is not read: video features are not supported yet
    assert episode.to_row() == {
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
    }


def test_episode_to_row_is_the_opposite_of_from_row():
    row = _episode_row()
    episode = LeRobotEpisode.from_row(row)

    # The `videos/...` column of the row is not read: video features are not supported yet
    assert episode.to_row() == {
        col: value for col, value in row.items() if not col.startswith("videos/")
    }
    assert LeRobotEpisode.from_row(episode.to_row()) == episode


def test_episode_requires_its_columns():
    row = _episode_row()
    del row["length"]

    with pytest.raises(ValueError, match=r"missing the required columns \['length'\]"):
        LeRobotEpisode.from_row(row)


def test_episode_drop_features_removes_their_stats():
    episode = LeRobotEpisode.from_row(_episode_row())

    dropped = episode.drop_features(["action", "unknown"])

    assert set(dropped.stats) == {"observation.state"}
    # The episode itself is left unchanged
    assert set(episode.stats) == {"observation.state", "action"}


# --- LeRobotInfo ---


def test_info_round_trips_info_json(lerobot_dataset):
    info_json = json.loads((lerobot_dataset / "meta" / "info.json").read_text())

    info = LeRobotInfo.from_dict(info_json)

    assert (info.fps, info.data_path, info.video_path) == (FPS, DATA_PATH, None)
    assert [f.key for f in info.features] == DATA_SCHEMA.names
    assert info.to_dict() == info_json


def test_info_keeps_its_extra_keys(lerobot_dataset):
    """The keys outside the v3.0 format are kept, and written back."""
    info_json = json.loads((lerobot_dataset / "meta" / "info.json").read_text())
    info_json["license"] = "apache-2.0"

    info = LeRobotInfo.from_dict(info_json)

    assert info.extra == {"license": "apache-2.0"}
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
        LeRobotInfo.from_dict(
            {
                "codebase_version": "v2.1",
                "fps": 30,
                "features": {},
                "total_episodes": 15,
                "data_path": "",
            }
        )


def test_info_incompatible_because_one_is_depthmap(lerobot_dataset):
    """A depth map holds different data than a feature of the same key, dtype and shape that
    is not one."""
    info = LeRobotDatasetMeta.load(lerobot_dataset).info
    features = [replace(f, info={"is_depth_map": True}) for f in info.features]

    with pytest.raises(ValueError, match="differing features"):
        info.check_compatible(replace(info, features=features))


@pytest.mark.parametrize(
    "change, error",
    [
        (lambda info: {"fps": info.fps + 1}, "fps"),
        (lambda info: {"robot_type": "other_robot"}, "robot type"),
        (lambda info: {"features": info.features[1:]}, "differing features"),
    ],
)
def test_info_incompatible(lerobot_dataset, change, error):
    info = LeRobotDatasetMeta.load(lerobot_dataset).info

    with pytest.raises(ValueError, match=error):
        info.check_compatible(replace(info, **change(info)))


# --- LeRobotDatasetMeta.is_dataset ---


def test_is_dataset_with_the_full_layout(lerobot_dataset):
    assert LeRobotDatasetMeta.is_dataset(lerobot_dataset)


@pytest.mark.parametrize(
    "removed",
    ["meta/info.json", "meta/episodes", "data"],
    ids=["no-info-json", "no-episodes-folder", "no-data-folder"],
)
def test_is_dataset_without_part_of_the_layout(lerobot_dataset_factory, removed):
    root = lerobot_dataset_factory()
    path = root / removed
    if path.is_dir():
        shutil.rmtree(path)
    else:
        path.unlink()

    assert not LeRobotDatasetMeta.is_dataset(root)


# --- LeRobotDatasetMeta.load ---


def test_load(lerobot_dataset):
    meta = LeRobotDatasetMeta.load(lerobot_dataset)

    assert meta.codebase_version == "v3.0"
    assert meta.fps == FPS
    assert meta.n_episodes == len(EPISODE_LENGTHS)
    assert [f.key for f in meta.features] == DATA_SCHEMA.names
    stats = meta.compute_stats()
    assert stats is not None
    assert set(stats) == {
        ACTION_COLUMN_NAME,
        STATE_COLUMN_NAME,
        *INDEX_COLUMN_NAMES,
    }
    # Episodes 0 and 1 are in the first episodes file, episode 2 in the second one
    assert meta.episode_indexes == [0, 1, 2]
    assert [e.length for e in meta.episodes_map.values()] == EPISODE_LENGTHS
    assert [e.data_file_index for e in meta.episodes_map.values()] == [0, 0, 1]
    # `meta/tasks.parquet` is indexed by task, in task index order
    assert meta.tasks == [TASK]


# --- LeRobotDatasetMeta.from_data ---


def test_from_data_sorts_the_episodes(lerobot_dataset):
    meta = LeRobotDatasetMeta.load(lerobot_dataset)
    episodes = list(reversed(meta.episodes_map.values()))

    created = LeRobotDatasetMeta.from_data(meta.root, meta.info, episodes, meta.tasks)

    assert created.episode_indexes == meta.episode_indexes


def test_from_data_rejects_an_episode_defined_twice(lerobot_dataset):
    meta = LeRobotDatasetMeta.load(lerobot_dataset)
    episodes = [*meta.episodes_map.values(), meta.episodes_map[0]]

    with pytest.raises(ValueError, match="Episode 0 is defined more than once"):
        LeRobotDatasetMeta.from_data(meta.root, meta.info, episodes, meta.tasks)


# --- LeRobotDatasetMeta.drop_features ---


def test_drop_features_removes_them_from_info_and_episodes(lerobot_dataset):
    meta = LeRobotDatasetMeta.load(lerobot_dataset)

    dropped = meta.drop_features([ACTION_COLUMN_NAME])

    assert ACTION_COLUMN_NAME not in [f.key for f in dropped.features]
    assert STATE_COLUMN_NAME in [f.key for f in dropped.features]
    for episode in dropped.episodes_map.values():
        assert set(episode.stats) == {STATE_COLUMN_NAME, *INDEX_COLUMN_NAMES}


def test_drop_features_leaves_the_metadata_unchanged(lerobot_dataset):
    """The metadata is shared (e.g. by the loader and the reader): dropping features for one
    caller must not change what the others see."""
    meta = LeRobotDatasetMeta.load(lerobot_dataset)

    meta.drop_features([ACTION_COLUMN_NAME])

    assert meta == LeRobotDatasetMeta.load(lerobot_dataset)
    assert ACTION_COLUMN_NAME in [f.key for f in meta.features]
    for episode in meta.episodes_map.values():
        assert set(episode.stats) == {
            ACTION_COLUMN_NAME,
            STATE_COLUMN_NAME,
            *INDEX_COLUMN_NAMES,
        }


# --- LeRobotDatasetMeta.compute_stats ---


def test_compute_stats_combines_those_of_the_episodes(lerobot_dataset):
    """As LeRobot writes `meta/stats.json`: those over every frame, for the features with
    stats in the episodes."""
    stats = LeRobotDatasetMeta.load(lerobot_dataset).compute_stats()

    assert stats is not None
    assert set(stats) == {ACTION_COLUMN_NAME, STATE_COLUMN_NAME, *INDEX_COLUMN_NAMES}
    every_frame = range(sum(EPISODE_LENGTHS))
    for key, value in (
        (ACTION_COLUMN_NAME, action_value),
        (STATE_COLUMN_NAME, state_value),
    ):
        for name, expected in _frames_stats(value, every_frame).items():
            np.testing.assert_allclose(stats[key][name], expected)


def test_compute_stats_of_episodes_without_stats_is_none(lerobot_dataset):
    meta = _without_stats(LeRobotDatasetMeta.load(lerobot_dataset))

    assert meta.compute_stats() is None


# --- LeRobotDatasetMeta.dump ---


def test_dump_writes_what_load_reads(lerobot_dataset, tmp_path):
    meta = replace(LeRobotDatasetMeta.load(lerobot_dataset), root=tmp_path / "dumped")

    meta.dump()

    assert LeRobotDatasetMeta.load(meta.root) == meta
    stats_json = json.loads((meta.root / "meta" / "stats.json").read_text())
    assert stats_json == meta.compute_stats()


def test_dump_writes_the_tasks_as_lerobot_reads_them(lerobot_dataset, tmp_path):
    """LeRobot reads `meta/tasks.parquet` with pandas, indexed by task."""
    meta = replace(LeRobotDatasetMeta.load(lerobot_dataset), root=tmp_path)

    meta.dump()

    tasks = pd.read_parquet(tmp_path / "meta" / "tasks.parquet")
    assert list(tasks.index) == [TASK]
    assert tasks.loc[TASK, "task_index"] == 0


def test_dump_of_episodes_without_stats_writes_no_stats_json(lerobot_dataset, tmp_path):
    """No data file is read: the episodes are written with the stats they hold."""
    meta = replace(
        _without_stats(LeRobotDatasetMeta.load(lerobot_dataset)),
        root=tmp_path / "dumped",
    )

    meta.dump()

    assert not (meta.root / "meta" / "stats.json").exists()
    reloaded = LeRobotDatasetMeta.load(meta.root)
    assert reloaded == meta
    assert all(episode.stats == {} for episode in reloaded.episodes_map.values())
