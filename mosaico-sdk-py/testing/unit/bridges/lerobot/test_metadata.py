import json
import math
from pathlib import Path
from typing import Any, Callable, Dict

import pytest

from mosaicolabs.bridges.lerobot.metadata import (
    LeRobotDatasetMeta,
    LeRobotEpisode,
    LeRobotFeature,
    LeRobotSchemaMetadata,
)

from .config import (
    ACTION_COLUMN_NAME,
    DATA_SCHEMA,
    EPISODE_LENGTHS,
    FPS,
    STATE_COLUMN_NAME,
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


def test_schema_metadata_nests_dotted_keys():
    """The server rejects metadata keys containing `.`."""
    meta = LeRobotSchemaMetadata(
        info={"features": {"observation.state": 1, "observation.images.front": 2}}
    )

    assert meta.to_dict() == {
        "_lerobot_": {
            "info": {"features": {"observation": {"state": 1, "images": {"front": 2}}}}
        }
    }


@pytest.mark.parametrize(
    "features",
    [
        {"observation": 1, "observation.state": 2},
        {"observation.state": 2, "observation": 1},
    ],
)
def test_schema_metadata_rejects_a_key_both_value_and_object(features):
    with pytest.raises(ValueError, match="Metadata key conflict on 'observation'"):
        LeRobotSchemaMetadata(info={"features": features})


def test_schema_metadata_update_sanitizes_the_fields():
    meta = LeRobotSchemaMetadata(source_file="pusht")

    result = meta.update(stats={"observation.state": {"std": [math.nan]}})

    assert result is meta
    assert meta.to_dict() == {
        "_lerobot_": {
            "source_file": "pusht",
            "stats": {"observation": {"state": {"std": [None]}}},
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
            info=meta.info, stats=meta.stats, episode=episode.to_dict()
        )

        features = lerobot_meta.to_dict()["_lerobot_"]["info"]["features"]
        assert "state" in features["observation"]


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
    assert episode.extra == {"videos/observation.images.side/file_index": 4}


def test_episode_to_dict_nests_the_column_names():
    episode = LeRobotEpisode.from_row(_episode_row())

    assert episode.to_dict() == {
        "episode_index": 3,
        "tasks": ["Pick up the cube"],
        "length": 10,
        "data": {"chunk_index": 0, "file_index": 1},
        "dataset_from_index": 20,
        "dataset_to_index": 30,
        "meta": {"episodes": {"chunk_index": 0, "file_index": 2}},
        "videos": {"observation.images.side": {"file_index": 4}},
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
    assert feature.to_dict() == {
        "key": "action",
        "dtype": "float32",
        "shape": [2],
        "names": ["x", "y"],
        "info": None,
    }


def test_feature_requires_a_dtype():
    with pytest.raises(ValueError, match="has no 'dtype'"):
        LeRobotFeature.from_dict("action", {"shape": [2]})


# --- LeRobotDatasetMeta.load ---


def test_load(lerobot_dataset):
    meta = LeRobotDatasetMeta.load(lerobot_dataset)

    assert meta.codebase_version == "v3.0"
    assert meta.fps == FPS
    assert meta.n_episodes == len(EPISODE_LENGTHS)
    assert list(meta.features) == DATA_SCHEMA.names
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
