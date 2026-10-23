import json
from types import SimpleNamespace

import pyarrow as pa
import pyarrow.parquet as pq
import pytest

from mosaicolabs.bridges.lerobot.metadata import (
    LeRobotDatasetMeta,
    LeRobotEpisode,
    LeRobotSchemaMetadata,
    sequences_to_lerobot_meta,
)

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
        "fps": fps,
        "data_path": "data/chunk-{chunk_index:03d}/file-{file_index:03d}.parquet",
        "total_episodes": 99,
        "features": features,
    }


def _sequence(name, row, info=None):
    """A sequence as ingested by the LeRobot bridge (see `LeRobotInjector`)."""
    meta = LeRobotSchemaMetadata(
        info=info or _info(),
        stats={},
        episode=LeRobotEpisode.from_row(row).to_dict(),
    )
    return SimpleNamespace(name=name, user_metadata=meta.merge_into({"operator": "x"}))


def test_sequences_to_lerobot_meta_merges_and_renumbers():
    # Two source datasets reusing episode 0 (FR-3), exported in this order
    sequences = [
        _sequence("a_episode_000000", _row(0, 300)),
        _sequence("b_episode_000000", _row(0, 250, task="place")),
        _sequence("a_episode_000042", _row(42, 400)),
    ]

    meta = sequences_to_lerobot_meta("out", sequences)

    assert [
        (e.episode_index, e.dataset_from_index, e.dataset_to_index)
        for e in meta.episodes_map.values()
    ] == [(0, 0, 300), (1, 300, 550), (2, 550, 950)]
    assert meta.episodes_map[2].extra == {
        "source_sequence": "a_episode_000042",
        "source_episode_index": 42,
    }
    assert meta.episodes_map[1].tasks == ["place"]
    # Dotted names rebuilt, video dropped
    assert set(meta.features) == {"observation.state", "action"}
    assert (meta.info["total_episodes"], meta.info["total_frames"]) == (3, 950)
    assert (meta.info["total_tasks"], meta.info["splits"]) == (2, {"train": "0:3"})


def test_sequences_to_lerobot_meta_rejects_different_features():
    other = {**FEATURES, "action": {"dtype": "float32", "shape": [7], "names": None}}
    sequences = [
        _sequence("a_episode_000000", _row(0, 300)),
        _sequence("b_episode_000000", _row(0, 300), info=_info(features=other)),
    ]

    with pytest.raises(ValueError, match=r"b_episode_000000.*\['action'\]"):
        sequences_to_lerobot_meta("out", sequences)


def test_sequences_to_lerobot_meta_rejects_missing_metadata():
    sequences = [
        SimpleNamespace(name="plain_sequence", user_metadata={"operator": "x"})
    ]

    with pytest.raises(ValueError, match="plain_sequence"):
        sequences_to_lerobot_meta("out", sequences)


def test_sequences_to_lerobot_meta_rejects_feature_holding_a_feature():
    # Ingestion merges `a.b` into the description of `a`
    features = {
        "a": {"dtype": "float32", "shape": [1]},
        "a.b": {"dtype": "float32", "shape": [1]},
    }
    sequences = [_sequence("s", _row(0, 10), info=_info(features=features))]

    with pytest.raises(ValueError, match="Feature 'a' holds another feature"):
        sequences_to_lerobot_meta("out", sequences)


def test_load_reads_folder(tmp_path):
    (tmp_path / "meta" / "episodes").mkdir(parents=True)
    (tmp_path / "meta" / "info.json").write_text(json.dumps(_info()))
    rows = [_row(1, 20), _row(0, 10)]
    pq.write_table(
        pa.Table.from_pylist(rows), tmp_path / "meta" / "episodes" / "file-000.parquet"
    )

    meta = LeRobotDatasetMeta.load(tmp_path)

    assert meta.episode_indexes == [0, 1]
    assert meta.n_frames() == 30
    assert meta.stats is None
