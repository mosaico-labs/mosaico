"""
Integration tests of the LeRobot dataset extraction: the example dataset is ingested by the
`LeRobotInjector`, one sequence per episode, then exported back by the
`LeRobotSequencesExtractor` and compared with it.
"""

from dataclasses import replace
from pathlib import Path
from typing import Iterator, List, Optional

import pyarrow as pa
import pyarrow.compute as pc
import pyarrow.parquet as pq
import pytest

from mosaicolabs.bridges.base_injector import InjectionStatus
from mosaicolabs.bridges.lerobot.injector import (
    LeRobotInjectionConfig,
    LeRobotInjector,
)
from mosaicolabs.bridges.lerobot.metadata import LeRobotDatasetMeta
from mosaicolabs.bridges.lerobot.sequences_extractor import (
    ExtractionStatus,
    LeRobotExtractorConfig,
    LeRobotSequencesExtractor,
)
from mosaicolabs.comm import MosaicoClient
from testing.unit.bridges.lerobot.config import ACTION_COLUMN_NAME, EPISODE_LENGTHS

SEQUENCE_PREFIX = "lerobot_extraction"

FILTERED_SEQUENCE_PREFIX = "lerobot_extraction_filtered"
"""The prefix of the sequences ingested without the `action` column."""


def _sequence_names(prefix: str) -> List[str]:
    """The sequences the episodes of the example dataset are ingested as with `prefix`, in
    episode order."""
    return [
        f"{prefix}_episode_{episode_index:06d}"
        for episode_index in range(len(EPISODE_LENGTHS))
    ]


SEQUENCE_NAMES = _sequence_names(SEQUENCE_PREFIX)
"""The sequences the episodes of the example dataset are ingested as, in episode order."""

MISSING_SEQUENCE_NAME = f"{SEQUENCE_PREFIX}_missing"

EXPORTED_DATA_FILE = Path("data/chunk-000/file-000.parquet")
"""The data file of the exported dataset, holding all its episodes."""


def _delete_sequences(sequence_names, host, port, api_key_manage, tls_cert_path):
    with MosaicoClient.connect(
        host=host, port=port, tls_cert_path=tls_cert_path, api_key=api_key_manage
    ) as client:
        for sequence_name in sequence_names:
            if client.sequence_exists(sequence_name):
                client.sequence_delete(sequence_name)


def _ingested_sequences(
    prefix: str,
    column_names: Optional[List[str]],
    host,
    port,
    api_key_manage,
    with_tls,
    tls_cert_path,
    lerobot_dataset,
) -> Iterator[List[str]]:
    """Ingests the example LeRobot dataset as the sequences of `prefix`, keeping the columns
    selected by `column_names` (all if None). The sequences are deleted before ingesting and
    after the caller is done with them."""
    sequence_names = _sequence_names(prefix)
    _delete_sequences(sequence_names, host, port, api_key_manage, tls_cert_path)

    status = LeRobotInjector(
        LeRobotInjectionConfig(
            file_path=lerobot_dataset,
            sequence_name=prefix,
            column_names=column_names,
            host=host,
            port=port,
            mosaico_api_key=api_key_manage,
            tls_cert_path=tls_cert_path,
            enable_tls=with_tls,
        )
    ).run()
    assert status is InjectionStatus.COMPLETED

    yield sequence_names

    _delete_sequences(sequence_names, host, port, api_key_manage, tls_cert_path)


def _original_data(root: Path, schema: pa.Schema) -> pa.Table:
    """The rows of the data files of a dataset, in episode order, with the columns and types of
    `schema`."""
    meta = LeRobotDatasetMeta.load(root)
    data_files = dict.fromkeys(meta.get_parquet_file(i) for i in meta.episode_indexes)
    return (
        pa.concat_tables(pq.read_table(f) for f in data_files)
        .select(schema.names)
        .cast(schema)
    )


def _episode_rows(table: pa.Table, episode_index: int) -> pa.Table:
    return table.filter(pc.equal(table.column("episode_index"), episode_index))


@pytest.fixture(scope="module")
def ingested_sequences(
    host, port, api_key_manage, with_tls, tls_cert_path, lerobot_dataset
) -> Iterator[List[str]]:
    """Ingests the example LeRobot dataset, whose sequences are deleted before and after the
    tests of the module."""
    yield from _ingested_sequences(
        SEQUENCE_PREFIX,
        None,
        host,
        port,
        api_key_manage,
        with_tls,
        tls_cert_path,
        lerobot_dataset,
    )


@pytest.fixture(scope="module")
def ingested_filtered_sequences(
    host, port, api_key_manage, with_tls, tls_cert_path, lerobot_dataset
) -> Iterator[List[str]]:
    """Ingests the example LeRobot dataset without its `action` column, whose sequences are
    deleted before and after the tests of the module."""
    yield from _ingested_sequences(
        FILTERED_SEQUENCE_PREFIX,
        [f"!{ACTION_COLUMN_NAME}"],
        host,
        port,
        api_key_manage,
        with_tls,
        tls_cert_path,
        lerobot_dataset,
    )


@pytest.fixture
def extractor_config(
    host, port, api_key_manage, with_tls, tls_cert_path, tmp_path, ingested_sequences
) -> LeRobotExtractorConfig:
    """The config exporting all the ingested sequences to a new folder."""
    return LeRobotExtractorConfig(
        saving_path=tmp_path / "export",
        sequence_names=ingested_sequences,
        host=host,
        port=port,
        mosaico_api_key=api_key_manage,
        tls_cert_path=tls_cert_path,
        enable_tls=with_tls,
    )


def test_extraction_recreates_the_dataset(extractor_config, lerobot_dataset):
    """Tests that exporting all the sequences recreates the ingested dataset: its metadata,
    and its rows in a single data file"""
    saving_path = extractor_config.saving_path

    extractor = LeRobotSequencesExtractor(extractor_config)

    assert extractor.run() is ExtractionStatus.COMPLETED
    assert extractor.dropped_sequences == []
    meta = extractor.meta

    assert LeRobotDatasetMeta.load(saving_path) == meta
    assert [e.length for e in meta.episodes_map.values()] == EPISODE_LENGTHS
    assert sorted(
        p.relative_to(saving_path) for p in (saving_path / "data").rglob("*.parquet")
    ) == [EXPORTED_DATA_FILE]

    # `task_index` included: it is rebuilt from the `task` of each frame (see `LeRobotWriter`)
    exported = pq.read_table(saving_path / EXPORTED_DATA_FILE)
    assert exported.equals(_original_data(lerobot_dataset, exported.schema))


def test_extraction_of_a_filtered_ingestion_recreates_the_filtered_dataset(
    extractor_config, ingested_filtered_sequences, lerobot_dataset
):
    """Tests that exporting the sequences ingested without a column recreates the ingested
    dataset without that column: in its metadata (features and episode stats) and in its rows"""
    config = replace(extractor_config, sequence_names=ingested_filtered_sequences)
    saving_path = config.saving_path

    extractor = LeRobotSequencesExtractor(config)

    assert extractor.run() is ExtractionStatus.COMPLETED
    meta = extractor.meta
    assert LeRobotDatasetMeta.load(saving_path) == meta
    assert [e.length for e in meta.episodes_map.values()] == EPISODE_LENGTHS
    original_meta = LeRobotDatasetMeta.load(lerobot_dataset)
    assert [f.key for f in meta.info.features] == [
        f.key for f in original_meta.info.features if f.key != ACTION_COLUMN_NAME
    ]
    for episode in meta.episodes_map.values():
        assert ACTION_COLUMN_NAME not in episode.stats

    exported = pq.read_table(saving_path / EXPORTED_DATA_FILE)
    assert ACTION_COLUMN_NAME not in exported.column_names
    assert exported.equals(_original_data(lerobot_dataset, exported.schema))


def test_extraction_drops_the_missing_sequences(extractor_config):
    """Tests that a sequence that does not exist is dropped, without stopping the extraction,
    and reported as a partial extraction"""
    config = replace(
        extractor_config,
        sequence_names=[MISSING_SEQUENCE_NAME, SEQUENCE_NAMES[0]],
    )
    extractor = LeRobotSequencesExtractor(config)

    assert extractor.run() is ExtractionStatus.PARTIAL
    assert extractor.dropped_sequences == [MISSING_SEQUENCE_NAME]
    meta = extractor.meta

    assert [e.length for e in meta.episodes_map.values()] == [EPISODE_LENGTHS[0]]
    exported = pq.read_table(config.saving_path / EXPORTED_DATA_FILE)
    assert exported.num_rows == EPISODE_LENGTHS[0]


def test_extraction_without_any_sequence_raises(extractor_config):
    """Tests that an extraction with no sequence to export raises, writing nothing"""
    config = replace(extractor_config, sequence_names=[MISSING_SEQUENCE_NAME])

    with pytest.raises(ValueError, match="No episode to export"):
        LeRobotSequencesExtractor(config).run()

    assert not config.saving_path.exists()


def test_extraction_refuses_an_existing_folder(extractor_config):
    """Tests that an existing folder is not replaced without `overwrite`"""
    extractor_config.saving_path.mkdir()

    with pytest.raises(FileExistsError, match="already exists"):
        LeRobotSequencesExtractor(extractor_config).run()


def test_extraction_overwrites_an_existing_dataset(extractor_config):
    """Tests that an existing LeRobot dataset is replaced with `overwrite`"""
    saving_path = extractor_config.saving_path
    (saving_path / "meta" / "episodes").mkdir(parents=True)
    (saving_path / "meta" / "info.json").write_text("{}")
    (saving_path / "data").mkdir()
    stale_file = saving_path / "stale.txt"
    stale_file.write_text("A previous extraction")

    LeRobotSequencesExtractor(replace(extractor_config, overwrite=True)).run()

    assert not stale_file.exists()
    assert (saving_path / EXPORTED_DATA_FILE).is_file()
    assert not list(saving_path.parent.glob(f".{saving_path.name}.*"))


def test_extraction_refuses_to_overwrite_a_folder_that_is_not_a_dataset(
    extractor_config,
):
    """Tests that a non-empty folder that is not a LeRobot dataset is not replaced, even
    with `overwrite`"""
    saving_path = extractor_config.saving_path
    saving_path.mkdir()
    other_file = saving_path / "other.txt"
    other_file.write_text("Not a LeRobot dataset")

    with pytest.raises(ValueError, match="not a LeRobot dataset"):
        LeRobotSequencesExtractor(replace(extractor_config, overwrite=True)).run()

    assert other_file.exists()
