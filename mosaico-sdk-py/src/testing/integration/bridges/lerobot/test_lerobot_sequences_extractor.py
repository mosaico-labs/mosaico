"""
Integration tests of the LeRobot dataset extraction: the example dataset is ingested by the
`LeRobotInjector`, one sequence per episode, then exported back by the
`LeRobotSequencesExtractor` and compared with it.
"""

from dataclasses import replace
from pathlib import Path
from typing import Iterator, List

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
    LeRobotExtractorConfig,
    LeRobotSequencesExtractor,
)
from mosaicolabs.comm import MosaicoClient
from testing.unit.bridges.lerobot.config import EPISODE_LENGTHS

SEQUENCE_PREFIX = "lerobot_extraction"

SEQUENCE_NAMES = [
    f"{SEQUENCE_PREFIX}_episode_{episode_index:06d}"
    for episode_index in range(len(EPISODE_LENGTHS))
]
"""The sequences the episodes of the example dataset are ingested as, in episode order."""

MISSING_SEQUENCE_NAME = f"{SEQUENCE_PREFIX}_missing"

EXPORTED_DATA_FILE = Path("data/chunk-000/file-000.parquet")
"""The data file of the exported dataset, holding all its episodes."""


def _delete_sequences(host, port, api_key_manage, tls_cert_path):
    with MosaicoClient.connect(
        host=host, port=port, tls_cert_path=tls_cert_path, api_key=api_key_manage
    ) as client:
        for sequence_name in SEQUENCE_NAMES:
            if client.sequence_exists(sequence_name):
                client.sequence_delete(sequence_name)


def _original_data(root: Path, schema: pa.Schema) -> pa.Table:
    """The rows of the data files of a dataset, in episode order, with the types of `schema`."""
    meta = LeRobotDatasetMeta.load(root)
    data_files = dict.fromkeys(meta.get_parquet_file(i) for i in meta.episode_indexes)
    return pa.concat_tables(pq.read_table(f) for f in data_files).cast(schema)


def _episode_rows(table: pa.Table, episode_index: int) -> pa.Table:
    return table.filter(pc.equal(table.column("episode_index"), episode_index))


@pytest.fixture(scope="module")
def ingested_sequences(
    host, port, api_key_manage, with_tls, tls_cert_path, lerobot_dataset
) -> Iterator[List[str]]:
    """Ingests the example LeRobot dataset, whose sequences are deleted before and after the
    tests of the module."""
    _delete_sequences(host, port, api_key_manage, tls_cert_path)

    status = LeRobotInjector(
        LeRobotInjectionConfig(
            file_path=lerobot_dataset,
            sequence_name=SEQUENCE_PREFIX,
            host=host,
            port=port,
            mosaico_api_key=api_key_manage,
            tls_cert_path=tls_cert_path,
            enable_tls=with_tls,
        )
    ).run()
    assert status is InjectionStatus.COMPLETED

    yield SEQUENCE_NAMES

    _delete_sequences(host, port, api_key_manage, tls_cert_path)


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

    meta = LeRobotSequencesExtractor(extractor_config).run()

    assert LeRobotDatasetMeta.load(saving_path) == meta
    assert [e.length for e in meta.episodes_map.values()] == EPISODE_LENGTHS
    assert sorted(
        p.relative_to(saving_path) for p in (saving_path / "data").rglob("*.parquet")
    ) == [EXPORTED_DATA_FILE]

    # `task_index` is 0 in the example dataset, as it is exported (see `LeRobotWriter`)
    exported = pq.read_table(saving_path / EXPORTED_DATA_FILE)
    assert exported.equals(_original_data(lerobot_dataset, exported.schema))


def test_extraction_drops_the_missing_sequences(extractor_config):
    """Tests that a sequence that does not exist is dropped, without stopping the extraction"""
    config = replace(
        extractor_config,
        sequence_names=[MISSING_SEQUENCE_NAME, SEQUENCE_NAMES[0]],
    )

    meta = LeRobotSequencesExtractor(config).run()

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


def test_extraction_overwrites_an_existing_folder(extractor_config):
    """Tests that an existing folder is replaced with `overwrite`"""
    saving_path = extractor_config.saving_path
    saving_path.mkdir()
    stale_file = saving_path / "stale.txt"
    stale_file.write_text("A previous extraction")

    LeRobotSequencesExtractor(replace(extractor_config, overwrite=True)).run()

    assert not stale_file.exists()
    assert (saving_path / EXPORTED_DATA_FILE).is_file()
