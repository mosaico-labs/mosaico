from pathlib import Path

import pytest

from mosaicolabs.bridges.base_injector import InjectionStatus
from mosaicolabs.bridges.ulog import ULogInjectionConfig, ULogInjector, ULogLoader
from mosaicolabs.comm.mosaico_client import MosaicoClient

ULOG_FILE = (
    Path(__file__).resolve().parents[3] / "data" / "ulog" / "sample_log_small.ulg"
)
SEQUENCE_NAME = "ulog_sample_log_small"


@pytest.fixture
def ulog_config(host, port, api_key_manage, with_tls, tls_cert_path):
    return ULogInjectionConfig(
        file_path=ULOG_FILE,
        sequence_name=SEQUENCE_NAME,
        host=host,
        port=port,
        mosaico_api_key=api_key_manage,
        tls_cert_path=tls_cert_path,
        enable_tls=with_tls,
    )


def test_ulog_injection(mosaico_client, ulog_config, monkeypatch):
    """The whole file lands on the server: topics, row counts, `_ulog_` and sequence metadata."""
    # A small batch limit, so that the larger topics are written in several chunks
    monkeypatch.setattr(MosaicoClient, "max_batch_size_bytes", 64 * 1024)
    with ULogLoader(ULOG_FILE) as loader:
        expected = {
            t: (
                loader.msg_count(t),
                loader.topic_schema_metadata(t)
                .update(source_file=ULOG_FILE.name)
                .fields,
            )
            for t in loader.topics
        }
        first_accel = next(
            msg.data for msg, _ in loader if msg.topic_name == "/sensor_accel/1"
        ).slice(0, 1)

    try:
        assert ULogInjector(ulog_config).run() is InjectionStatus.COMPLETED

        seq = mosaico_client.sequence_handler(SEQUENCE_NAME)
        assert seq is not None
        assert sorted(seq.topics) == sorted(expected)
        assert {"/ulog/logging", "/ulog/dropouts"} <= set(seq.topics)
        assert seq.user_metadata["ulog_ingestion"]["source_file"] == ULOG_FILE.name

        for topic, (n_rows, ulog_meta) in expected.items():
            t_handler = seq.get_topic_handler(topic)
            assert t_handler.user_metadata["_ulog_"] == ulog_meta
            assert t_handler.get_data_streamer().msg_count == n_rows

        streamer = seq.get_topic_handler("/sensor_accel/1").get_data_streamer()
        assert next(iter(streamer))._to_pa_record_batch().to_pylist() == (
            first_accel.to_pylist()
        )
    finally:
        if mosaico_client.sequence_exists(SEQUENCE_NAME):
            mosaico_client.sequence_delete(SEQUENCE_NAME)


def test_ulog_dry_run_and_unsupported_update(ulog_config):
    """A dry run never connects; `update_if_exists` is rejected (one file = one sequence)."""
    ulog_config.dry_run = True
    assert ULogInjector(ulog_config).run() is InjectionStatus.DRY_RUN

    ulog_config.update_if_exists = True
    with pytest.raises(ValueError, match="update_if_exists"):
        ULogInjector(ulog_config).run()
