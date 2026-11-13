import hashlib
import http.client
import urllib.request
from pathlib import Path

import pytest

from mosaicolabs.bridges.base_injector import InjectionStatus
from mosaicolabs.bridges.ulog import ULogInjectionConfig, ULogInjector, ULogLoader
from mosaicolabs.comm.mosaico_client import MosaicoClient

# PX4/pyulog at release v1.2.4
PYULOG_TEST_URL = (
    "https://raw.githubusercontent.com/PX4/pyulog/"
    "ffbe3755d903e93797a89fb4fce26e0c0420a42f/test/"
)
SAMPLE_LOGS = {  # file name: SHA-256
    "sample_appended_multiple.ulg": "daf30f3224303e39d5c97701e048e84ba04480797e369502331f45ab2e99a2b7",
    "sample_log_small.ulg": "68d1020f688109f027dba08d2950247cc0ae34aaefdcf37e4a7d60838f3e1aa3",
}

# In both logs, and without NaN values in its first row
FIRST_ROW_TOPIC = "/sensor_combined/0"


def _sha256(content: bytes) -> str:
    return hashlib.sha256(content).hexdigest()


@pytest.fixture(
    scope="session", params=sorted(SAMPLE_LOGS), ids=lambda name: Path(name).stem
)
def ulog_file(request) -> Path:
    """
    One of the two sample logs of pyulog, downloaded on first use and kept in the pytest
    cache. The tests using it are skipped if the download is not possible.

    The two logs are third-party files, used only by the tests of the ULog bridge.

    Origin: both are downloaded unchanged from the `test/` folder of
    [PX4/pyulog](https://github.com/PX4/pyulog) at release
    [v1.2.4](https://github.com/PX4/pyulog/tree/v1.2.4/test), the library the bridge
    reads ULog files with, and checked against the SHA-256 in `SAMPLE_LOGS`.

    | File | Size (bytes) | Added upstream |
    |---|---|---|
    | `sample_appended_multiple.ulg` | 486737 | [dd44f43](https://github.com/PX4/pyulog/commit/dd44f431deb1f9b2075b7d513d41073b459b1109), 2017-07-05 |
    | `sample_log_small.ulg` | 921631 | [0197eba](https://github.com/PX4/pyulog/commit/0197ebaad28a1d73c366acf0064311e3d289ec40), 2022-07-22 |

    What they contain, exactly as published upstream:

    - `sample_appended_multiple.ulg`: a 10-second log of a PX4FMU_V4PRO board, with data
      appended after the end of the log. It has no geographic position data.
    - `sample_log_small.ulg`: a flight of about 20 minutes of a CubePilot Cube Orange.
      Like any PX4 flight log, it includes the GNSS position of the vehicle.

    Both logs carry the identifier of the board that recorded them (`sys_uuid`).

    License: pyulog is distributed under the BSD 3-Clause license (SPDX `BSD-3-Clause`),
    see the upstream [`LICENSE.md`](https://github.com/PX4/pyulog/blob/v1.2.4/LICENSE.md).
    """
    name = request.param
    path = request.config.cache.mkdir("ulog") / name
    if not path.is_file() or _sha256(path.read_bytes()) != SAMPLE_LOGS[name]:
        try:
            with urllib.request.urlopen(PYULOG_TEST_URL + name, timeout=60) as response:
                content = response.read()
        except (OSError, http.client.HTTPException) as e:
            pytest.skip(f"Cannot download '{name}': {e}")
        assert _sha256(content) == SAMPLE_LOGS[name], f"SHA-256 mismatch for '{name}'"
        path.write_bytes(content)
    return path


@pytest.fixture
def ulog_config(ulog_file, host, port, api_key_manage, with_tls, tls_cert_path):
    return ULogInjectionConfig(
        file_path=ulog_file,
        sequence_name=f"ulog_{ulog_file.stem}",
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
    ulog_file, sequence_name = ulog_config.file_path, ulog_config.sequence_name

    # A limit larger than any topic of the two logs: the first chunk has the first row
    with ULogLoader(ulog_file, 64 * 1024 * 1024) as loader:
        expected = {
            t: (
                loader.msg_count(t),
                loader.topic_schema_metadata(t)
                .update(source_file=ulog_file.name)
                .fields,
            )
            for t in loader.topics
        }
        first_row = next(
            next(msg.data) for msg in loader if msg.topic_name == FIRST_ROW_TOPIC
        ).slice(0, 1)

    try:
        assert ULogInjector(ulog_config).run() is InjectionStatus.COMPLETED

        seq = mosaico_client.sequence_handler(sequence_name)
        assert seq is not None
        assert sorted(seq.topics) == sorted(expected)
        assert "/ulog/logging" in seq.topics
        assert (
            seq.user_metadata["_ulog_"]["ulog_ingestion"]["source_file"]
            == ulog_file.name
        )

        for topic, (n_rows, ulog_meta) in expected.items():
            t_handler = seq.get_topic_handler(topic)
            assert t_handler.user_metadata["_ulog_"] == ulog_meta
            assert t_handler.get_data_streamer().msg_count == n_rows

        streamer = seq.get_topic_handler(FIRST_ROW_TOPIC).get_data_streamer()
        assert next(iter(streamer))._to_pa_record_batch().to_pylist() == (
            first_row.to_pylist()
        )
    finally:
        if mosaico_client.sequence_exists(sequence_name):
            mosaico_client.sequence_delete(sequence_name)


def test_ulog_dry_run_and_unsupported_update(ulog_config):
    """A dry run never connects; `update_if_exists` is rejected (one file = one sequence)."""
    ulog_config.dry_run = True
    assert ULogInjector(ulog_config).run() is InjectionStatus.DRY_RUN

    ulog_config.update_if_exists = True
    with pytest.raises(ValueError, match="update_if_exists"):
        ULogInjector(ulog_config).run()
