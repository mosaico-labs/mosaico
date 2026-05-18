from pathlib import Path

import pytest

from mosaicolabs.ros_bridge.sequence_extractor import ROSExtractorConfig
from testing.integration.config import UPLOADED_SEQUENCE_NAME


@pytest.fixture
def rosbag_output_path(tmp_path: Path) -> Path:
    return tmp_path / "extracted.bag"


@pytest.fixture
def default_extractor_config(
    host,
    port,
    api_key_mgmt,
    with_tls,
    tls_cert_path,
    rosbag_output_path,
) -> ROSExtractorConfig:
    return ROSExtractorConfig(
        rosbag_path=rosbag_output_path,
        sequence_name=UPLOADED_SEQUENCE_NAME,
        host=host,
        port=port,
        mosaico_api_key=api_key_mgmt,
        tls_cert_path=tls_cert_path,
        enable_tls=with_tls,
        overwrite=True,
    )
