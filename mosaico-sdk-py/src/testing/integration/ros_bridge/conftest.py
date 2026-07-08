import pytest
from rosbags.typesys import Stores

from mosaicolabs.ros_bridge.sequence_extractor import ROSExtractorConfig
from testing.integration.config import UPLOADED_SEQUENCE_NAME


@pytest.fixture
def default_extractor_config(
    host,
    port,
    api_key_manage,
    with_tls,
    tls_cert_path,
    tmp_path,
) -> ROSExtractorConfig:
    return ROSExtractorConfig(
        rosbag_path=tmp_path,
        sequence_name=UPLOADED_SEQUENCE_NAME,
        host=host,
        port=port,
        ros_distro=Stores.LATEST,
        mosaico_api_key=api_key_manage,
        tls_cert_path=tls_cert_path,
        enable_tls=with_tls,
        overwrite=True,
    )
