import pytest
from rosbags.typesys import Stores

# from mosaicolabs.bridges.mcap.sequence_extractor import MCAPExtractorConfig
from mosaicolabs.bridges.ros.sequence_extractor import ROSExtractorConfig
from testing.integration.config import UPLOADED_SEQUENCE_NAME


@pytest.fixture
def default_ros_extractor_config(
    host,
    port,
    api_key_manage,
    with_tls,
    tls_cert_path,
    tmp_path,
) -> ROSExtractorConfig:
    return ROSExtractorConfig(
        saving_path=tmp_path,
        sequence_name=UPLOADED_SEQUENCE_NAME,
        host=host,
        port=port,
        mosaico_api_key=api_key_manage,
        tls_cert_path=tls_cert_path,
        enable_tls=with_tls,
        overwrite=True,
        ros_distro=Stores.LATEST,
    )


# @pytest.fixture
# def default_mcap_extractor_config(
#     host,
#     port,
#     api_key_manage,
#     with_tls,
#     tls_cert_path,
#     tmp_path,
# ) -> MCAPExtractorConfig:
#     return MCAPExtractorConfig(
#         saving_path=tmp_path,
#         sequence_name="",
#         host=host,
#         port=port,
#         mosaico_api_key=api_key_manage,
#         tls_cert_path=tls_cert_path,
#         enable_tls=with_tls,
#         overwrite=True,
#     )
