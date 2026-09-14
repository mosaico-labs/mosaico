from pathlib import Path

import pytest

from mosaicolabs.bridges.mcap.injector import MCAPInjectionConfig


@pytest.fixture
def default_mcap_protobuf_injector_config(
    host, port, api_key_manage, with_tls, tls_cert_path, mcap_protobuf_file
) -> MCAPInjectionConfig:
    return MCAPInjectionConfig(
        file_path=Path(mcap_protobuf_file),
        sequence_name=Path(mcap_protobuf_file).stem,
        host=host,
        port=port,
        mosaico_api_key=api_key_manage,
        tls_cert_path=tls_cert_path,
        enable_tls=with_tls,
    )


# @pytest.fixture
# def default_mcap_jsonschema_injector_config(
#     host, port, api_key_manage, with_tls, tls_cert_path, mcap_jsonschema_file
# ) -> MCAPInjectionConfig:
#     return MCAPInjectionConfig(
#         file_path=Path(mcap_jsonschema_file),
#         sequence_name=Path(mcap_jsonschema_file).stem,
#         host=host,
#         port=port,
#         mosaico_api_key=api_key_manage,
#         tls_cert_path=tls_cert_path,
#         enable_tls=with_tls,
#     )


@pytest.fixture
def default_mcap_mixed_injector_config(
    host, port, api_key_manage, with_tls, tls_cert_path, mcap_mixed_file
) -> MCAPInjectionConfig:
    return MCAPInjectionConfig(
        file_path=Path(mcap_mixed_file),
        sequence_name=Path(mcap_mixed_file).stem,
        host=host,
        port=port,
        mosaico_api_key=api_key_manage,
        tls_cert_path=tls_cert_path,
        enable_tls=with_tls,
    )
