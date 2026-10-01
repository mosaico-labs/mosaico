from mosaicolabs.bridges.mcap import MCAPInjectionConfig, MCAPInjector


def test_mcap_injection_succeds(
    mosaico_client,
    default_mcap_protobuf_injector_config: MCAPInjectionConfig,
    # default_mcap_jsonschema_injector_config: MCAPInjectionConfig,
    default_mcap_mixed_injector_config: MCAPInjectionConfig,
):
    """Tests that the whole ingestion pipeline works using all available sample mcap files"""

    # 1) Protobuf only mcap
    MCAPInjector(default_mcap_protobuf_injector_config).run()

    assert (
        mosaico_client.sequence_exists(
            default_mcap_protobuf_injector_config.sequence_name
        )
        is True
    )
    mosaico_client.sequence_delete(default_mcap_protobuf_injector_config.sequence_name)

    # 2) jsonschema only mcap
    # default_mcap_jsonschema_injector_config.sequence_name = Path(mcap_jsonschema_file).stem
    # MCAPInjector(default_mcap_jsonschema_injector_config).run()

    # assert mosaico_client.sequence_exists(default_mcap_jsonschema_injector_config.sequence_name) is True
    # mosaico_client.sequence_delete(default_mcap_jsonschema_injector_config.sequence_name)

    # 3) protobuf + jsonschema mcap
    MCAPInjector(default_mcap_mixed_injector_config).run()

    assert (
        mosaico_client.sequence_exists(default_mcap_mixed_injector_config.sequence_name)
        is True
    )
    mosaico_client.sequence_delete(default_mcap_mixed_injector_config.sequence_name)
    mosaico_client.close()
