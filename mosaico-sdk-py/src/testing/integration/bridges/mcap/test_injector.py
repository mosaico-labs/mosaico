from pathlib import Path

from mosaicolabs.bridges.mcap import MCAPInjectionConfig, MCAPInjector


def create_mcap_injection_config(
    mcap_path: Path, sequence_name: str, host: str, port: int, dry_run=False
):
    return MCAPInjectionConfig(
        file_path=mcap_path,
        sequence_name=sequence_name,
        metadata={},
        host=host,
        port=port,
        dry_run=dry_run,
    )


def test_mcap_ingection_succeds(
    mcap_protobuf_file, mcap_jsonschema_file, mcap_mixed_file, mosaico_client
):
    """Tests that the whole ingestion pipeline works using all available sample mcap files"""
    mosaico_client._host
    # 1) Protobuf only mcap
    sequence_name_proto = Path(mcap_protobuf_file).stem
    mcap_injection_config_proto = create_mcap_injection_config(
        Path(mcap_protobuf_file),
        sequence_name_proto,
        host=mosaico_client._host,
        port=mosaico_client._port,
    )
    MCAPInjector(mcap_injection_config_proto).run()

    assert mosaico_client.sequence_exists(sequence_name_proto) is True
    mosaico_client.sequence_delete(sequence_name_proto)

    # 2) jsonschema only mcap
    sequence_name_json = Path(mcap_jsonschema_file).stem
    mcap_injection_config_json = create_mcap_injection_config(
        Path(mcap_jsonschema_file),
        sequence_name_json,
        host=mosaico_client._host,
        port=mosaico_client._port,
    )
    MCAPInjector(mcap_injection_config_json).run()

    assert mosaico_client.sequence_exists(sequence_name_json) is True
    mosaico_client.sequence_delete(sequence_name_json)

    # 3) protobuf + jsonschema mcap
    sequence_name_mixed = Path(mcap_mixed_file).stem
    mcap_injection_config_protojson = create_mcap_injection_config(
        Path(mcap_mixed_file),
        sequence_name_mixed,
        host=mosaico_client._host,
        port=mosaico_client._port,
    )
    MCAPInjector(mcap_injection_config_protojson).run()

    assert mosaico_client.sequence_exists(sequence_name_mixed) is True
    mosaico_client.sequence_delete(sequence_name_mixed)
