import pytest

from mosaicolabs import IMU
from mosaicolabs.bridges.mcap import MCAPAdapterBase
from mosaicolabs.bridges.mcap.bridge import (
    MCAPBridge,
    compute_mcap_msg_type,
    register_default_adapter,
)


class UnsupportedAdapter(MCAPAdapterBase[IMU]):
    """Custom adapter created for the Imu.proto message available at src/testing/unit/bridges/utils/proto/imu.proto"""

    schema_name = "Mosaico.invented.data"
    schema_encoding = "not-supported-encoding"


class MyImuProtobufAdapter(MCAPAdapterBase[IMU]):
    """Custom adapter created for the Imu.proto message available at src/testing/unit/bridges/utils/proto/imu.proto"""

    schema_name = "Mosaico.Imu"
    schema_encoding = "protobuf"
    __mosaico_ontology_type__ = IMU


def test_register_custom_adapters():
    """Test to check that custom adapter are correctly registered within MCAPBridge"""

    # By default, there should be not adapters recorded
    assert MCAPBridge.get_default_adapters() == {}

    # Cannot register custom adapter with unsupported schema_encoding
    with pytest.raises(
        ValueError,
        match=f"{UnsupportedAdapter.__name__} does not define a supported schema_encoding*",
    ):
        register_default_adapter(UnsupportedAdapter)

    # Still no adapters recorded since previous registration is supposed to fail
    assert MCAPBridge.get_default_adapters() == {}

    # Register valid custom adapter
    register_default_adapter(MyImuProtobufAdapter)

    imu_adapter_type = compute_mcap_msg_type(
        MyImuProtobufAdapter.schema_name, MyImuProtobufAdapter.schema_encoding
    )

    # Try registering again -> raises error
    with pytest.raises(
        ValueError,
        match=f"Adapter for MCAP message type '{imu_adapter_type}' is already registered",
    ):
        register_default_adapter(MyImuProtobufAdapter)

    assert MCAPBridge.get_default_adapters() == {imu_adapter_type: MyImuProtobufAdapter}
    assert (
        MCAPBridge.get_default_adapter("Mosaico.Imu", "protobuf")
        == MyImuProtobufAdapter
    )

    assert MCAPBridge.is_adapted(IMU)
    assert MCAPBridge.is_msgtype_adapted(imu_adapter_type)
