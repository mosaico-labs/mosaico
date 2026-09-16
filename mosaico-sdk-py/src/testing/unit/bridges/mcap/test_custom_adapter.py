from typing import Dict, Type

import pytest
from google.protobuf.json_format import MessageToDict

from mosaicolabs import IMU, Time, Vector3d
from mosaicolabs.bridges.mcap import (
    MCAPAdapterBase,
    MCAPMessage,
)
from mosaicolabs.bridges.mcap.bridge import (
    MCAPBridge,
    compute_mcap_msg_type,
    register_default_adapter,
)
from mosaicolabs.models.core import Message as Message

from ..config import (
    make_imu_mcap,
)
from ..utils.generated.imu_pb2 import Imu as ImuProtobuf


class UnsupportedAdapter(MCAPAdapterBase[IMU, Dict]):
    """Custom adapter created for the Imu.proto message available at src/testing/unit/bridges/utils/proto/imu.proto"""

    schema_name = "Mosaico.invented.data"
    schema_encoding = "not-supported-encoding"


class MyImuProtobufAdapter(MCAPAdapterBase[IMU, ImuProtobuf]):
    """Custom adapter created for the Imu.proto message available at src/testing/unit/bridges/utils/proto/imu.proto"""

    schema_name = "Mosaico.Imu"
    schema_encoding = "protobuf"
    __mosaico_ontology_type__: Type[IMU] = IMU

    @classmethod
    def from_dict(cls, mcap_data: dict) -> IMU:

        mcap_acceleration = mcap_data["linear_acceleration"]
        mcap_angular_velocity = mcap_data["angular_velocity"]

        return IMU(
            acceleration=Vector3d(
                x=mcap_acceleration["x"],
                y=mcap_acceleration["y"],
                z=mcap_acceleration["z"],
            ),
            angular_velocity=Vector3d(
                x=mcap_angular_velocity["x"],
                y=mcap_angular_velocity["y"],
                z=mcap_angular_velocity["z"],
            ),
        )


class MyImuJsonschemaAdapter(MCAPAdapterBase[IMU, Dict]):
    """Custom adapter created for the Imu jsonschema message available at src/testing/unit/bridges/utils/jsonschema/imu.schema.json"""

    schema_name = "Mosaico.Imu"
    schema_encoding = "jsonschema"
    __mosaico_ontology_type__: Type[IMU] = IMU

    @classmethod
    def from_dict(cls, mcap_data: dict) -> IMU:

        mcap_acceleration = mcap_data["linear_acceleration"]
        mcap_angular_velocity = mcap_data["angular_velocity"]

        return IMU(
            acceleration=Vector3d(
                x=mcap_acceleration["x"],
                y=mcap_acceleration["y"],
                z=mcap_acceleration["z"],
            ),
            angular_velocity=Vector3d(
                x=mcap_angular_velocity["x"],
                y=mcap_angular_velocity["y"],
                z=mcap_angular_velocity["z"],
            ),
        )


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
    register_default_adapter(MyImuJsonschemaAdapter)

    imu_adapter_protobuf_type = compute_mcap_msg_type(
        MyImuProtobufAdapter.schema_name, MyImuProtobufAdapter.schema_encoding
    )
    imu_adapter_jsonschema = compute_mcap_msg_type(
        MyImuJsonschemaAdapter.schema_name, MyImuJsonschemaAdapter.schema_encoding
    )

    # Try registering again -> raises error
    with pytest.raises(
        ValueError,
        match=f"Adapter for MCAP message type '{imu_adapter_protobuf_type}' is already registered",
    ):
        register_default_adapter(MyImuProtobufAdapter)

    assert MCAPBridge.get_default_adapters() == {
        imu_adapter_protobuf_type: MyImuProtobufAdapter,
        imu_adapter_jsonschema: MyImuJsonschemaAdapter,
    }
    assert (
        MCAPBridge.get_default_adapter("Mosaico.Imu", "protobuf")
        == MyImuProtobufAdapter
    )
    assert (
        MCAPBridge.get_default_adapter("Mosaico.Imu", "jsonschema")
        == MyImuJsonschemaAdapter
    )

    assert MCAPBridge.is_adapted(IMU)
    assert MCAPBridge.is_msgtype_adapted(imu_adapter_protobuf_type)


def test_custom_adapter_translate():
    """Test that MyImuProtobufAdapter custom adapter create correctly a Mosaico IMU message"""

    msg = make_imu_mcap(Time(seconds=0, nanoseconds=0), "protobuf")

    msg_dict = MessageToDict(
        msg,
        always_print_fields_with_no_presence=True,
        preserving_proto_field_name=True,
        use_integers_for_enums=True,
        descriptor_pool=None,
        unquote_int64_if_possible=True,
    )

    mcap_message = MCAPMessage(
        channel_name="front_car/imu",
        channel_encoding="protobuf",
        schema_name=MyImuProtobufAdapter.schema_name,
        schema_encoding=MyImuProtobufAdapter.schema_encoding,
        schema_def=None,
        data=msg_dict,
        log_time_ns=1,
        publish_time_ns=1,
    )

    msco_imu = MyImuProtobufAdapter.translate(mcap_message).get_data(IMU)

    assert msco_imu is not None
    assert msco_imu.acceleration.x == msg_dict["linear_acceleration"]["x"]
    assert msco_imu.acceleration.y == msg_dict["linear_acceleration"]["y"]
    assert msco_imu.acceleration.z == msg_dict["linear_acceleration"]["z"]
    assert msco_imu.angular_velocity.x == msg_dict["angular_velocity"]["x"]
    assert msco_imu.angular_velocity.y == msg_dict["angular_velocity"]["y"]
    assert msco_imu.angular_velocity.z == msg_dict["angular_velocity"]["z"]


def test_custom_adapter_to_native():
    """TODO"""
    pass
