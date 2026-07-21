import pytest
from rosbags.typesys.store import Typestore
from rosbags.typesys.stores import Stores, get_typestore

from mosaicolabs import (
    Boolean,
    Floating32,
    Floating64,
    Integer8,
    Integer16,
    Integer32,
    Integer64,
    Message,
    Serializable,
    String,
    Unsigned8,
    Unsigned16,
    Unsigned32,
    Unsigned64,
)
from mosaicolabs.ros_bridge import ROSBridge
from mosaicolabs.ros_bridge.adapters.std_msgs import ROSAdapterBase
from mosaicolabs.ros_bridge.ros_message import ROSMessage

ROS_TYPESTORE_TO_TEST = [
    get_typestore(Stores.LATEST),
    get_typestore(Stores.ROS1_NOETIC),
    get_typestore(Stores.ROS2_DASHING),
    get_typestore(Stores.ROS2_ELOQUENT),
    get_typestore(Stores.ROS2_FOXY),
    get_typestore(Stores.ROS2_GALACTIC),
    get_typestore(Stores.ROS2_HUMBLE),
    get_typestore(Stores.ROS2_IRON),
    get_typestore(Stores.ROS2_JAZZY),
    get_typestore(Stores.ROS2_KILTED),
]


###############################################################################
############################ TestGenericStdAdapter ############################
###############################################################################


StringAdapter = ROSBridge._default_adapters["std_msgs/msg/String"]
Int8Adapter = ROSBridge._default_adapters["std_msgs/msg/Int8"]
Int16Adapter = ROSBridge._default_adapters["std_msgs/msg/Int16"]
Int32Adapter = ROSBridge._default_adapters["std_msgs/msg/Int32"]
Int64Adapter = ROSBridge._default_adapters["std_msgs/msg/Int64"]
UInt8Adapter = ROSBridge._default_adapters["std_msgs/msg/UInt8"]
UInt16Adapter = ROSBridge._default_adapters["std_msgs/msg/UInt16"]
UInt32Adapter = ROSBridge._default_adapters["std_msgs/msg/UInt32"]
UInt64Adapter = ROSBridge._default_adapters["std_msgs/msg/UInt64"]
Float32Adapter = ROSBridge._default_adapters["std_msgs/msg/Float32"]
Float64Adapter = ROSBridge._default_adapters["std_msgs/msg/Float64"]
BoolAdapter = ROSBridge._default_adapters["std_msgs/msg/Bool"]

ADAPTERS_TO_TEST = [
    StringAdapter,
    Int8Adapter,
    Int16Adapter,
    Int32Adapter,
    Int64Adapter,
    UInt8Adapter,
    UInt16Adapter,
    UInt32Adapter,
    UInt64Adapter,
    Float32Adapter,
    Float64Adapter,
    BoolAdapter,
]

MS_STD_MSGS_TO_TEST = [
    (String(data="mosaico_string"), StringAdapter, "std_msgs/msg/String"),
    (Integer8(data=42), Int8Adapter, "std_msgs/msg/Int8"),
    (Integer16(data=42), Int16Adapter, "std_msgs/msg/Int16"),
    (Integer32(data=42), Int32Adapter, "std_msgs/msg/Int32"),
    (Integer64(data=42), Int64Adapter, "std_msgs/msg/Int64"),
    (Unsigned8(data=42), UInt8Adapter, "std_msgs/msg/UInt8"),
    (Unsigned16(data=42), UInt16Adapter, "std_msgs/msg/UInt16"),
    (Unsigned32(data=42), UInt32Adapter, "std_msgs/msg/UInt32"),
    (Unsigned64(data=42), UInt64Adapter, "std_msgs/msg/UInt64"),
    (Floating32(data=42.0), Float32Adapter, "std_msgs/msg/Float32"),
    (Floating64(data=42.0), Float64Adapter, "std_msgs/msg/Float64"),
    (Boolean(data=True), BoolAdapter, "std_msgs/msg/Bool"),
]


def to_ROSMessage(data):
    return ROSMessage(
        bag_timestamp_ns=100,
        topic="/camera_info",
        msg_type="sensor_msgs/msg/CameraInfo",
        data={"data": data},
    )


ROS_STD_MSGS_TO_TEST = [
    (to_ROSMessage("my_string"), StringAdapter),
    (to_ROSMessage(1), Int8Adapter),
    (to_ROSMessage(2), Int16Adapter),
    (to_ROSMessage(3), Int32Adapter),
    (to_ROSMessage(4), Int64Adapter),
    (to_ROSMessage(5), UInt8Adapter),
    (to_ROSMessage(6), UInt16Adapter),
    (to_ROSMessage(7), UInt32Adapter),
    (to_ROSMessage(8), UInt64Adapter),
    (to_ROSMessage(9.0), Float32Adapter),
    (to_ROSMessage(10.0), Float64Adapter),
    (to_ROSMessage(True), BoolAdapter),
]


def to_ms_message(ms_type_instance: Serializable):
    return Message(data=ms_type_instance, timestamp_ns=100)


MESSAGE_TO_TEST = [
    (to_ms_message(ms_type_instance), adapter, rosmsg_type)
    for ms_type_instance, adapter, rosmsg_type in MS_STD_MSGS_TO_TEST
]


class TestGenericStdAdapter:
    def assert_std_msg(self, ms_std_msg, ros_msg):
        assert ms_std_msg.data == ros_msg.data

    @pytest.mark.parametrize("std_rosmsg, adapter", ROS_STD_MSGS_TO_TEST)
    def test_translate_std_msg_rosmsg(self, std_rosmsg, adapter: ROSAdapterBase):

        ms_msg = adapter.translate(std_rosmsg)

        assert (
            ms_msg.get_data(adapter.__mosaico_ontology_type__).data
            == std_rosmsg.data_field["data"]
        )

    @pytest.mark.parametrize("typestore", ROS_TYPESTORE_TO_TEST)
    @pytest.mark.parametrize("ms_type, adapter, rosmsg_type", MS_STD_MSGS_TO_TEST)
    def test_to_ros_std_msg(
        self,
        ms_type: Serializable,
        adapter: ROSAdapterBase,
        rosmsg_type: str,
        typestore: Typestore,
    ):
        ros_msg = adapter.to_ros(ms_type, typestore, rosmsg_type)

        self.assert_std_msg(ms_type, ros_msg)

    @pytest.mark.parametrize("typestore", ROS_TYPESTORE_TO_TEST)
    @pytest.mark.parametrize("ms_message, adapter, rosmsg_type", MESSAGE_TO_TEST)
    def test_to_ros_std_msg_message(
        self,
        ms_message: Message,
        adapter: ROSAdapterBase,
        rosmsg_type: str,
        typestore: Typestore,
    ):
        std_msg = ms_message.data
        ros_msg = adapter.to_ros(ms_message, typestore, rosmsg_type)

        self.assert_std_msg(std_msg, ros_msg)

    @pytest.mark.parametrize("typestore", ROS_TYPESTORE_TO_TEST)
    @pytest.mark.parametrize("ms_type, adapter, rosmsg_type", MS_STD_MSGS_TO_TEST)
    def test_to_ros_default_type(
        self,
        ms_type: Serializable,
        adapter: ROSAdapterBase,
        rosmsg_type: str,
        typestore: Typestore,
    ):
        ros_msg = adapter.to_ros(ms_type, typestore)

        self.assert_std_msg(ms_type, ros_msg)

    @pytest.mark.parametrize("ms_type, adapter, rosmsg_type", MS_STD_MSGS_TO_TEST)
    def test_to_ros_invalid_rosmsg_type(
        self, ms_type: Serializable, adapter: ROSAdapterBase, rosmsg_type: str
    ):

        with pytest.raises(
            TypeError,
            match=f"Adapter {adapter.__name__} does not support std_msgs/msg/Bogus",
        ):
            adapter.to_ros(ms_type, get_typestore(Stores.LATEST), "std_msgs/msg/Bogus")

    @pytest.mark.parametrize("adapter", ADAPTERS_TO_TEST)
    def test_to_ros_invalid_mosaico_type(
        self,
        invalid_ms_msg,
        adapter: ROSAdapterBase,
    ):
        with pytest.raises(TypeError):
            adapter.to_ros(invalid_ms_msg, get_typestore(Stores.LATEST))
