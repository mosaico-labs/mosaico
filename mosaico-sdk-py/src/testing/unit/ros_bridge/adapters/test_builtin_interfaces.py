import pytest
from rosbags.typesys.stores import Stores, Typestore, get_typestore

from mosaicolabs import Duration, Message, Time
from mosaicolabs.ros_bridge.adapters.builtin_interfaces import (
    DurationAdapter,
    TimeAdapter,
)
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
############################### TestTimeAdapter ###############################
###############################################################################


@pytest.fixture
def time():
    return Time(seconds=1000, nanoseconds=500000000)


@pytest.fixture
def time_msg(time):
    return Message(
        data=time,
        timestamp_ns=time.to_nanoseconds(),
    )


@pytest.fixture
def time_rosmsg(time: Time):
    return ROSMessage(
        bag_timestamp_ns=10000,
        topic="/clock",
        msg_type="builtin_interfaces/msg/Time",
        data={"sec": time.seconds, "nanosec": time.nanoseconds},
    )


class TestTimeAdapter:
    def test_translate_time(self, time_rosmsg: ROSMessage, time: Time):
        ms_msg = TimeAdapter.translate(time_rosmsg)

        assert ms_msg.get_data(Time).seconds == time.seconds
        assert ms_msg.get_data(Time).nanoseconds == time.nanoseconds
        assert ms_msg.timestamp_ns == time_rosmsg.bag_timestamp_ns

    def test_translate_raise_missing_required_key(self, time_rosmsg: ROSMessage):
        data = time_rosmsg.data
        data.pop("sec")
        with pytest.raises(ValueError, match="missing required keys"):
            TimeAdapter.from_dict(data)

    @pytest.mark.parametrize("typestore", ROS_TYPESTORE_TO_TEST)
    def test_to_ros_time(self, time: Time, typestore: Typestore):
        ros_msg = TimeAdapter.to_ros(time, typestore, "builtin_interfaces/msg/Time")

        assert ros_msg.sec == time.seconds
        assert ros_msg.nanosec == time.nanoseconds

    @pytest.mark.parametrize("typestore", ROS_TYPESTORE_TO_TEST)
    def test_to_ros_default_type(self, time: Time, typestore: Typestore):
        ros_msg = TimeAdapter.to_ros(time, typestore)

        assert ros_msg.sec == time.seconds
        assert ros_msg.nanosec == time.nanoseconds

    @pytest.mark.parametrize("typestore", ROS_TYPESTORE_TO_TEST)
    def test_to_ros_message(self, time_msg: Message, typestore: Typestore):
        time = time_msg.get_data(Time)
        ros_msg = TimeAdapter.to_ros(time_msg, typestore, "builtin_interfaces/msg/Time")

        assert ros_msg.sec == time.seconds
        assert ros_msg.nanosec == time.nanoseconds

    def test_to_ros_invalid_rosmsg_type(self, time: Time):

        with pytest.raises(
            TypeError,
            match=f"Adapter {TimeAdapter.__name__} does not support builtin_interfaces/msg/Bogus",
        ):
            TimeAdapter.to_ros(
                time, get_typestore(Stores.LATEST), "builtin_interfaces/msg/Bogus"
            )

    def test_to_ros_invalid_mosaico_type(self, invalid_ms_msg):
        with pytest.raises(TypeError):
            TimeAdapter.to_ros(invalid_ms_msg, get_typestore(Stores.LATEST))


###############################################################################
############################# TestDurationAdapter #############################
###############################################################################


@pytest.fixture
def duration():
    return Duration(seconds=1000, nanoseconds=500000000)


@pytest.fixture
def duration_msg(duration: Duration):
    return Message(
        data=duration,
        timestamp_ns=duration.to_nanoseconds(),
    )


@pytest.fixture
def duration_rosmsg(duration: Duration):
    return ROSMessage(
        bag_timestamp_ns=10000,
        topic="/duration",
        msg_type="builtin_interfaces/msg/Duration",
        data={"sec": duration.seconds, "nanosec": duration.nanoseconds},
    )


class TestDurationAdapter:
    def test_translate_duration(self, duration_rosmsg: ROSMessage):
        ms_msg = DurationAdapter.translate(duration_rosmsg)

        assert ms_msg.get_data(Duration).seconds == duration_rosmsg.data["sec"]
        assert ms_msg.get_data(Duration).nanoseconds == duration_rosmsg.data["nanosec"]
        assert ms_msg.timestamp_ns == duration_rosmsg.bag_timestamp_ns

    def test_translate_raise_missing_required_key(self, duration_rosmsg: ROSMessage):
        data = duration_rosmsg.data
        data.pop("sec")
        with pytest.raises(ValueError, match="missing required keys"):
            DurationAdapter.from_dict(data)

    @pytest.mark.parametrize("typestore", ROS_TYPESTORE_TO_TEST)
    def test_to_ros_duration(self, duration: Duration, typestore: Typestore):
        ros_msg = DurationAdapter.to_ros(
            duration, typestore, "builtin_interfaces/msg/Duration"
        )

        assert ros_msg.sec == duration.seconds
        assert ros_msg.nanosec == duration.nanoseconds

    @pytest.mark.parametrize("typestore", ROS_TYPESTORE_TO_TEST)
    def test_to_ros_default_type(self, duration: Duration, typestore: Typestore):
        ros_msg = DurationAdapter.to_ros(duration, typestore)

        assert ros_msg.sec == duration.seconds
        assert ros_msg.nanosec == duration.nanoseconds

    @pytest.mark.parametrize("typestore", ROS_TYPESTORE_TO_TEST)
    def test_to_ros_message(self, duration_msg: Message, typestore: Typestore):
        duration = duration_msg.get_data(Duration)
        ros_msg = DurationAdapter.to_ros(
            duration_msg, typestore, "builtin_interfaces/msg/Duration"
        )

        assert ros_msg.sec == duration.seconds
        assert ros_msg.nanosec == duration.nanoseconds

    def test_to_ros_invalid_rosmsg_type(self, duration: Duration):

        with pytest.raises(
            TypeError,
            match=f"Adapter {DurationAdapter.__name__} does not support builtin_interfaces/msg/Bogus",
        ):
            DurationAdapter.to_ros(
                duration, get_typestore(Stores.LATEST), "builtin_interfaces/msg/Bogus"
            )

    def test_to_ros_invalid_mosaico_type(self, invalid_ms_msg):
        with pytest.raises(TypeError):
            DurationAdapter.to_ros(invalid_ms_msg, get_typestore(Stores.LATEST))
