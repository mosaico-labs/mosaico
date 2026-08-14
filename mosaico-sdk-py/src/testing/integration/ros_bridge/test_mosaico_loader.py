import pyarrow as pa
import pytest
from rosbags.typesys import Stores, get_typestore

from mosaicolabs import (
    Message,
    Point3d,
    Pose,
    Quaternion,
    Serializable,
    SessionLevelErrorPolicy,
)
from mosaicolabs.models.core import resolve_ontology_class
from mosaicolabs.ros_bridge import MosaicoLoader
from mosaicolabs.ros_bridge.adapters import UnmodeledAdapter


def test_valid_msgtype(mosaico_client):
    ros_distro = Stores.ROS2_JAZZY
    ros_sequence_name = "ros-sequence-valid-msgtype"
    ros_topic_name = "/car/pose"
    topic_with_ros_metadata = {"_ros_": {"msgtype": "geometry_msgs/msg/PoseStamped"}}

    with mosaico_client:
        # Writing topic
        with mosaico_client.sequence_create(
            ros_sequence_name, {}, SessionLevelErrorPolicy.Delete
        ) as s_writer:
            s_writer.topic_create(ros_topic_name, topic_with_ros_metadata, Pose)

        t_handler = mosaico_client.topic_handler(ros_sequence_name, ros_topic_name)

        # Reading topic
        mosaico_loader = MosaicoLoader(
            mosaico_client, get_typestore(ros_distro), ros_sequence_name
        )
        adapter, rosmsg_type = mosaico_loader._get_or_create_adapter(t_handler)

        assert adapter and adapter.ontology_data_type() is Pose
        assert (
            rosmsg_type is not None and rosmsg_type == "geometry_msgs/msg/PoseStamped"
        )

        mosaico_client.sequence_delete(ros_sequence_name)


def test_invalid_msgtype(mosaico_client):
    ros_distro = Stores.ROS2_JAZZY
    ros_sequence_name = "ros-sequence-invalid-msgtype"
    ros_topic_name = "/car/pose"
    topic_with_ros_metadata = {"_ros_": {"msgtype": 1234}}

    with mosaico_client:
        # Writing topic
        with mosaico_client.sequence_create(
            ros_sequence_name, {}, SessionLevelErrorPolicy.Delete
        ) as s_writer:
            s_writer.topic_create(ros_topic_name, topic_with_ros_metadata, Pose)

        # Reading topic
        t_handler = mosaico_client.topic_handler(ros_sequence_name, ros_topic_name)

        mosaico_loader = MosaicoLoader(
            mosaico_client, get_typestore(ros_distro), ros_sequence_name
        )

        with pytest.raises(
            TypeError,
            match=f"Topic {t_handler.name} contains msgtype within metadata but it has unexpected type.",
        ):
            mosaico_loader._get_or_create_adapter(t_handler)

        mosaico_client.sequence_delete(ros_sequence_name)


def test_no_msgtype_fallback_to_default_adapter(mosaico_client):
    ros_distro = Stores.ROS2_JAZZY
    ros_sequence_name = "ros-sequence-no-msgtype-fallback-to-default-adapter"
    ros_topic_name = "/car/pose"
    topic_with_ros_metadata = {}

    with mosaico_client:
        # Writing topic
        with mosaico_client.sequence_create(
            ros_sequence_name, {}, SessionLevelErrorPolicy.Delete
        ) as s_writer:
            t_writer = s_writer.topic_create(
                ros_topic_name, topic_with_ros_metadata, Pose
            )

            # Create and push data
            pose_data = Pose(
                position=Point3d(x=1, y=2, z=3),
                orientation=Quaternion(x=0, y=0, z=0, w=1),
            )

            t_writer.push(Message(timestamp_ns=12345678, data=pose_data))

        # Reading topic
        t_handler = mosaico_client.topic_handler(ros_sequence_name, ros_topic_name)

        mosaico_loader = MosaicoLoader(
            mosaico_client, get_typestore(ros_distro), ros_sequence_name
        )

        adapter, rosmsg_type = mosaico_loader._get_or_create_adapter(t_handler)

        assert adapter and adapter.ontology_data_type() is Pose
        assert (
            rosmsg_type is not None and rosmsg_type == adapter.get_default_ros_msg()
        )  # Here you need to get the default since there are no info on topic's metadata

        mosaico_client.sequence_delete(ros_sequence_name)


class NotAdaptedClass(Serializable): ...


def test_no_adapter_available(mosaico_client):

    ros_distro = Stores.ROS2_JAZZY
    ros_sequence_name = "ros-sequence-no-adapter-available"
    ros_topic_name = "/car/pose"
    topic_with_ros_metadata = {"_ros_": {"msgtype": "not_adapted_ontology"}}

    with mosaico_client:
        # Writing topic
        with mosaico_client.sequence_create(
            ros_sequence_name, {}, SessionLevelErrorPolicy.Delete
        ) as s_writer:
            t_writer = s_writer.topic_create(
                ros_topic_name, topic_with_ros_metadata, NotAdaptedClass
            )  # Serializable has no adapter!

            # Create and push data
            unadapted_data = NotAdaptedClass()

            t_writer.push(Message(timestamp_ns=12345678, data=unadapted_data))

        # Reading topic
        t_handler = mosaico_client.topic_handler(ros_sequence_name, ros_topic_name)

        mosaico_loader = MosaicoLoader(
            mosaico_client, get_typestore(ros_distro), ros_sequence_name
        )

        adapter, rosmsg_type = mosaico_loader._get_or_create_adapter(t_handler)

        assert issubclass(adapter, UnmodeledAdapter)
        assert rosmsg_type == "not_adapted_ontology"

        mosaico_client.sequence_delete(ros_sequence_name)


def test_not_adapted_msgtype_fallack_to_default_adapter(
    mosaico_client,
):
    ros_distro = Stores.ROS2_JAZZY
    ros_sequence_name = "ros-sequence-not-adapted-msgtype-fallack-to-default-adapter"
    ros_topic_name = "/car/pose"
    topic_with_ros_metadata = {"_ros_": {"msgtype": "not-helpful-msgtype"}}

    with mosaico_client:
        with mosaico_client.sequence_create(
            ros_sequence_name, {}, SessionLevelErrorPolicy.Delete
        ) as s_writer:
            t_writer = s_writer.topic_create(
                ros_topic_name, topic_with_ros_metadata, Pose
            )

            # Create and push data
            pose_data = Pose(
                position=Point3d(x=1, y=2, z=3),
                orientation=Quaternion(x=0, y=0, z=0, w=1),
            )

            t_writer.push(Message(timestamp_ns=12345678, data=pose_data))

        t_handler = mosaico_client.topic_handler(ros_sequence_name, ros_topic_name)

        mosaico_loader = MosaicoLoader(
            mosaico_client, get_typestore(ros_distro), ros_sequence_name
        )

        adapter, rosmsg_type = mosaico_loader._get_or_create_adapter(t_handler)

        assert adapter and adapter.ontology_data_type() is Pose
        assert (
            rosmsg_type is not None and rosmsg_type == adapter.get_default_ros_msg()
        )  # Here you need to get the default since topic metadata hints to a non existing adapter but the ontology tag is adapted and can fallback to default

        mosaico_client.sequence_delete(ros_sequence_name)


UnmodeledFlowSensor = resolve_ontology_class(
    ontology_tag="FlowSensor",
    schema=pa.struct(
        [
            pa.field(
                "fluid_pressure",
                pa.float32(),
                nullable=False,
            ),
            pa.field(
                "variance",
                pa.float32(),
                nullable=False,
            ),
        ]
    ),
)


unmodeled_fluid_pressure_msgdef = """
float32 fluid_pressure  # Absolute pressure reading in Pascals.
float32 variance        # 0 is interpreted as variance unknown
"""


def test_unmodeled_adapter(mosaico_client):
    """Test with unmodeled ontology"""
    ros_distro = Stores.ROS2_JAZZY
    ros_sequence_name = "ros-sequence-unmodeled-adapter"
    ros_topic_name = "/gasoline_tube_flow"
    fluid_pressure_metadata = {
        "_ros_": {
            "msgtype": "custom_msgs/msg/MyFluidPressure",
            "msgdef": unmodeled_fluid_pressure_msgdef,
        }
    }

    with mosaico_client:
        # Writing topic
        with mosaico_client.sequence_create(
            ros_sequence_name, {}, SessionLevelErrorPolicy.Delete
        ) as s_writer:
            t_writer = s_writer.topic_create(
                ros_topic_name, fluid_pressure_metadata, UnmodeledFlowSensor
            )

            # Create and Push a message like a default ontology
            unm_data = UnmodeledFlowSensor(
                raw_data={"fluid_pressure": 1, "variance": 0}
            )
            t_writer.push(Message(timestamp_ns=12345678, data=unm_data))

        # Reading topic
        t_handler = mosaico_client.topic_handler(ros_sequence_name, ros_topic_name)

        mosaico_loader = MosaicoLoader(
            mosaico_client, get_typestore(ros_distro), ros_sequence_name
        )

        adapter, rosmsg_type = mosaico_loader._get_or_create_adapter(t_handler)

        assert adapter is not None
        assert issubclass(adapter, UnmodeledAdapter)
        assert (
            adapter.ontology_data_type().ontology_tag()
            == UnmodeledFlowSensor.ontology_tag()
        )
        assert (
            adapter.ontology_data_type().__schema_fingerprint__
            == UnmodeledFlowSensor.__schema_fingerprint__
        )
        assert adapter.get_default_ros_msg() == "custom_msgs/msg/MyFluidPressure"
        assert rosmsg_type == "custom_msgs/msg/MyFluidPressure"

        mosaico_client.sequence_delete(ros_sequence_name)


UnmodeledTemperature = resolve_ontology_class(
    ontology_tag="Temperature",  # ontology tag  is the same as the registered Temperature one, creating then an Unmodeled ontology
    schema=pa.struct(
        [
            pa.field(
                "fluid_temperature",
                pa.float64(),
                nullable=False,
            ),
            pa.field(
                "variance",
                pa.float64(),
                nullable=False,
            ),
        ]
    ),
)


def test_unmodeled_adapter_with_existing_ontology_tag(mosaico_client):

    unmodeled_fluid_temperature_msgdef = """
    float64 fluid_temperature  # Absolute temperature Degrees.
    float64 variance        # 0 is interpreted as variance unknown
    """

    """Test with unmodeled ontology containing existing ontology_tag"""
    ros_distro = Stores.ROS2_JAZZY
    ros_sequence_name = "ros-sequence-unmodeled-adapter"
    ros_topic_name = "/gasoline_tube_temperature"
    fluid_pressure_metadata = {
        "_ros_": {
            "msgtype": "custom_msgs/msg/MyFluidTemperature",
            "msgdef": unmodeled_fluid_temperature_msgdef,
        }
    }

    with mosaico_client:
        # Writing topic
        with mosaico_client.sequence_create(
            ros_sequence_name, {}, SessionLevelErrorPolicy.Delete
        ) as s_writer:
            t_writer = s_writer.topic_create(
                ros_topic_name, fluid_pressure_metadata, UnmodeledTemperature
            )

            # Create and Push a message like a default ontology
            unm_data = UnmodeledTemperature(
                raw_data={"fluid_temperature": 1, "variance": 0}
            )
            t_writer.push(Message(timestamp_ns=12345678, data=unm_data))

        # Reading topic
        t_handler = mosaico_client.topic_handler(ros_sequence_name, ros_topic_name)

        mosaico_loader = MosaicoLoader(
            mosaico_client, get_typestore(ros_distro), ros_sequence_name
        )

        adapter, rosmsg_type = mosaico_loader._get_or_create_adapter(t_handler)

        assert adapter is not None
        assert issubclass(adapter, UnmodeledAdapter)
        assert (
            adapter.ontology_data_type().ontology_tag()
            == UnmodeledTemperature.ontology_tag()
        )
        assert (
            adapter.ontology_data_type().__schema_fingerprint__
            == UnmodeledTemperature.__schema_fingerprint__
        )
        assert adapter.get_default_ros_msg() == "custom_msgs/msg/MyFluidTemperature"
        assert rosmsg_type == "custom_msgs/msg/MyFluidTemperature"

        mosaico_client.sequence_delete(ros_sequence_name)


UnmodeledImu = resolve_ontology_class(
    ontology_tag="FlowSensor",
    schema=pa.struct(
        [
            pa.field(
                "angular_velocity",
                pa.struct(
                    [
                        pa.field("x", pa.float64(), nullable=False),
                        pa.field("y", pa.float64(), nullable=False),
                        pa.field("z", pa.float64(), nullable=False),
                    ]
                ),
                nullable=False,
            ),
            pa.field(
                "linear_acceleration",
                pa.struct(
                    [
                        pa.field("x", pa.float64(), nullable=False),
                        pa.field("y", pa.float64(), nullable=False),
                        pa.field("z", pa.float64(), nullable=False),
                    ]
                ),
                nullable=False,
            ),
        ]
    ),
)


unmodeled_imu_msgdef = """
Vector3 angular_velocity
Vector3 linear_acceleration

================================================================================
MSG: custom_msgs/msg/Vector3
float64 x
float64 y
float64 z
"""


def test_unmodeled_adapter_with_nested_msgdef(mosaico_client):
    """Test with unmodeled ontology whose msgdef contains nested custom struct types"""
    ros_distro = Stores.ROS2_JAZZY
    ros_sequence_name = "ros-sequence-unmodeled-adapter-nested-msgdef"
    ros_topic_name = "/custom_imu"
    imu_metadata = {
        "_ros_": {
            "msgtype": "custom_msgs/msg/CustomImu",
            "msgdef": unmodeled_imu_msgdef,
        }
    }

    with mosaico_client:
        # Writing topic
        with mosaico_client.sequence_create(
            ros_sequence_name, {}, SessionLevelErrorPolicy.Delete
        ) as s_writer:
            t_writer = s_writer.topic_create(ros_topic_name, imu_metadata, UnmodeledImu)

            # Create and push a message with nested struct payload
            unm_data = UnmodeledImu(
                raw_data={
                    "angular_velocity": {"x": 0.1, "y": 0.2, "z": 0.3},
                    "linear_acceleration": {"x": 1.0, "y": 2.0, "z": 3.0},
                }
            )
            t_writer.push(Message(timestamp_ns=12345678, data=unm_data))

        # Reading topic
        t_handler = mosaico_client.topic_handler(ros_sequence_name, ros_topic_name)

        mosaico_loader = MosaicoLoader(
            mosaico_client, get_typestore(ros_distro), ros_sequence_name
        )

        adapter, rosmsg_type = mosaico_loader._get_or_create_adapter(t_handler)

        assert adapter is not None
        assert issubclass(adapter, UnmodeledAdapter)
        assert (
            adapter.ontology_data_type().ontology_tag() == UnmodeledImu.ontology_tag()
        )
        assert (
            adapter.ontology_data_type().__schema_fingerprint__
            == UnmodeledImu.__schema_fingerprint__
        )
        assert adapter.get_default_ros_msg() == "custom_msgs/msg/CustomImu"
        assert rosmsg_type == "custom_msgs/msg/CustomImu"

        mosaico_client.sequence_delete(ros_sequence_name)
