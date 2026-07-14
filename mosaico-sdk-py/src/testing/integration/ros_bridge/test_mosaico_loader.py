import pytest
from rosbags.typesys import Stores, get_typestore

from mosaicolabs import Pose, Serializable, SessionLevelErrorPolicy
from mosaicolabs.ros_bridge import MosaicoLoader


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
        adapter, rosmsg_type = mosaico_loader._resolve_topic_adapter(t_handler)

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
            mosaico_loader._resolve_topic_adapter(t_handler)

        mosaico_client.sequence_delete(ros_sequence_name)


def test_no_msgtype_fallback_to_default_adapter(mosaico_client):
    ros_distro = Stores.ROS2_JAZZY
    ros_sequence_name = "ros-sequence-no-msgtype-fallabck-to-default-adapter"
    ros_topic_name = "/car/pose"
    topic_with_ros_metadata = {}

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

        adapter, rosmsg_type = mosaico_loader._resolve_topic_adapter(t_handler)

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
            s_writer.topic_create(
                ros_topic_name, topic_with_ros_metadata, NotAdaptedClass
            )  # Serializable has no adapter!

        # Reading topic
        t_handler = mosaico_client.topic_handler(ros_sequence_name, ros_topic_name)

        mosaico_loader = MosaicoLoader(
            mosaico_client, get_typestore(ros_distro), ros_sequence_name
        )

        adapter, rosmsg_type = mosaico_loader._resolve_topic_adapter(t_handler)

        assert adapter is None
        assert rosmsg_type == "not_adapted_ontology"

        mosaico_client.sequence_delete(ros_sequence_name)


def test_not_adapted_msgtype_fallack_to_default_adapter(
    mosaico_client,
):
    ros_distro = Stores.ROS2_JAZZY
    ros_sequence_name = "ros-sequence-not-adapted-msgtype-fallack-to-default-adapter"
    ros_topic_name = "/car/pose"
    topic_with_ros_metadata = {"_ros_": {"msgtype": "not-adapted"}}

    with mosaico_client:
        with mosaico_client.sequence_create(
            ros_sequence_name, {}, SessionLevelErrorPolicy.Delete
        ) as s_writer:
            s_writer.topic_create(ros_topic_name, topic_with_ros_metadata, Pose)

        t_handler = mosaico_client.topic_handler(ros_sequence_name, ros_topic_name)

        mosaico_loader = MosaicoLoader(
            mosaico_client, get_typestore(ros_distro), ros_sequence_name
        )

        adapter, rosmsg_type = mosaico_loader._resolve_topic_adapter(t_handler)

        assert adapter and adapter.ontology_data_type() is Pose
        assert (
            rosmsg_type is not None and rosmsg_type == adapter.get_default_ros_msg()
        )  # Here you need to get the default since topic metadata hints to a non existing adapter but the ontology tag is adapted and can fallback to default

        mosaico_client.sequence_delete(ros_sequence_name)
