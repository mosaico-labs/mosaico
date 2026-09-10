# QoS profiles for ROS2 bag writing.
#
# Standard predefined profiles mirror the constants in rclcpp / rclpy (rmw layer).
# See: https://docs.ros.org/en/rolling/Concepts/Intermediate/About-Quality-of-Service-Settings.html

from rosbags.interfaces import (
    Qos,
    QosDurability,
    QosHistory,
    QosLiveliness,
    QosReliability,
    QosTime,
)

_NO_DEADLINE = QosTime(0, 0)

# Default — matches the ROS2 publisher/subscriber defaults (mirrors qos_profile_default).
DEFAULT_QOS = Qos(
    history=QosHistory.KEEP_LAST,
    depth=10,
    reliability=QosReliability.RELIABLE,
    durability=QosDurability.VOLATILE,
    deadline=_NO_DEADLINE,
    lifespan=_NO_DEADLINE,
    liveliness=QosLiveliness.SYSTEM_DEFAULT,
    liveliness_lease_duration=_NO_DEADLINE,
    avoid_ros_namespace_conventions=False,
)

# Services (qos_profile_services) — same as default.
SERVICES_QOS = Qos(
    history=QosHistory.KEEP_LAST,
    depth=10,
    reliability=QosReliability.RELIABLE,
    durability=QosDurability.VOLATILE,
    deadline=_NO_DEADLINE,
    lifespan=_NO_DEADLINE,
    liveliness=QosLiveliness.SYSTEM_DEFAULT,
    liveliness_lease_duration=_NO_DEADLINE,
    avoid_ros_namespace_conventions=False,
)

# Sensor data (qos_profile_sensor_data) — BEST_EFFORT, smaller queue.
SENSOR_DATA_QOS = Qos(
    history=QosHistory.KEEP_LAST,
    depth=5,
    reliability=QosReliability.BEST_EFFORT,
    durability=QosDurability.VOLATILE,
    deadline=_NO_DEADLINE,
    lifespan=_NO_DEADLINE,
    liveliness=QosLiveliness.SYSTEM_DEFAULT,
    liveliness_lease_duration=_NO_DEADLINE,
    avoid_ros_namespace_conventions=False,
)

# Parameters (qos_profile_parameters) — large queue for parameter server.
PARAMETERS_QOS = Qos(
    history=QosHistory.KEEP_LAST,
    depth=1000,
    reliability=QosReliability.RELIABLE,
    durability=QosDurability.VOLATILE,
    deadline=_NO_DEADLINE,
    lifespan=_NO_DEADLINE,
    liveliness=QosLiveliness.SYSTEM_DEFAULT,
    liveliness_lease_duration=_NO_DEADLINE,
    avoid_ros_namespace_conventions=False,
)

# Parameter events (qos_profile_parameter_events).
PARAMETER_EVENTS_QOS = Qos(
    history=QosHistory.KEEP_LAST,
    depth=1000,
    reliability=QosReliability.RELIABLE,
    durability=QosDurability.VOLATILE,
    deadline=_NO_DEADLINE,
    lifespan=_NO_DEADLINE,
    liveliness=QosLiveliness.SYSTEM_DEFAULT,
    liveliness_lease_duration=_NO_DEADLINE,
    avoid_ros_namespace_conventions=False,
)

# Clock (qos_profile_clock) — BEST_EFFORT for /clock topic.
CLOCK_QOS = Qos(
    history=QosHistory.KEEP_LAST,
    depth=10,
    reliability=QosReliability.BEST_EFFORT,
    durability=QosDurability.VOLATILE,
    deadline=_NO_DEADLINE,
    lifespan=_NO_DEADLINE,
    liveliness=QosLiveliness.SYSTEM_DEFAULT,
    liveliness_lease_duration=_NO_DEADLINE,
    avoid_ros_namespace_conventions=False,
)

# System default — all fields delegated to the underlying middleware.
SYSTEM_DEFAULT_QOS = Qos(
    history=QosHistory.SYSTEM_DEFAULT,
    depth=0,
    reliability=QosReliability.SYSTEM_DEFAULT,
    durability=QosDurability.SYSTEM_DEFAULT,
    deadline=_NO_DEADLINE,
    lifespan=_NO_DEADLINE,
    liveliness=QosLiveliness.SYSTEM_DEFAULT,
    liveliness_lease_duration=_NO_DEADLINE,
    avoid_ros_namespace_conventions=False,
)

# Static transforms — TRANSIENT_LOCAL (latched) so late subscribers receive the last message.
TF_STATIC_QOS = Qos(
    history=QosHistory.KEEP_LAST,
    depth=1,
    reliability=QosReliability.RELIABLE,
    durability=QosDurability.TRANSIENT_LOCAL,
    deadline=_NO_DEADLINE,
    lifespan=_NO_DEADLINE,
    liveliness=QosLiveliness.SYSTEM_DEFAULT,
    liveliness_lease_duration=_NO_DEADLINE,
    avoid_ros_namespace_conventions=False,
)

# Robot description — TRANSIENT_LOCAL (latched) so late subscribers receive the last message.
ROBOT_DESCRIPTION_QOS = Qos(
    history=QosHistory.UNKNOWN,
    depth=0,
    reliability=QosReliability.RELIABLE,
    durability=QosDurability.TRANSIENT_LOCAL,
    deadline=_NO_DEADLINE,
    lifespan=_NO_DEADLINE,
    liveliness=QosLiveliness.AUTOMATIC,
    liveliness_lease_duration=_NO_DEADLINE,
    avoid_ros_namespace_conventions=False,
)

# Well-known topic names → non-default QoS.
# Keys are matched exactly or as a trailing path segment (e.g. "tf_static" matches "/robot/tf_static").
_TOPIC_QOS_OVERRIDES: dict[str, Qos] = {
    "robot_description": ROBOT_DESCRIPTION_QOS,
    "tf_static": TF_STATIC_QOS,
    "/clock": CLOCK_QOS,
    "/parameter_events": PARAMETER_EVENTS_QOS,
}


def get_qos_for_topic(topic: str) -> list[Qos]:
    """Return the appropriate QoS profile for a topic name."""
    if topic in _TOPIC_QOS_OVERRIDES:
        return [_TOPIC_QOS_OVERRIDES[topic]]
    for suffix, qos in _TOPIC_QOS_OVERRIDES.items():
        if topic.endswith(f"/{suffix}"):
            return [qos]
    return [DEFAULT_QOS]
