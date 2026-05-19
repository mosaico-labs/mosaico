from dataclasses import asdict

import numpy as np
import pytest
from rosbags.typesys.stores import Stores, Typestore, get_typestore

from mosaicolabs import (
    Acceleration,
    ForceTorque,
    Inertia,
    Message,
    Point3d,
    Polygon,
    Pose,
    Quaternion,
    Time,
    Transform,
    Vector3d,
    Velocity,
)
from mosaicolabs.ros_bridge.adapters import (
    AccelAdapter,
    InertiaAdapter,
    PointAdapter,
    PolygonAdapter,
    PoseAdapter,
    QuaternionAdapter,
    TransformAdapter,
    TwistAdapter,
    Vector3Adapter,
    WrenchAdapter,
)
from mosaicolabs.ros_bridge.ros_message import ROSMessage
from testing.unit.ros_bridge.adapters.helper import (
    assert_force_torque,
    assert_inertia,
    assert_point3d,
    assert_polygon,
    assert_pose,
    assert_quaternion,
    assert_transform,
    assert_vector3,
    asset_pose_w_cov,
)

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
############################# TestVector3Adapter ##############################
###############################################################################


@pytest.fixture
def vector3d_msg(vector3d):
    return Message(
        data=vector3d,
        timestamp_ns=100,
        frame_id="base_link",
    )


@pytest.fixture
def vector3d_rosmsg(vector3d: Vector3d):
    return ROSMessage(
        bag_timestamp_ns=100,
        topic="/vector3",
        msg_type="geometry_msgs/msg/Vector3",
        data=vector3d.model_dump(),
    )


@pytest.fixture
def vector3d_rosmsg_stamped(vector3d: Vector3d, ros_header):
    return ROSMessage(
        bag_timestamp_ns=100,
        topic="/vector3",
        msg_type="geometry_msgs/msg/Vector3Stamped",
        data={"header": ros_header, "vector": vector3d.model_dump()},
    )


class TestVectoradapter:
    def assert_vector3(self, vector3d: Vector3d, ros_msg: dict):
        assert vector3d.x == ros_msg["x"]
        assert vector3d.y == ros_msg["y"]
        assert vector3d.z == ros_msg["z"]

    def test_translate_vector(self, vector3d_rosmsg: ROSMessage):
        ms_msg = Vector3Adapter.translate(vector3d_rosmsg)

        assert_vector3(ms_msg.get_data(Vector3d), vector3d_rosmsg.data)
        assert ms_msg.timestamp_ns == vector3d_rosmsg.bag_timestamp_ns

    def test_translate_vector_stamped(self, vector3d_rosmsg_stamped: ROSMessage):
        ms_msg = Vector3Adapter.translate(vector3d_rosmsg_stamped)

        assert (
            ms_msg.timestamp_ns == vector3d_rosmsg_stamped.header.stamp.to_nanoseconds()
        )
        assert ms_msg.frame_id == vector3d_rosmsg_stamped.header.frame_id
        assert_vector3(
            ms_msg.get_data(Vector3d), vector3d_rosmsg_stamped.data["vector"]
        )

    def test_translate_raise_missing_required_key(self, vector3d: Vector3d):
        data = vector3d.model_dump()
        data.pop("z")
        with pytest.raises(ValueError):
            Vector3Adapter.from_dict(data)

    @pytest.mark.parametrize("typestore", ROS_TYPESTORE_TO_TEST)
    def test_to_ros_vector3(self, vector3d: Vector3d, typestore: Typestore):
        ros_msg = Vector3Adapter.to_ros(
            vector3d, typestore, "geometry_msgs/msg/Vector3"
        )

        assert_vector3(vector3d, asdict(ros_msg))

    @pytest.mark.parametrize("typestore", ROS_TYPESTORE_TO_TEST)
    def test_to_ros_vector3_stamped(self, vector3d_msg: Message, typestore: Typestore):
        vector3d = vector3d_msg.get_data(Vector3d)
        ros_msg = Vector3Adapter.to_ros(
            vector3d_msg, typestore, "geometry_msgs/msg/Vector3Stamped"
        )

        assert (
            vector3d_msg.timestamp_ns
            == Time(
                seconds=ros_msg.header.stamp.sec,
                nanoseconds=ros_msg.header.stamp.nanosec,
            ).to_nanoseconds()
        )
        assert vector3d_msg.frame_id == ros_msg.header.frame_id
        assert_vector3(vector3d, asdict(ros_msg.vector))

    @pytest.mark.parametrize("typestore", ROS_TYPESTORE_TO_TEST)
    def test_to_ros_default_type(self, vector3d: Vector3d, typestore: Typestore):
        ros_msg = Vector3Adapter.to_ros(vector3d, typestore)

        assert_vector3(vector3d, asdict(ros_msg))

    def test_to_ros_invalid_rosmsg_type(self, vector3d: Vector3d):
        ros_msg = Vector3Adapter.to_ros(
            vector3d, get_typestore(Stores.LATEST), "geometry_msgs/msg/Bogus"
        )

        assert ros_msg is None

    def test_to_ros_invalid_mosaico_type(self, invalid_ms_msg):
        with pytest.raises(TypeError):
            Vector3Adapter.to_ros(invalid_ms_msg, get_typestore(Stores.LATEST))


###############################################################################
############################### TestPointAdapter ##############################
###############################################################################


@pytest.fixture
def point3d_msg(point3d):
    return Message(
        data=point3d,
        timestamp_ns=100,
        frame_id="base_link",
    )


@pytest.fixture
def point3d_rosmsg(point3d: Point3d):
    return ROSMessage(
        bag_timestamp_ns=100,
        topic="/point3",
        msg_type="geometry_msgs/msg/Point",
        data=point3d.model_dump(),
    )


@pytest.fixture
def point3d_rosmsg_stamped(point3d: Point3d, ros_header):
    return ROSMessage(
        bag_timestamp_ns=100,
        topic="/point3",
        msg_type="geometry_msgs/msg/PointStamped",
        data={"header": ros_header, "point": point3d.model_dump()},
    )


class TestPointadapter:
    def test_translate_point(self, point3d_rosmsg: ROSMessage):
        ms_msg = PointAdapter.translate(point3d_rosmsg)

        assert_point3d(ms_msg.get_data(Point3d), point3d_rosmsg.data)
        assert ms_msg.timestamp_ns == point3d_rosmsg.bag_timestamp_ns

    def test_translate_point_stamped(self, point3d_rosmsg_stamped: ROSMessage):
        ms_msg = PointAdapter.translate(point3d_rosmsg_stamped)

        assert (
            ms_msg.timestamp_ns == point3d_rosmsg_stamped.header.stamp.to_nanoseconds()
        )
        assert ms_msg.frame_id == point3d_rosmsg_stamped.header.frame_id
        assert_point3d(ms_msg.get_data(Point3d), point3d_rosmsg_stamped.data["point"])

    def test_translate_raise_missing_required_key(self, point3d: Point3d):
        data = point3d.model_dump()
        data.pop("z")
        with pytest.raises(ValueError):
            PointAdapter.from_dict(data)

    @pytest.mark.parametrize("typestore", ROS_TYPESTORE_TO_TEST)
    def test_to_ros_point(self, point3d: Point3d, typestore: Typestore):
        ros_msg = PointAdapter.to_ros(point3d, typestore, "geometry_msgs/msg/Point")

        assert_point3d(point3d, asdict(ros_msg))

    @pytest.mark.parametrize("typestore", ROS_TYPESTORE_TO_TEST)
    def test_to_ros_point_stamped(self, point3d_msg: Message, typestore: Typestore):
        point3d = point3d_msg.get_data(Point3d)
        ros_msg = PointAdapter.to_ros(
            point3d_msg, typestore, "geometry_msgs/msg/PointStamped"
        )

        assert (
            point3d_msg.timestamp_ns
            == Time(
                seconds=ros_msg.header.stamp.sec,
                nanoseconds=ros_msg.header.stamp.nanosec,
            ).to_nanoseconds()
        )
        assert point3d_msg.frame_id == ros_msg.header.frame_id
        assert_point3d(point3d, asdict(ros_msg.point))

    @pytest.mark.parametrize("typestore", ROS_TYPESTORE_TO_TEST)
    def test_to_ros_default_type(self, point3d: Point3d, typestore: Typestore):
        ros_msg = PointAdapter.to_ros(point3d, typestore)

        assert_point3d(point3d, asdict(ros_msg))

    def test_to_ros_invalid_rosmsg_type(self, point3d: Point3d):
        ros_msg = PointAdapter.to_ros(
            point3d, get_typestore(Stores.LATEST), "geometry_msgs/msg/Bogus"
        )

        assert ros_msg is None

    def test_to_ros_invalid_mosaico_type(self, invalid_ms_msg):
        with pytest.raises(TypeError):
            PointAdapter.to_ros(invalid_ms_msg, get_typestore(Stores.LATEST))


###############################################################################
############################ TestQuaternionAdapter ############################
###############################################################################


@pytest.fixture
def quat_msg(quaternion):
    return Message(
        data=quaternion,
        timestamp_ns=100,
        frame_id="base_link",
    )


@pytest.fixture
def quaternion_rosmsg(quaternion: Quaternion):
    return ROSMessage(
        bag_timestamp_ns=100,
        topic="/quaternion",
        msg_type="geometry_msgs/msg/Quaternion",
        data=quaternion.model_dump(),
    )


@pytest.fixture
def quaternion_rosmsg_stamped(quaternion: Quaternion, ros_header):
    return ROSMessage(
        bag_timestamp_ns=100,
        topic="/quaternion",
        msg_type="geometry_msgs/msg/QuaternionStamped",
        data={"header": ros_header, "quaternion": quaternion.model_dump()},
    )


class TestQuaternionAdapter:
    def test_translate_quaternion(self, quaternion_rosmsg: ROSMessage):
        ms_msg = QuaternionAdapter.translate(quaternion_rosmsg)

        assert_quaternion(ms_msg.get_data(Quaternion), quaternion_rosmsg.data)
        assert ms_msg.timestamp_ns == quaternion_rosmsg.bag_timestamp_ns

    def test_translate_quaternion_stamped(self, quaternion_rosmsg_stamped: ROSMessage):
        ms_msg = QuaternionAdapter.translate(quaternion_rosmsg_stamped)

        assert (
            ms_msg.timestamp_ns
            == quaternion_rosmsg_stamped.header.stamp.to_nanoseconds()
        )
        assert ms_msg.frame_id == quaternion_rosmsg_stamped.header.frame_id
        assert_quaternion(
            ms_msg.get_data(Quaternion), quaternion_rosmsg_stamped.data["quaternion"]
        )

    def test_translate_raise_missing_required_key(self, quaternion: Quaternion):
        data = quaternion.model_dump()
        data.pop("w")
        with pytest.raises(ValueError):
            QuaternionAdapter.from_dict(data)

    @pytest.mark.parametrize("typestore", ROS_TYPESTORE_TO_TEST)
    def test_to_ros_quaternion(self, quaternion: Quaternion, typestore: Typestore):
        ros_msg = QuaternionAdapter.to_ros(
            quaternion, typestore, "geometry_msgs/msg/Quaternion"
        )

        assert_quaternion(quaternion, asdict(ros_msg))

    @pytest.mark.parametrize("typestore", ROS_TYPESTORE_TO_TEST)
    def test_to_ros_quaternion_stamped(self, quat_msg: Message, typestore: Typestore):
        quaternion = quat_msg.get_data(Quaternion)
        ros_msg = QuaternionAdapter.to_ros(
            quat_msg, typestore, "geometry_msgs/msg/QuaternionStamped"
        )

        assert (
            quat_msg.timestamp_ns
            == Time(
                seconds=ros_msg.header.stamp.sec,
                nanoseconds=ros_msg.header.stamp.nanosec,
            ).to_nanoseconds()
        )
        assert quat_msg.frame_id == ros_msg.header.frame_id
        assert_quaternion(quaternion, asdict(ros_msg.quaternion))

    @pytest.mark.parametrize("typestore", ROS_TYPESTORE_TO_TEST)
    def test_to_ros_default_type(self, quaternion: Quaternion, typestore: Typestore):
        ros_msg = QuaternionAdapter.to_ros(quaternion, typestore)

        assert_quaternion(quaternion, asdict(ros_msg))

    def test_to_ros_invalid_rosmsg_type(self, quaternion: Quaternion):
        ros_msg = QuaternionAdapter.to_ros(
            quaternion, get_typestore(Stores.LATEST), "geometry_msgs/msg/Bogus"
        )

        assert ros_msg is None

    def test_to_ros_invalid_mosaico_type(self, invalid_ms_msg):
        with pytest.raises(TypeError):
            QuaternionAdapter.to_ros(invalid_ms_msg, get_typestore(Stores.LATEST))


###############################################################################
############################ TestTransformAdapter ############################
###############################################################################


@pytest.fixture
def transform():
    return Transform(
        translation=Vector3d(x=1.0, y=2.0, z=3.0),
        rotation=Quaternion(x=0.0, y=0.0, z=0.0, w=1.0),
        source_frame_id="frame1",
        target_frame_id="frame2",
    )


@pytest.fixture
def transform_msg(transform):
    return Message(
        data=transform,
        timestamp_ns=100,
        frame_id="base_link",
    )


@pytest.fixture
def transform_rosmsg(transform: Transform):
    return ROSMessage(
        bag_timestamp_ns=100,
        topic="/transform",
        msg_type="geometry_msgs/msg/Transform",
        data=transform.model_dump(),
    )


@pytest.fixture
def transform_rosmsg_stamped(transform: Transform, ros_header):
    return ROSMessage(
        bag_timestamp_ns=100,
        topic="/transform",
        msg_type="geometry_msgs/msg/TransformStamped",
        data={
            "header": ros_header,
            "frame_id": "base_link",
            "transform": transform.model_dump(),
        },
    )


class TestTransformAdapter:
    def test_translate_transform(self, transform_rosmsg):
        ms_msg = TransformAdapter.translate(transform_rosmsg)

        assert_transform(ms_msg.get_data(Transform), transform_rosmsg.data)
        assert ms_msg.timestamp_ns == transform_rosmsg.bag_timestamp_ns

    def test_translate_transform_stamped(self, transform_rosmsg_stamped):
        ms_msg = TransformAdapter.translate(transform_rosmsg_stamped)

        assert_transform(
            ms_msg.get_data(Transform), transform_rosmsg_stamped.data["transform"]
        )
        assert (
            ms_msg.timestamp_ns
            == transform_rosmsg_stamped.header.stamp.to_nanoseconds()
        )
        assert ms_msg.frame_id == transform_rosmsg_stamped.header.frame_id

    def test_translate_raise_missing_required_key(self, transform):
        data = transform.model_dump()
        data.pop("translation")
        with pytest.raises(ValueError):
            QuaternionAdapter.from_dict(data)

    @pytest.mark.parametrize("typestore", ROS_TYPESTORE_TO_TEST)
    def test_to_ros_transform(self, transform: Transform, typestore: Typestore):
        ros_msg = TransformAdapter.to_ros(
            transform, typestore, "geometry_msgs/msg/Transform"
        )

        assert_transform(transform, asdict(ros_msg))

    @pytest.mark.parametrize("typestore", ROS_TYPESTORE_TO_TEST)
    def test_to_ros_transform_stamped(
        self, transform_msg: Message, typestore: Typestore
    ):
        transform = transform_msg.get_data(Transform)
        ros_msg = TransformAdapter.to_ros(
            transform_msg, typestore, "geometry_msgs/msg/TransformStamped"
        )

        assert (
            transform_msg.timestamp_ns
            == Time(
                seconds=ros_msg.header.stamp.sec,
                nanoseconds=ros_msg.header.stamp.nanosec,
            ).to_nanoseconds()
        )
        assert transform_msg.frame_id == ros_msg.header.frame_id
        assert transform.target_frame_id == ros_msg.child_frame_id
        assert_transform(transform, asdict(ros_msg.transform))

    @pytest.mark.parametrize("typestore", ROS_TYPESTORE_TO_TEST)
    def test_to_ros_default_type(self, transform: Transform, typestore: Typestore):
        ros_msg = TransformAdapter.to_ros(transform, typestore)

        assert_transform(transform, asdict(ros_msg))

    def test_to_ros_invalid_rosmsg_type(self, transform: Transform):
        ros_msg = TransformAdapter.to_ros(
            transform, get_typestore(Stores.LATEST), "geometry_msgs/msg/Bogus"
        )

        assert ros_msg is None

    def test_to_ros_invalid_mosaico_type(self, invalid_ms_msg):
        with pytest.raises(TypeError):
            TransformAdapter.to_ros(invalid_ms_msg, get_typestore(Stores.LATEST))


###############################################################################
############################### TestWrenchAdapter #############################
###############################################################################


@pytest.fixture
def force_torque():
    return ForceTorque(
        force=Vector3d(x=1.0, y=2.0, z=3.0), torque=Vector3d(x=1.0, y=2.0, z=3.0)
    )


@pytest.fixture
def force_torque_msg(force_torque):
    return Message(
        data=force_torque,
        timestamp_ns=100,
        frame_id="base_link",
    )


@pytest.fixture
def force_torque_ros_msg(force_torque: ForceTorque):
    return ROSMessage(
        bag_timestamp_ns=100,
        topic="/wrench",
        msg_type="geometry_msgs/msg/Wrench",
        data=force_torque.model_dump(),
    )


@pytest.fixture
def force_torque_ros_msg_stamped(force_torque: ForceTorque, ros_header):
    return ROSMessage(
        bag_timestamp_ns=100,
        topic="/wrench",
        msg_type="geometry_msgs/msg/WrenchStamped",
        data={"header": ros_header, "wrench": force_torque.model_dump()},
    )


class TestWrenchAdapter:
    def test_translate_wrench(self, force_torque_ros_msg: ROSMessage):
        ms_msg = WrenchAdapter.translate(force_torque_ros_msg)

        assert_force_torque(ms_msg.get_data(ForceTorque), force_torque_ros_msg.data)
        assert ms_msg.timestamp_ns == force_torque_ros_msg.bag_timestamp_ns

    def test_translate_wrench_stamped(self, force_torque_ros_msg_stamped: ROSMessage):
        ms_msg = WrenchAdapter.translate(force_torque_ros_msg_stamped)

        assert (
            ms_msg.timestamp_ns
            == force_torque_ros_msg_stamped.header.stamp.to_nanoseconds()
        )
        assert ms_msg.frame_id == force_torque_ros_msg_stamped.header.frame_id
        assert_force_torque(
            ms_msg.get_data(ForceTorque), force_torque_ros_msg_stamped.data["wrench"]
        )

    def test_translate_raise_wrench_not_dict(self):
        with pytest.raises(ValueError):
            WrenchAdapter.from_dict({"wrench": "not_a_dict"})

    def test_translate_raise_missing_required_key(self, force_torque: ForceTorque):
        data = force_torque.model_dump()
        data.pop("torque")
        with pytest.raises(ValueError):
            WrenchAdapter.from_dict(data)

    @pytest.mark.parametrize("typestore", ROS_TYPESTORE_TO_TEST)
    def test_to_ros_wrench(self, force_torque: ForceTorque, typestore: Typestore):
        ros_msg = WrenchAdapter.to_ros(
            force_torque, typestore, "geometry_msgs/msg/Wrench"
        )

        assert_force_torque(force_torque, asdict(ros_msg))

    @pytest.mark.parametrize("typestore", ROS_TYPESTORE_TO_TEST)
    def test_to_ros_wrench_stamped(
        self, force_torque_msg: Message, typestore: Typestore
    ):
        force_torque = force_torque_msg.get_data(ForceTorque)
        ros_msg = WrenchAdapter.to_ros(
            force_torque_msg, typestore, "geometry_msgs/msg/WrenchStamped"
        )

        assert (
            force_torque_msg.timestamp_ns
            == Time(
                seconds=ros_msg.header.stamp.sec,
                nanoseconds=ros_msg.header.stamp.nanosec,
            ).to_nanoseconds()
        )
        assert force_torque_msg.frame_id == ros_msg.header.frame_id
        assert_force_torque(force_torque, asdict(ros_msg.wrench))

    @pytest.mark.parametrize("typestore", ROS_TYPESTORE_TO_TEST)
    def test_to_ros_default_type(self, force_torque: ForceTorque, typestore: Typestore):
        ros_msg = WrenchAdapter.to_ros(force_torque, typestore)

        assert_force_torque(force_torque, asdict(ros_msg))

    def test_to_ros_invalid_rosmsg_type(self, force_torque: ForceTorque):
        ros_msg = WrenchAdapter.to_ros(
            force_torque, get_typestore(Stores.LATEST), "geometry_msgs/msg/Bogus"
        )

        assert ros_msg is None

    def test_to_ros_invalid_mosaico_type(self, invalid_ms_msg):
        with pytest.raises(TypeError):
            WrenchAdapter.to_ros(invalid_ms_msg, get_typestore(Stores.LATEST))


###############################################################################
############################## TestPolygonAdapter #############################
###############################################################################


@pytest.fixture
def polygon():
    return Polygon(
        points=[
            Point3d(x=1.0, y=2.0, z=3.0),
            Point3d(x=4.0, y=5.0, z=6.0),
            Point3d(x=7.0, y=8.0, z=9.0),
        ]
    )


@pytest.fixture
def polygon_msg(polygon):
    return Message(
        data=polygon,
        timestamp_ns=100,
        frame_id="base_link",
    )


@pytest.fixture
def polygon_ros_msg(polygon: Polygon):
    return ROSMessage(
        bag_timestamp_ns=100,
        topic="/polygon",
        msg_type="geometry_msgs/msg/Polygon",
        data=polygon.model_dump(),
    )


@pytest.fixture
def polygon_ros_msg_stamped(polygon: Polygon, ros_header):
    return ROSMessage(
        bag_timestamp_ns=100,
        topic="/polygon",
        msg_type="geometry_msgs/msg/PolygonStamped",
        data={"header": ros_header, "polygon": polygon.model_dump()},
    )


class TestPolygonAdapter:
    def test_translate_polygon(self, polygon_ros_msg: ROSMessage):
        ms_msg = PolygonAdapter.translate(polygon_ros_msg)

        assert_polygon(ms_msg.get_data(Polygon), polygon_ros_msg.data)
        assert ms_msg.timestamp_ns == polygon_ros_msg.bag_timestamp_ns

    def test_translate_polygon_stamped(self, polygon_ros_msg_stamped: ROSMessage):
        ms_msg = PolygonAdapter.translate(polygon_ros_msg_stamped)

        assert (
            ms_msg.timestamp_ns == polygon_ros_msg_stamped.header.stamp.to_nanoseconds()
        )
        assert ms_msg.frame_id == polygon_ros_msg_stamped.header.frame_id
        assert_polygon(
            ms_msg.get_data(Polygon), polygon_ros_msg_stamped.data["polygon"]
        )

    def test_translate_raise_polygon_not_dict(self):
        with pytest.raises(ValueError):
            PolygonAdapter.from_dict({"polygon": "not_a_dict"})

    def test_translate_raise_missing_required_key(self, polygon: Polygon):
        data = polygon.model_dump()
        data.pop("points")
        with pytest.raises(ValueError):
            PolygonAdapter.from_dict(data)

    @pytest.mark.parametrize("typestore", ROS_TYPESTORE_TO_TEST)
    def test_to_ros_polygon(self, polygon: Polygon, typestore: Typestore):
        ros_msg = PolygonAdapter.to_ros(polygon, typestore, "geometry_msgs/msg/Polygon")

        assert_polygon(polygon, asdict(ros_msg))

    @pytest.mark.parametrize("typestore", ROS_TYPESTORE_TO_TEST)
    def test_to_ros_polygon_stamped(self, polygon_msg: Message, typestore: Typestore):
        polygon = polygon_msg.get_data(Polygon)
        ros_msg = PolygonAdapter.to_ros(
            polygon_msg, typestore, "geometry_msgs/msg/PolygonStamped"
        )

        assert (
            polygon_msg.timestamp_ns
            == Time(
                seconds=ros_msg.header.stamp.sec,
                nanoseconds=ros_msg.header.stamp.nanosec,
            ).to_nanoseconds()
        )
        assert polygon_msg.frame_id == ros_msg.header.frame_id
        assert_polygon(polygon, asdict(ros_msg.polygon))

    @pytest.mark.parametrize("typestore", ROS_TYPESTORE_TO_TEST)
    def test_to_ros_default_type(self, polygon: Polygon, typestore: Typestore):
        ros_msg = PolygonAdapter.to_ros(polygon, typestore)

        assert_polygon(polygon, asdict(ros_msg))

    def test_to_ros_invalid_rosmsg_type(self, polygon: Polygon):
        ros_msg = PolygonAdapter.to_ros(
            polygon, get_typestore(Stores.LATEST), "geometry_msgs/msg/Bogus"
        )

        assert ros_msg is None

    def test_to_ros_invalid_mosaico_type(self, invalid_ms_msg):
        with pytest.raises(TypeError):
            PolygonAdapter.to_ros(invalid_ms_msg, get_typestore(Stores.LATEST))


###############################################################################
############################## TestInertiaAdapter #############################
###############################################################################


@pytest.fixture
def inertia():
    return Inertia(
        mass=1,
        center_of_mass=Vector3d(x=1.0, y=2.0, z=3.0),
        inertia=list(range(0, 6)),
    )


@pytest.fixture
def inertia_msg(inertia):
    return Message(
        data=inertia,
        timestamp_ns=100,
        frame_id="base_link",
    )


@pytest.fixture
def inertia_ros_msg(inertia: Inertia):
    return ROSMessage(
        bag_timestamp_ns=100,
        topic="/inertia",
        msg_type="geometry_msgs/msg/Inertia",
        data={
            "m": inertia.mass,
            "com": {
                "x": inertia.center_of_mass.x,
                "y": inertia.center_of_mass.y,
                "z": inertia.center_of_mass.z,
            },
            "ixx": inertia.inertia[0],
            "ixy": inertia.inertia[1],
            "ixz": inertia.inertia[2],
            "iyy": inertia.inertia[3],
            "iyz": inertia.inertia[4],
            "izz": inertia.inertia[5],
        },
    )


@pytest.fixture
def inertia_ros_msg_stamped(inertia: Inertia, ros_header):
    return ROSMessage(
        bag_timestamp_ns=100,
        topic="/inertia",
        msg_type="geometry_msgs/msg/InertiaStamped",
        data={
            "header": ros_header,
            "inertia": {
                "m": inertia.mass,
                "com": {
                    "x": inertia.center_of_mass.x,
                    "y": inertia.center_of_mass.y,
                    "z": inertia.center_of_mass.z,
                },
                "ixx": inertia.inertia[0],
                "ixy": inertia.inertia[1],
                "ixz": inertia.inertia[2],
                "iyy": inertia.inertia[3],
                "iyz": inertia.inertia[4],
                "izz": inertia.inertia[5],
            },
        },
    )


class TestInertiaAdapter:
    def test_translate_inertia(self, inertia_ros_msg: ROSMessage):
        ms_msg = InertiaAdapter.translate(inertia_ros_msg)

        assert_inertia(ms_msg.get_data(Inertia), inertia_ros_msg.data)
        assert ms_msg.timestamp_ns == inertia_ros_msg.bag_timestamp_ns

    def test_translate_inertia_stamped(self, inertia_ros_msg_stamped: ROSMessage):
        ms_msg = InertiaAdapter.translate(inertia_ros_msg_stamped)

        assert (
            ms_msg.timestamp_ns == inertia_ros_msg_stamped.header.stamp.to_nanoseconds()
        )
        assert ms_msg.frame_id == inertia_ros_msg_stamped.header.frame_id
        assert_inertia(
            ms_msg.get_data(Inertia), inertia_ros_msg_stamped.data["inertia"]
        )

    def test_translate_raise_inertia_not_dict(self):
        with pytest.raises(ValueError):
            InertiaAdapter.from_dict({"inertia": "not_a_dict"})

    def test_translate_raise_missing_required_key(self, inertia_ros_msg: ROSMessage):
        data = dict(inertia_ros_msg.data)
        data.pop("m")
        with pytest.raises(ValueError):
            InertiaAdapter.from_dict(data)

    @pytest.mark.parametrize("typestore", ROS_TYPESTORE_TO_TEST)
    def test_to_ros_inertia(self, inertia: Inertia, typestore: Typestore):
        ros_msg = InertiaAdapter.to_ros(inertia, typestore, "geometry_msgs/msg/Inertia")

        assert_inertia(inertia, asdict(ros_msg))

    @pytest.mark.parametrize("typestore", ROS_TYPESTORE_TO_TEST)
    def test_to_ros_inertia_stamped(self, inertia_msg: Message, typestore: Typestore):
        inertia = inertia_msg.get_data(Inertia)
        ros_msg = InertiaAdapter.to_ros(
            inertia_msg, typestore, "geometry_msgs/msg/InertiaStamped"
        )

        assert (
            inertia_msg.timestamp_ns
            == Time(
                seconds=ros_msg.header.stamp.sec,
                nanoseconds=ros_msg.header.stamp.nanosec,
            ).to_nanoseconds()
        )
        assert inertia_msg.frame_id == ros_msg.header.frame_id
        assert_inertia(inertia, asdict(ros_msg.inertia))

    @pytest.mark.parametrize("typestore", ROS_TYPESTORE_TO_TEST)
    def test_to_ros_default_type(self, inertia: Inertia, typestore: Typestore):
        ros_msg = InertiaAdapter.to_ros(inertia, typestore)

        assert_inertia(inertia, asdict(ros_msg))

    def test_to_ros_invalid_rosmsg_type(self, inertia: Inertia):
        ros_msg = InertiaAdapter.to_ros(
            inertia, get_typestore(Stores.LATEST), "geometry_msgs/msg/Bogus"
        )

        assert ros_msg is None

    def test_to_ros_invalid_mosaico_type(self, invalid_ms_msg):
        with pytest.raises(TypeError):
            InertiaAdapter.to_ros(invalid_ms_msg, get_typestore(Stores.LATEST))


###############################################################################
############################### TestPoseAdapter ###############################
###############################################################################


@pytest.fixture
def pose(point3d, quaternion) -> Pose:
    return Pose(
        position=point3d,
        orientation=quaternion,
    )


@pytest.fixture
def pose_msg(pose) -> Message:
    return Message(
        data=pose,
        timestamp_ns=100,
        frame_id="base_link",
    )


@pytest.fixture
def pose_w_cov(point3d, quaternion) -> Pose:
    return Pose(position=point3d, orientation=quaternion, covariance=list(range(0, 36)))


@pytest.fixture
def pose_w_cov_msg(pose_w_cov) -> Message:
    return Message(
        data=pose_w_cov,
        timestamp_ns=100,
        frame_id="base_link",
    )


@pytest.fixture
def pose_rosmsg(pose: Pose):
    return ROSMessage(
        bag_timestamp_ns=100,
        topic="/pose",
        msg_type="geometry_msgs/msg/Pose",
        data=pose.model_dump(),
    )


@pytest.fixture
def pose_w_cov_rosmsg(pose_w_cov: Pose):
    return ROSMessage(
        bag_timestamp_ns=100,
        topic="/pose",
        msg_type="geometry_msgs/msg/Pose",
        data=pose_w_cov.model_dump(),
    )


@pytest.fixture
def pose_rosmsg_stamped(pose: Pose, ros_header):
    return ROSMessage(
        bag_timestamp_ns=100,
        topic="/pose",
        msg_type="geometry_msgs/msg/PoseStamped",
        data={
            "header": ros_header,
            "frame_id": "base_link",
            "pose": pose.model_dump(),
        },
    )


class TestPoseAdapter:
    def test_translate_pose(self, pose_rosmsg):
        ms_msg = PoseAdapter.translate(pose_rosmsg)

        assert_pose(ms_msg.get_data(Pose), pose_rosmsg.data)
        assert pose_rosmsg.bag_timestamp_ns == ms_msg.recording_timestamp_ns

    # def test_translate_pose_stamped(self): ...  # TODO
    # def test_translate_pose_w_cov_stamped(self): ...  # TODO
    # def test_translate_raise_missing_required_key(self): ...  # TODO

    @pytest.mark.parametrize("typestore", ROS_TYPESTORE_TO_TEST)
    def test_to_ros_pose(self, pose: Pose, typestore: Typestore):
        ros_msg = PoseAdapter.to_ros(pose, typestore, "geometry_msgs/msg/Pose")

        assert_pose(pose, asdict(ros_msg))

    @pytest.mark.parametrize("typestore", ROS_TYPESTORE_TO_TEST)
    def test_to_ros_null_cov(self, pose: Pose, typestore: Typestore):
        ros_msg = PoseAdapter.to_ros(
            pose, typestore, "geometry_msgs/msg/PoseWithCovariance"
        )

        asset_pose_w_cov(pose, asdict(ros_msg))

    @pytest.mark.parametrize("typestore", ROS_TYPESTORE_TO_TEST)
    def test_to_ros_with_cov(self, pose_w_cov: Pose, typestore: Typestore):
        ros_msg = PoseAdapter.to_ros(
            pose_w_cov, typestore, "geometry_msgs/msg/PoseWithCovariance"
        )

        asset_pose_w_cov(pose_w_cov, asdict(ros_msg))

    @pytest.mark.parametrize("typestore", ROS_TYPESTORE_TO_TEST)
    def test_to_ros_pose_stamped(self, pose_msg: Message, typestore: Typestore):
        pose = pose_msg.get_data(Pose)
        ros_msg = PoseAdapter.to_ros(
            pose_msg, typestore, "geometry_msgs/msg/PoseStamped"
        )

        assert (
            pose_msg.timestamp_ns
            == Time(
                seconds=ros_msg.header.stamp.sec,
                nanoseconds=ros_msg.header.stamp.nanosec,
            ).to_nanoseconds()
        )
        assert pose_msg.frame_id == ros_msg.header.frame_id
        assert_pose(pose, asdict(ros_msg.pose))

    @pytest.mark.parametrize("typestore", ROS_TYPESTORE_TO_TEST)
    def test_to_ros_pose_stamped_w_cov(
        self, pose_w_cov_msg: Message, typestore: Typestore
    ):
        pose_w_cov = pose_w_cov_msg.get_data(Pose)
        ros_msg = PoseAdapter.to_ros(
            pose_w_cov_msg,
            typestore,
            "geometry_msgs/msg/PoseWithCovarianceStamped",
        )

        assert (
            pose_w_cov_msg.timestamp_ns
            == Time(
                seconds=ros_msg.header.stamp.sec,
                nanoseconds=ros_msg.header.stamp.nanosec,
            ).to_nanoseconds()
        )
        asset_pose_w_cov(pose_w_cov, asdict(ros_msg.pose))

    @pytest.mark.parametrize("typestore", ROS_TYPESTORE_TO_TEST)
    def test_to_ros_default_type(self, pose: Pose, typestore: Typestore):
        ros_msg = PoseAdapter.to_ros(pose, typestore)

        assert_pose(pose, asdict(ros_msg))

    def test_to_ros_invalid_rosmsg_type(self, pose: Pose):
        ros_msg = PoseAdapter.to_ros(
            pose, get_typestore(Stores.LATEST), "geometry_msgs/msg/Bogus"
        )

        assert ros_msg is None

    def test_to_ros_invalid_mosaico_type(self, invalid_ms_msg):

        with pytest.raises(TypeError):
            PoseAdapter.to_ros(invalid_ms_msg, get_typestore(Stores.LATEST))


###############################################################################
############################## TestTwistAdapter ###############################
###############################################################################


@pytest.fixture
def twist():
    return Velocity(
        linear=Vector3d(x=1.0, y=2.0, z=3.0),
        angular=Vector3d(x=0.1, y=0.2, z=0.3),
    )


@pytest.fixture
def twist_msg(twist):
    return Message(
        data=twist,
        timestamp_ns=100,
        frame_id="base_link",
    )


@pytest.fixture
def twist_w_cov():
    return Velocity(
        linear=Vector3d(x=1.0, y=2.0, z=3.0),
        angular=Vector3d(x=0.1, y=0.2, z=0.3),
        covariance=list(range(0, 36)),
    )


@pytest.fixture
def twist_w_cov_msg(twist_w_cov):
    return Message(
        data=twist_w_cov,
        timestamp_ns=100,
        frame_id="base_link",
    )


class TestTwistAdapter:
    # def test_translate_twist(self): ...  # TODO
    # def test_translate_twist_stamped(self): ...  # TODO
    # def test_translate_twist_with_covariance(self): ...  # TODO
    # def test_translate_twist_with_covariance_stamped(self): ...  # TODO
    # def test_translate_raise_missing_required_key(self): ...  # TODO

    @pytest.mark.parametrize("typestore", ROS_TYPESTORE_TO_TEST)
    def test_to_ros_twist(self, twist: Velocity, typestore: Typestore):
        ros_msg = TwistAdapter.to_ros(twist, typestore, "geometry_msgs/msg/Twist")

        assert twist.linear.x == ros_msg.linear.x
        assert twist.linear.y == ros_msg.linear.y
        assert twist.linear.z == ros_msg.linear.z
        assert twist.angular.x == ros_msg.angular.x
        assert twist.angular.y == ros_msg.angular.y
        assert twist.angular.z == ros_msg.angular.z

    @pytest.mark.parametrize("typestore", ROS_TYPESTORE_TO_TEST)
    def test_to_ros_null_cov(self, twist: Velocity, typestore: Typestore):
        ros_msg = TwistAdapter.to_ros(
            twist, typestore, "geometry_msgs/msg/TwistWithCovariance"
        )

        assert (ros_msg.covariance == 0).all()
        assert twist.linear.x == ros_msg.twist.linear.x
        assert twist.linear.y == ros_msg.twist.linear.y
        assert twist.linear.z == ros_msg.twist.linear.z
        assert twist.angular.x == ros_msg.twist.angular.x
        assert twist.angular.y == ros_msg.twist.angular.y
        assert twist.angular.z == ros_msg.twist.angular.z

    @pytest.mark.parametrize("typestore", ROS_TYPESTORE_TO_TEST)
    def test_to_ros_with_cov(self, twist_w_cov: Velocity, typestore: Typestore):
        ros_msg = TwistAdapter.to_ros(
            twist_w_cov, typestore, "geometry_msgs/msg/TwistWithCovariance"
        )

        assert np.array_equal(twist_w_cov.covariance, ros_msg.covariance)
        assert twist_w_cov.linear.x == ros_msg.twist.linear.x
        assert twist_w_cov.linear.y == ros_msg.twist.linear.y
        assert twist_w_cov.linear.z == ros_msg.twist.linear.z
        assert twist_w_cov.angular.x == ros_msg.twist.angular.x
        assert twist_w_cov.angular.y == ros_msg.twist.angular.y
        assert twist_w_cov.angular.z == ros_msg.twist.angular.z

    @pytest.mark.parametrize("typestore", ROS_TYPESTORE_TO_TEST)
    def test_to_ros_twist_stamped(self, twist_msg: Message, typestore: Typestore):
        twist = twist_msg.get_data(Velocity)
        ros_msg = TwistAdapter.to_ros(
            twist_msg, typestore, "geometry_msgs/msg/TwistStamped"
        )

        assert (
            twist_msg.timestamp_ns
            == Time(
                seconds=ros_msg.header.stamp.sec,
                nanoseconds=ros_msg.header.stamp.nanosec,
            ).to_nanoseconds()
        )
        assert twist_msg.frame_id == ros_msg.header.frame_id
        assert twist.linear.x == ros_msg.twist.linear.x
        assert twist.linear.y == ros_msg.twist.linear.y
        assert twist.linear.z == ros_msg.twist.linear.z
        assert twist.angular.x == ros_msg.twist.angular.x
        assert twist.angular.y == ros_msg.twist.angular.y
        assert twist.angular.z == ros_msg.twist.angular.z

    @pytest.mark.parametrize("typestore", ROS_TYPESTORE_TO_TEST)
    def test_to_ros_twist_stamped_w_cov(
        self, twist_w_cov_msg: Message, typestore: Typestore
    ):
        twist_w_cov = twist_w_cov_msg.get_data(Velocity)
        ros_msg = TwistAdapter.to_ros(
            twist_w_cov_msg, typestore, "geometry_msgs/msg/TwistWithCovarianceStamped"
        )

        assert (
            twist_w_cov_msg.timestamp_ns
            == Time(
                seconds=ros_msg.header.stamp.sec,
                nanoseconds=ros_msg.header.stamp.nanosec,
            ).to_nanoseconds()
        )
        assert twist_w_cov_msg.frame_id == ros_msg.header.frame_id
        assert np.array_equal(twist_w_cov.covariance, ros_msg.twist.covariance)
        assert twist_w_cov.linear.x == ros_msg.twist.twist.linear.x
        assert twist_w_cov.linear.y == ros_msg.twist.twist.linear.y
        assert twist_w_cov.linear.z == ros_msg.twist.twist.linear.z
        assert twist_w_cov.angular.x == ros_msg.twist.twist.angular.x
        assert twist_w_cov.angular.y == ros_msg.twist.twist.angular.y
        assert twist_w_cov.angular.z == ros_msg.twist.twist.angular.z

    @pytest.mark.parametrize("typestore", ROS_TYPESTORE_TO_TEST)
    def test_to_ros_default_type(self, twist: Velocity, typestore: Typestore):
        ros_msg = TwistAdapter.to_ros(twist, typestore)

        assert twist.linear.x == ros_msg.linear.x
        assert twist.linear.y == ros_msg.linear.y
        assert twist.linear.z == ros_msg.linear.z
        assert twist.angular.x == ros_msg.angular.x
        assert twist.angular.y == ros_msg.angular.y
        assert twist.angular.z == ros_msg.angular.z

    def test_to_ros_invalid_rosmsg_type(self, twist: Velocity):
        ros_msg = TwistAdapter.to_ros(
            twist, get_typestore(Stores.LATEST), "geometry_msgs/msg/Bogus"
        )

        assert ros_msg is None

    def test_to_ros_invalid_mosaico_type(self, invalid_ms_msg):
        with pytest.raises(TypeError):
            TwistAdapter.to_ros(invalid_ms_msg, get_typestore(Stores.LATEST))


###############################################################################
############################## TestAccelAdapter ###############################
###############################################################################


@pytest.fixture
def accel():
    return Acceleration(
        linear=Vector3d(x=1.0, y=2.0, z=3.0),
        angular=Vector3d(x=0.1, y=0.2, z=0.3),
    )


@pytest.fixture
def accel_msg(accel):
    return Message(
        data=accel,
        timestamp_ns=100,
        frame_id="base_link",
    )


@pytest.fixture
def accel_w_cov():
    return Acceleration(
        linear=Vector3d(x=1.0, y=2.0, z=3.0),
        angular=Vector3d(x=0.1, y=0.2, z=0.3),
        covariance=list(range(0, 36)),
    )


@pytest.fixture
def accel_w_cov_msg(accel_w_cov):
    return Message(
        data=accel_w_cov,
        timestamp_ns=100,
        frame_id="base_link",
    )


class TestAccelAdapter:
    # def test_translate_accel(self): ...  # TODO
    # def test_translate_accel_stamped(self): ...  # TODO
    # def test_translate_accel_with_covariance(self): ...  # TODO
    # def test_translate_accel_with_covariance_stamped(self): ...  # TODO
    # def test_translate_raise_missing_required_key(self): ...  # TODO

    @pytest.mark.parametrize("typestore", ROS_TYPESTORE_TO_TEST)
    def test_to_ros_accel(self, accel: Acceleration, typestore: Typestore):
        ros_msg = AccelAdapter.to_ros(accel, typestore, "geometry_msgs/msg/Accel")

        assert accel.linear.x == ros_msg.linear.x
        assert accel.linear.y == ros_msg.linear.y
        assert accel.linear.z == ros_msg.linear.z
        assert accel.angular.x == ros_msg.angular.x
        assert accel.angular.y == ros_msg.angular.y
        assert accel.angular.z == ros_msg.angular.z

    @pytest.mark.parametrize("typestore", ROS_TYPESTORE_TO_TEST)
    def test_to_ros_null_cov(self, accel: Acceleration, typestore: Typestore):
        ros_msg = AccelAdapter.to_ros(
            accel, typestore, "geometry_msgs/msg/AccelWithCovariance"
        )

        assert (ros_msg.covariance == 0).all()
        assert accel.linear.x == ros_msg.accel.linear.x
        assert accel.linear.y == ros_msg.accel.linear.y
        assert accel.linear.z == ros_msg.accel.linear.z
        assert accel.angular.x == ros_msg.accel.angular.x
        assert accel.angular.y == ros_msg.accel.angular.y
        assert accel.angular.z == ros_msg.accel.angular.z

    @pytest.mark.parametrize("typestore", ROS_TYPESTORE_TO_TEST)
    def test_to_ros_with_cov(self, accel_w_cov: Acceleration, typestore: Typestore):
        ros_msg = AccelAdapter.to_ros(
            accel_w_cov, typestore, "geometry_msgs/msg/AccelWithCovariance"
        )

        assert np.array_equal(accel_w_cov.covariance, ros_msg.covariance)
        assert accel_w_cov.linear.x == ros_msg.accel.linear.x
        assert accel_w_cov.linear.y == ros_msg.accel.linear.y
        assert accel_w_cov.linear.z == ros_msg.accel.linear.z
        assert accel_w_cov.angular.x == ros_msg.accel.angular.x
        assert accel_w_cov.angular.y == ros_msg.accel.angular.y
        assert accel_w_cov.angular.z == ros_msg.accel.angular.z

    @pytest.mark.parametrize("typestore", ROS_TYPESTORE_TO_TEST)
    def test_to_ros_accel_stamped(self, accel_msg: Message, typestore: Typestore):
        accel = accel_msg.get_data(Acceleration)
        ros_msg = AccelAdapter.to_ros(
            accel_msg, typestore, "geometry_msgs/msg/AccelStamped"
        )

        assert (
            accel_msg.timestamp_ns
            == Time(
                seconds=ros_msg.header.stamp.sec,
                nanoseconds=ros_msg.header.stamp.nanosec,
            ).to_nanoseconds()
        )
        assert accel_msg.frame_id == ros_msg.header.frame_id
        assert accel.linear.x == ros_msg.accel.linear.x
        assert accel.linear.y == ros_msg.accel.linear.y
        assert accel.linear.z == ros_msg.accel.linear.z
        assert accel.angular.x == ros_msg.accel.angular.x
        assert accel.angular.y == ros_msg.accel.angular.y
        assert accel.angular.z == ros_msg.accel.angular.z

    @pytest.mark.parametrize("typestore", ROS_TYPESTORE_TO_TEST)
    def test_to_ros_accel_stamped_w_cov(
        self, accel_w_cov_msg: Message, typestore: Typestore
    ):
        accel_w_cov = accel_w_cov_msg.get_data(Acceleration)
        ros_msg = AccelAdapter.to_ros(
            accel_w_cov_msg, typestore, "geometry_msgs/msg/AccelWithCovarianceStamped"
        )

        assert (
            accel_w_cov_msg.timestamp_ns
            == Time(
                seconds=ros_msg.header.stamp.sec,
                nanoseconds=ros_msg.header.stamp.nanosec,
            ).to_nanoseconds()
        )
        assert accel_w_cov_msg.frame_id == ros_msg.header.frame_id
        assert np.array_equal(accel_w_cov.covariance, ros_msg.accel.covariance)
        assert accel_w_cov.linear.x == ros_msg.accel.accel.linear.x
        assert accel_w_cov.linear.y == ros_msg.accel.accel.linear.y
        assert accel_w_cov.linear.z == ros_msg.accel.accel.linear.z
        assert accel_w_cov.angular.x == ros_msg.accel.accel.angular.x
        assert accel_w_cov.angular.y == ros_msg.accel.accel.angular.y
        assert accel_w_cov.angular.z == ros_msg.accel.accel.angular.z

    @pytest.mark.parametrize("typestore", ROS_TYPESTORE_TO_TEST)
    def test_to_ros_default_type(self, accel: Acceleration, typestore: Typestore):
        ros_msg = AccelAdapter.to_ros(accel, typestore)

        assert accel.linear.x == ros_msg.linear.x
        assert accel.linear.y == ros_msg.linear.y
        assert accel.linear.z == ros_msg.linear.z
        assert accel.angular.x == ros_msg.angular.x
        assert accel.angular.y == ros_msg.angular.y
        assert accel.angular.z == ros_msg.angular.z

    def test_to_ros_invalid_rosmsg_type(self, accel: Acceleration):
        ros_msg = AccelAdapter.to_ros(
            accel, get_typestore(Stores.LATEST), "geometry_msgs/msg/Bogus"
        )

        assert ros_msg is None

    def test_to_ros_invalid_mosaico_type(self, invalid_ms_msg):
        with pytest.raises(TypeError):
            AccelAdapter.to_ros(invalid_ms_msg, get_typestore(Stores.LATEST))
