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
    Serializable,
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


@pytest.fixture
def invalid_ms_msg() -> Message:
    return Message(
        data=Serializable(),
        timestamp_ns=0,
    )


###############################################################################
############################# TestVector3Adapter ##############################
###############################################################################


@pytest.fixture
def vector3d():
    return Vector3d(x=1.0, y=2.0, z=3.0)


@pytest.fixture
def vector3d_msg(vector3d):
    return Message(
        data=vector3d,
        timestamp_ns=100,
        frame_id="base_link",
    )


class TestVectoradapter:
    # def test_translate_vector(self): ...  # TODO
    # def test_translate_vector_stamped(self): ...  # TODO
    # def test_translate_raise_vector_not_dict(self): ...  # TODO
    # def test_translate_raise_missing_required_key(self): ...  # TODO

    @pytest.mark.parametrize("typestore", ROS_TYPESTORE_TO_TEST)
    def test_to_ros_vector3(self, vector3d: Vector3d, typestore: Typestore):
        ros_msg = Vector3Adapter.to_ros(
            vector3d, typestore, "geometry_msgs/msg/Vector3"
        )

        assert vector3d.x == ros_msg.x
        assert vector3d.y == ros_msg.y
        assert vector3d.z == ros_msg.z

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
        assert vector3d.x == ros_msg.vector.x
        assert vector3d.y == ros_msg.vector.y
        assert vector3d.z == ros_msg.vector.z

    @pytest.mark.parametrize("typestore", ROS_TYPESTORE_TO_TEST)
    def test_to_ros_default_type(self, vector3d: Vector3d, typestore: Typestore):
        ros_msg = Vector3Adapter.to_ros(vector3d, typestore)

        assert vector3d.x == ros_msg.x
        assert vector3d.y == ros_msg.y
        assert vector3d.z == ros_msg.z

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
def point3d():
    return Point3d(x=1.0, y=2.0, z=3.0)


@pytest.fixture
def point3d_msg(point3d):
    return Message(
        data=point3d,
        timestamp_ns=100,
        frame_id="base_link",
    )


class TestPointadapter:
    # def test_translate_point(self): ...  # TODO
    # def test_translate_point_stamped(self): ...  # TODO
    # def test_translate_raise_point_not_dict(self): ...  # TODO
    # def test_translate_raise_missing_required_key(self): ...  # TODO

    @pytest.mark.parametrize("typestore", ROS_TYPESTORE_TO_TEST)
    def test_to_ros_point(self, point3d: Point3d, typestore: Typestore):
        ros_msg = PointAdapter.to_ros(point3d, typestore, "geometry_msgs/msg/Point")

        assert point3d.x == ros_msg.x
        assert point3d.y == ros_msg.y
        assert point3d.z == ros_msg.z

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
        assert point3d.x == ros_msg.point.x
        assert point3d.y == ros_msg.point.y
        assert point3d.z == ros_msg.point.z

    @pytest.mark.parametrize("typestore", ROS_TYPESTORE_TO_TEST)
    def test_to_ros_default_type(self, point3d: Point3d, typestore: Typestore):
        ros_msg = PointAdapter.to_ros(point3d, typestore)

        assert point3d.x == ros_msg.x
        assert point3d.y == ros_msg.y
        assert point3d.z == ros_msg.z

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
def quaternion():
    return Quaternion(x=0.0, y=0.0, z=0.0, w=1.0)


@pytest.fixture
def quat_msg(quaternion):
    return Message(
        data=quaternion,
        timestamp_ns=100,
        frame_id="base_link",
    )


class TestQuaternionAdapter:
    # def test_translate_quaternion(self): ...  # TODO
    # def test_translate_quaternion_stamped(self): ...  # TODO
    # def test_translate_raise_quaternion_not_dict(self): ...  # TODO
    # def test_translate_raise_missing_required_key(self): ...  # TODO

    @pytest.mark.parametrize("typestore", ROS_TYPESTORE_TO_TEST)
    def test_to_ros_quaternion(self, quaternion: Quaternion, typestore: Typestore):
        ros_msg = QuaternionAdapter.to_ros(
            quaternion, typestore, "geometry_msgs/msg/Quaternion"
        )

        assert quaternion.x == ros_msg.x
        assert quaternion.y == ros_msg.y
        assert quaternion.z == ros_msg.z
        assert quaternion.w == ros_msg.w

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
        assert quaternion.x == ros_msg.quaternion.x
        assert quaternion.y == ros_msg.quaternion.y
        assert quaternion.z == ros_msg.quaternion.z
        assert quaternion.w == ros_msg.quaternion.w

    @pytest.mark.parametrize("typestore", ROS_TYPESTORE_TO_TEST)
    def test_to_ros_default_type(self, quaternion: Quaternion, typestore: Typestore):
        ros_msg = QuaternionAdapter.to_ros(quaternion, typestore)

        assert quaternion.x == ros_msg.x
        assert quaternion.y == ros_msg.y
        assert quaternion.z == ros_msg.z
        assert quaternion.w == ros_msg.w

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


class TestTransformAdapter:
    # def test_translate_transform(self): ...  # TODO
    # def test_translate_transform_stamped(self): ...  # TODO
    # def test_translate_raise_transform_not_dict(self): ...  # TODO
    # def test_translate_raise_missing_required_key(self): ...  # TODO

    @pytest.mark.parametrize("typestore", ROS_TYPESTORE_TO_TEST)
    def test_to_ros_transform(self, transform: Transform, typestore: Typestore):
        ros_msg = TransformAdapter.to_ros(
            transform, typestore, "geometry_msgs/msg/Transform"
        )

        assert transform.translation.x == ros_msg.translation.x
        assert transform.translation.y == ros_msg.translation.y
        assert transform.translation.z == ros_msg.translation.z
        assert transform.rotation.x == ros_msg.rotation.x
        assert transform.rotation.y == ros_msg.rotation.y
        assert transform.rotation.z == ros_msg.rotation.z
        assert transform.rotation.w == ros_msg.rotation.w

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
        assert transform.translation.x == ros_msg.transform.translation.x
        assert transform.translation.y == ros_msg.transform.translation.y
        assert transform.translation.z == ros_msg.transform.translation.z
        assert transform.rotation.z == ros_msg.transform.rotation.z
        assert transform.rotation.z == ros_msg.transform.rotation.z
        assert transform.rotation.z == ros_msg.transform.rotation.z
        assert transform.rotation.z == ros_msg.transform.rotation.z

    @pytest.mark.parametrize("typestore", ROS_TYPESTORE_TO_TEST)
    def test_to_ros_default_type(self, transform: Transform, typestore: Typestore):
        ros_msg = TransformAdapter.to_ros(transform, typestore)

        assert transform.translation.x == ros_msg.translation.x
        assert transform.translation.y == ros_msg.translation.y
        assert transform.translation.z == ros_msg.translation.z
        assert transform.rotation.z == ros_msg.rotation.z
        assert transform.rotation.z == ros_msg.rotation.z
        assert transform.rotation.z == ros_msg.rotation.z
        assert transform.rotation.z == ros_msg.rotation.z

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


class TestWrenchAdapter:
    # def test_translate_wrench(self): ...  # TODO
    # def test_translate_wrench_stamped(self): ...  # TODO
    # def test_translate_raise_wrench_not_dict(self): ...  # TODO
    # def test_translate_raise_missing_required_key(self): ...  # TODO

    @pytest.mark.parametrize("typestore", ROS_TYPESTORE_TO_TEST)
    def test_to_ros_wrench(self, force_torque: ForceTorque, typestore: Typestore):
        ros_msg = WrenchAdapter.to_ros(
            force_torque, typestore, "geometry_msgs/msg/Wrench"
        )

        assert force_torque.force.x == ros_msg.force.x
        assert force_torque.force.y == ros_msg.force.y
        assert force_torque.force.z == ros_msg.force.z
        assert force_torque.torque.x == ros_msg.torque.x
        assert force_torque.torque.y == ros_msg.torque.y
        assert force_torque.torque.z == ros_msg.torque.z

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
        assert force_torque.force.x == ros_msg.wrench.force.x
        assert force_torque.force.y == ros_msg.wrench.force.y
        assert force_torque.force.z == ros_msg.wrench.force.z
        assert force_torque.torque.x == ros_msg.wrench.torque.x
        assert force_torque.torque.y == ros_msg.wrench.torque.y
        assert force_torque.torque.z == ros_msg.wrench.torque.z

    @pytest.mark.parametrize("typestore", ROS_TYPESTORE_TO_TEST)
    def test_to_ros_default_type(self, force_torque: ForceTorque, typestore: Typestore):
        ros_msg = WrenchAdapter.to_ros(force_torque, typestore)

        assert force_torque.force.x == ros_msg.force.x
        assert force_torque.force.y == ros_msg.force.y
        assert force_torque.force.z == ros_msg.force.z
        assert force_torque.torque.x == ros_msg.torque.x
        assert force_torque.torque.y == ros_msg.torque.y
        assert force_torque.torque.z == ros_msg.torque.z

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


class TestPolygonAdapter:
    # def test_translate_polygon(self): ...  # TODO
    # def test_translate_polygon_stamped(self): ...  # TODO
    # def test_translate_raise_polygon_not_dict(self): ...  # TODO
    # def test_translate_raise_missing_required_key(self): ...  # TODO

    @pytest.mark.parametrize("typestore", ROS_TYPESTORE_TO_TEST)
    def test_to_ros_polygon(self, polygon: Polygon, typestore: Typestore):
        ros_msg = PolygonAdapter.to_ros(polygon, typestore, "geometry_msgs/msg/Polygon")

        for point3d, ros_point in zip(polygon.points, ros_msg.points):
            assert point3d.x == ros_point.x
            assert point3d.y == ros_point.y
            assert point3d.z == ros_point.z

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
        for point3d, ros_point in zip(polygon.points, ros_msg.polygon.points):
            assert point3d.x == ros_point.x
            assert point3d.y == ros_point.y
            assert point3d.z == ros_point.z

    @pytest.mark.parametrize("typestore", ROS_TYPESTORE_TO_TEST)
    def test_to_ros_default_type(self, polygon: Polygon, typestore: Typestore):
        ros_msg = PolygonAdapter.to_ros(polygon, typestore)

        for point3d, ros_point in zip(polygon.points, ros_msg.points):
            assert point3d.x == ros_point.x
            assert point3d.y == ros_point.y
            assert point3d.z == ros_point.z

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


class TestInertiaAdapter:
    # def test_translate_inertia(self): ...  # TODO
    # def test_translate_inertia_stamped(self): ...  # TODO
    # def test_translate_raise_inertia_not_dict(self): ...  # TODO
    # def test_translate_raise_missing_required_key(self): ...  # TODO

    @pytest.mark.parametrize("typestore", ROS_TYPESTORE_TO_TEST)
    def test_to_ros_inertia(self, inertia: Inertia, typestore: Typestore):
        ros_msg = InertiaAdapter.to_ros(inertia, typestore, "geometry_msgs/msg/Inertia")

        assert inertia.mass == ros_msg.m
        assert inertia.center_of_mass.x == ros_msg.com.x
        assert inertia.center_of_mass.y == ros_msg.com.y
        assert inertia.center_of_mass.z == ros_msg.com.z
        assert inertia.inertia == [
            ros_msg.ixx,
            ros_msg.ixy,
            ros_msg.ixz,
            ros_msg.iyy,
            ros_msg.iyz,
            ros_msg.izz,
        ]

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
        assert inertia.mass == ros_msg.inertia.m
        assert inertia.center_of_mass.x == ros_msg.inertia.com.x
        assert inertia.center_of_mass.y == ros_msg.inertia.com.y
        assert inertia.center_of_mass.z == ros_msg.inertia.com.z
        assert inertia.inertia == [
            ros_msg.inertia.ixx,
            ros_msg.inertia.ixy,
            ros_msg.inertia.ixz,
            ros_msg.inertia.iyy,
            ros_msg.inertia.iyz,
            ros_msg.inertia.izz,
        ]

    @pytest.mark.parametrize("typestore", ROS_TYPESTORE_TO_TEST)
    def test_to_ros_default_type(self, inertia: Inertia, typestore: Typestore):
        ros_msg = InertiaAdapter.to_ros(inertia, typestore)

        assert inertia.mass == ros_msg.m
        assert inertia.center_of_mass.x == ros_msg.com.x
        assert inertia.center_of_mass.y == ros_msg.com.y
        assert inertia.center_of_mass.z == ros_msg.com.z
        assert inertia.inertia == [
            ros_msg.ixx,
            ros_msg.ixy,
            ros_msg.ixz,
            ros_msg.iyy,
            ros_msg.iyz,
            ros_msg.izz,
        ]

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


class TestPoseAdapter:
    def assert_pose(self, pose, ros_msg):
        assert pose.position.x == ros_msg.position.x
        assert pose.position.y == ros_msg.position.y
        assert pose.position.z == ros_msg.position.z
        assert pose.orientation.x == ros_msg.orientation.x
        assert pose.orientation.y == ros_msg.orientation.y
        assert pose.orientation.z == ros_msg.orientation.z
        assert pose.orientation.w == ros_msg.orientation.w

    def asset_pose_w_cov(self, pose_w_cov, ros_msg):

        self.assert_pose(pose_w_cov, ros_msg.pose)

        if pose_w_cov.covariance is None:
            assert (ros_msg.covariance == 0).all()
        else:
            assert np.array_equal(pose_w_cov.covariance, ros_msg.covariance)

    @pytest.mark.parametrize("typestore", ROS_TYPESTORE_TO_TEST)
    def test_to_ros_pose(self, pose: Pose, typestore: Typestore):
        ros_msg = PoseAdapter.to_ros(pose, typestore, "geometry_msgs/msg/Pose")

        self.assert_pose(pose, ros_msg)

    @pytest.mark.parametrize("typestore", ROS_TYPESTORE_TO_TEST)
    def test_to_ros_null_cov(self, pose: Pose, typestore: Typestore):
        ros_msg = PoseAdapter.to_ros(
            pose, typestore, "geometry_msgs/msg/PoseWithCovariance"
        )

        self.asset_pose_w_cov(pose, ros_msg)

    @pytest.mark.parametrize("typestore", ROS_TYPESTORE_TO_TEST)
    def test_to_ros_with_cov(self, pose_w_cov: Pose, typestore: Typestore):
        ros_msg = PoseAdapter.to_ros(
            pose_w_cov, typestore, "geometry_msgs/msg/PoseWithCovariance"
        )

        self.asset_pose_w_cov(pose_w_cov, ros_msg)

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
        self.assert_pose(pose, ros_msg.pose)

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
        self.asset_pose_w_cov(pose_w_cov, ros_msg.pose)

    @pytest.mark.parametrize("typestore", ROS_TYPESTORE_TO_TEST)
    def test_to_ros_default_type(self, pose: Pose, typestore: Typestore):
        ros_msg = PoseAdapter.to_ros(pose, typestore)

        self.assert_pose(pose, ros_msg)

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
    # def test_translate_raise_twist_not_dict(self): ...  # TODO
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
    # def test_translate_raise_accel_not_dict(self): ...  # TODO
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
