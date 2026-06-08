from dataclasses import asdict

import pytest
from rosbags.typesys.stores import Stores, Typestore, get_typestore

from mosaicolabs import (
    Message,
    MotionState,
    Pose,
    RobotPath,
    Time,
    Velocity,
)
from mosaicolabs.models.futures import GridCells, MapMetadata, OccupancyGrid
from mosaicolabs.ros_bridge.adapters import (
    GridCellsAdapter,
    MapMetadataAdapter,
    OccupancyGridAdapter,
    OdometryAdapter,
    RobotPathAdapter,
)
from mosaicolabs.ros_bridge.ros_message import ROSMessage
from testing.unit.ros_bridge.adapters.helper import (
    assert_grid_cells,
    assert_map_metadata,
    assert_motion_state,
    assert_occupancy_grid,
    assert_path,
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
def pose(point3d, quaternion):
    return Pose(position=point3d, orientation=quaternion)


@pytest.fixture
def velocity(vector3d):
    return Velocity(linear=vector3d, angular=vector3d)


###############################################################################
############################# TestOdometryAdapter #############################
###############################################################################


@pytest.fixture
def motion_state(pose, velocity):
    return MotionState(
        pose=pose,
        velocity=velocity,
        target_frame_id="base_link",
    )


@pytest.fixture
def motion_state_rosmsg(ros_header, motion_state: MotionState):
    return ROSMessage(
        bag_timestamp_ns=100,
        topic="/odometry",
        msg_type="nav_msgs/msg/Odometry",
        data={
            "header": ros_header,
            "child_frame_id": motion_state.target_frame_id,
            "pose": {
                "pose": motion_state.pose.model_dump(
                    exclude={"covariance", "covariance_type"}
                ),
                "covariance": [0.0] * 36,
            },
            "twist": {
                "twist": motion_state.velocity.model_dump(
                    exclude={"covariance", "covariance_type"}
                ),
                "covariance": [0.0] * 36,
            },
        },
    )


@pytest.fixture
def motion_state_msg(motion_state):
    return Message(
        data=motion_state,
        timestamp_ns=100,
        frame_id="base_link",
    )


class TestOdometryAdapter:
    def test_translate_motion_state(self, motion_state_rosmsg: ROSMessage):
        ms_msg = OdometryAdapter.translate(motion_state_rosmsg)

        assert ms_msg.timestamp_ns == motion_state_rosmsg.header.stamp.to_nanoseconds()
        assert_motion_state(ms_msg.get_data(MotionState), motion_state_rosmsg.data)

    def test_translate_raise_missing_required_key(
        self, motion_state_rosmsg: ROSMessage
    ):
        data = motion_state_rosmsg.data
        data.pop("child_frame_id")
        with pytest.raises(ValueError, match="missing required keys"):
            OdometryAdapter.from_dict(data)

    @pytest.mark.parametrize("typestore", ROS_TYPESTORE_TO_TEST)
    def test_to_ros_motion_state(self, motion_state: MotionState, typestore: Typestore):
        ros_msg = OdometryAdapter.to_ros(
            motion_state, typestore, "nav_msgs/msg/Odometry"
        )

        assert_motion_state(motion_state, asdict(ros_msg))

    @pytest.mark.parametrize("typestore", ROS_TYPESTORE_TO_TEST)
    def test_to_ros_motion_state_message(
        self, motion_state_msg: Message, typestore: Typestore
    ):
        motion_state = motion_state_msg.get_data(MotionState)
        ros_msg = OdometryAdapter.to_ros(
            motion_state_msg, typestore, "nav_msgs/msg/Odometry"
        )

        assert (
            motion_state_msg.timestamp_ns
            == Time(
                seconds=ros_msg.header.stamp.sec,
                nanoseconds=ros_msg.header.stamp.nanosec,
            ).to_nanoseconds()
        )
        assert motion_state_msg.frame_id == ros_msg.header.frame_id
        assert_motion_state(motion_state, asdict(ros_msg))

    @pytest.mark.parametrize("typestore", ROS_TYPESTORE_TO_TEST)
    def test_to_ros_default_type(self, motion_state: MotionState, typestore: Typestore):
        ros_msg = OdometryAdapter.to_ros(motion_state, typestore)

        assert_motion_state(motion_state, asdict(ros_msg))

    def test_to_ros_invalid_rosmsg_type(self, motion_state: MotionState):
        ros_msg = OdometryAdapter.to_ros(
            motion_state, get_typestore(Stores.LATEST), "nav_msgs/msg/Bogus"
        )

        assert ros_msg is None

    def test_to_ros_invalid_mosaico_type(self, invalid_ms_msg):
        with pytest.raises(TypeError):
            OdometryAdapter.to_ros(invalid_ms_msg, get_typestore(Stores.LATEST))


###############################################################################
############################ TestRobotPathAdapter #############################
###############################################################################


@pytest.fixture
def robot_path(pose):
    return RobotPath(
        path_frame="base_link",
        poses=[pose, pose, pose],
    )


@pytest.fixture
def path_rosmsg(ros_header, robot_path: RobotPath):
    return ROSMessage(
        bag_timestamp_ns=100,
        topic="/path",
        msg_type="nav_msgs/msg/Path",
        data={
            "header": ros_header,
            "poses": [
                {
                    "header": ros_header,
                    "pose": pose.model_dump(exclude={"covariance", "covariance_type"}),
                }
                for pose in robot_path.poses
            ],
        },
    )


@pytest.fixture
def path_msg(robot_path):
    return Message(
        data=robot_path,
        timestamp_ns=100,
        frame_id="base_link",
    )


class TestRobotPathAdapter:
    def test_translate_path(self, path_rosmsg: ROSMessage):
        ms_msg = RobotPathAdapter.translate(path_rosmsg)

        assert ms_msg.timestamp_ns == path_rosmsg.header.stamp.to_nanoseconds()
        assert_path(ms_msg.get_data(RobotPath), path_rosmsg.data)

    def test_translate_raise_missing_required_key(self, path_rosmsg: ROSMessage):
        data = path_rosmsg.data
        data.pop("poses")
        with pytest.raises(ValueError, match="missing required keys"):
            RobotPathAdapter.from_dict(data)

    @pytest.mark.parametrize("typestore", ROS_TYPESTORE_TO_TEST)
    def test_to_ros_path(self, robot_path: RobotPath, typestore: Typestore):
        ros_msg = RobotPathAdapter.to_ros(robot_path, typestore, "nav_msgs/msg/Path")
        assert_path(robot_path, asdict(ros_msg))

    @pytest.mark.parametrize("typestore", ROS_TYPESTORE_TO_TEST)
    def test_to_ros_path_message(self, path_msg: Message, typestore: Typestore):
        path = path_msg.get_data(RobotPath)
        ros_msg = RobotPathAdapter.to_ros(path_msg, typestore, "nav_msgs/msg/Path")

        assert (
            path_msg.timestamp_ns
            == Time(
                seconds=ros_msg.header.stamp.sec,
                nanoseconds=ros_msg.header.stamp.nanosec,
            ).to_nanoseconds()
        )
        assert path_msg.frame_id == ros_msg.header.frame_id
        assert_path(path, asdict(ros_msg))

    @pytest.mark.parametrize("typestore", ROS_TYPESTORE_TO_TEST)
    def test_to_ros_default_type(self, robot_path: RobotPath, typestore: Typestore):
        ros_msg = RobotPathAdapter.to_ros(robot_path, typestore)

        assert_path(robot_path, asdict(ros_msg))

    def test_to_ros_invalid_rosmsg_type(self, robot_path: RobotPath):
        ros_msg = RobotPathAdapter.to_ros(
            robot_path, get_typestore(Stores.LATEST), "nav_msgs/msg/Bogus"
        )

        assert ros_msg is None

    def test_to_ros_invalid_mosaico_type(self, invalid_ms_msg):
        with pytest.raises(TypeError):
            RobotPathAdapter.to_ros(invalid_ms_msg, get_typestore(Stores.LATEST))


###############################################################################
############################ TestGridCellsAdapter #############################
###############################################################################


@pytest.fixture
def grid_cells(point3d):
    return GridCells(
        cell_width=1.0,
        cell_height=2.0,
        cells=[point3d, point3d],
    )


@pytest.fixture
def grid_cells_msg(grid_cells):
    return Message(
        data=grid_cells,
        timestamp_ns=100,
        frame_id="base_link",
    )


@pytest.fixture
def grid_cells_rosmsg(ros_header, grid_cells: GridCells):
    return ROSMessage(
        bag_timestamp_ns=100,
        topic="/grid_cells",
        msg_type="nav_msgs/msg/GridCells",
        data={
            "header": ros_header,
            "cell_width": grid_cells.cell_width,
            "cell_height": grid_cells.cell_height,
            "cells": [
                {"x": cell.x, "y": cell.y, "z": cell.z} for cell in grid_cells.cells
            ],
        },
    )


class TestGridCellsAdapter:
    def test_translate_grid_cells(self, grid_cells_rosmsg: ROSMessage):
        gc_msg = GridCellsAdapter.translate(grid_cells_rosmsg)

        assert gc_msg.timestamp_ns == grid_cells_rosmsg.header.stamp.to_nanoseconds()
        assert_grid_cells(gc_msg.get_data(GridCells), grid_cells_rosmsg.data)

    def test_translate_raise_missing_required_key(self, grid_cells_rosmsg: ROSMessage):
        data = grid_cells_rosmsg.data
        data.pop("cell_width")
        with pytest.raises(ValueError, match="missing required keys"):
            GridCellsAdapter.from_dict(data)

    @pytest.mark.parametrize("typestore", ROS_TYPESTORE_TO_TEST)
    def test_to_ros_grid_cells(self, grid_cells: GridCells, typestore: Typestore):
        ros_msg = GridCellsAdapter.to_ros(
            grid_cells, typestore, "nav_msgs/msg/GridCells"
        )
        assert_grid_cells(grid_cells, asdict(ros_msg))

    @pytest.mark.parametrize("typestore", ROS_TYPESTORE_TO_TEST)
    def test_to_ros_grid_cells_message(
        self, grid_cells_msg: Message, typestore: Typestore
    ):
        grid_cells = grid_cells_msg.get_data(GridCells)
        ros_msg = GridCellsAdapter.to_ros(
            grid_cells_msg, typestore, "nav_msgs/msg/GridCells"
        )

        assert (
            grid_cells_msg.timestamp_ns
            == Time(
                seconds=ros_msg.header.stamp.sec,
                nanoseconds=ros_msg.header.stamp.nanosec,
            ).to_nanoseconds()
        )
        assert grid_cells_msg.frame_id == ros_msg.header.frame_id
        assert_grid_cells(grid_cells, asdict(ros_msg))

    @pytest.mark.parametrize("typestore", ROS_TYPESTORE_TO_TEST)
    def test_to_ros_default_type(self, grid_cells: GridCells, typestore: Typestore):
        ros_msg = GridCellsAdapter.to_ros(grid_cells, typestore)

        assert_grid_cells(grid_cells, asdict(ros_msg))

    def test_to_ros_invalid_rosmsg_type(self, grid_cells: GridCells):
        ros_msg = GridCellsAdapter.to_ros(
            grid_cells, get_typestore(Stores.LATEST), "nav_msgs/msg/Bogus"
        )

        assert ros_msg is None

    def test_to_ros_invalid_mosaico_type(self, invalid_ms_msg):
        with pytest.raises(TypeError):
            GridCellsAdapter.to_ros(invalid_ms_msg, get_typestore(Stores.LATEST))


###############################################################################
########################### TestMapMetadataAdapter ############################
###############################################################################


@pytest.fixture
def map_metadata(pose):
    return MapMetadata(
        map_load_time=Time(seconds=100000, nanoseconds=1000).to_nanoseconds(),
        resolution=0.05,
        width=100,
        height=100,
        origin=pose,
    )


@pytest.fixture
def map_metadata_msg(map_metadata):
    return Message(
        data=map_metadata,
        timestamp_ns=100,
        frame_id="map",
    )


@pytest.fixture
def map_metadata_rosmsg(pose: Pose):
    return ROSMessage(
        bag_timestamp_ns=100,
        topic="/map_metadata",
        msg_type="nav_msgs/msg/MapMetaData",
        data={
            "map_load_time": {"sec": 100000, "nanosec": 1000},
            "resolution": 0.05,
            "width": 100,
            "height": 100,
            "origin": {
                "position": {
                    "x": pose.position.x,
                    "y": pose.position.y,
                    "z": pose.position.z,
                },
                "orientation": {
                    "x": pose.orientation.x,
                    "y": pose.orientation.y,
                    "z": pose.orientation.z,
                    "w": pose.orientation.w,
                },
            },
        },
    )


class TestMapMetadataAdapter:
    def test_translate_map_metadata(self, map_metadata_rosmsg: ROSMessage):
        mm_msg = MapMetadataAdapter.translate(map_metadata_rosmsg)

        # nav_msgs/msg/MapMetaData has no header; timestamp falls back to bag_timestamp_ns
        assert mm_msg.timestamp_ns == map_metadata_rosmsg.bag_timestamp_ns
        assert_map_metadata(mm_msg.get_data(MapMetadata), map_metadata_rosmsg.data)

    def test_translate_raise_missing_required_key(
        self, map_metadata_rosmsg: ROSMessage
    ):
        data = map_metadata_rosmsg.data
        data.pop("map_load_time")
        with pytest.raises(ValueError, match="missing required keys"):
            MapMetadataAdapter.from_dict(data)

    @pytest.mark.parametrize("typestore", ROS_TYPESTORE_TO_TEST)
    def test_to_ros_map_metadata(self, map_metadata: MapMetadata, typestore: Typestore):
        ros_msg = MapMetadataAdapter.to_ros(
            map_metadata, typestore, "nav_msgs/msg/MapMetaData"
        )
        assert_map_metadata(map_metadata, asdict(ros_msg))

    @pytest.mark.parametrize("typestore", ROS_TYPESTORE_TO_TEST)
    def test_to_ros_map_metadata_message(
        self, map_metadata_msg: Message, typestore: Typestore
    ):
        map_metadata = map_metadata_msg.get_data(MapMetadata)
        ros_msg = MapMetadataAdapter.to_ros(
            map_metadata_msg, typestore, "nav_msgs/msg/MapMetaData"
        )
        # nav_msgs/msg/MapMetaData has no header; Message timestamp/frame_id are not propagated
        assert_map_metadata(map_metadata, asdict(ros_msg))

    @pytest.mark.parametrize("typestore", ROS_TYPESTORE_TO_TEST)
    def test_to_ros_default_type(self, map_metadata: MapMetadata, typestore: Typestore):
        ros_msg = MapMetadataAdapter.to_ros(map_metadata, typestore)
        assert_map_metadata(map_metadata, asdict(ros_msg))

    def test_to_ros_invalid_rosmsg_type(self, map_metadata: MapMetadata):
        ros_msg = MapMetadataAdapter.to_ros(
            map_metadata, get_typestore(Stores.LATEST), "nav_msgs/msg/Bogus"
        )
        assert ros_msg is None

    def test_to_ros_invalid_mosaico_type(self, invalid_ms_msg):
        with pytest.raises(TypeError):
            MapMetadataAdapter.to_ros(invalid_ms_msg, get_typestore(Stores.LATEST))


###############################################################################
########################### TestOccupancyGridAdapter ##########################
###############################################################################


@pytest.fixture
def occupancy_grid(map_metadata):
    return OccupancyGrid(
        info=map_metadata,
        data=[0, 1, -1, 50],
    )


@pytest.fixture
def occupancy_grid_msg(occupancy_grid):
    return Message(
        data=occupancy_grid,
        timestamp_ns=100,
        frame_id="map",
    )


@pytest.fixture
def occupancy_grid_rosmsg(ros_header, pose: Pose):
    return ROSMessage(
        bag_timestamp_ns=100,
        topic="/occupancy_grid",
        msg_type="nav_msgs/msg/OccupancyGrid",
        data={
            "header": ros_header,
            "info": {
                "map_load_time": {"sec": 100000, "nanosec": 1000},
                "resolution": 0.05,
                "width": 100,
                "height": 100,
                "origin": {
                    "position": {
                        "x": pose.position.x,
                        "y": pose.position.y,
                        "z": pose.position.z,
                    },
                    "orientation": {
                        "x": pose.orientation.x,
                        "y": pose.orientation.y,
                        "z": pose.orientation.z,
                        "w": pose.orientation.w,
                    },
                },
            },
            "data": [0, 1, -1, 50],
        },
    )


class TestOccupancyGridAdapter:
    def test_translate_occupancy_grid(self, occupancy_grid_rosmsg: ROSMessage):
        og_msg = OccupancyGridAdapter.translate(occupancy_grid_rosmsg)

        assert (
            og_msg.timestamp_ns == occupancy_grid_rosmsg.header.stamp.to_nanoseconds()
        )
        assert_occupancy_grid(
            og_msg.get_data(OccupancyGrid), occupancy_grid_rosmsg.data
        )

    def test_translate_raise_missing_required_key(
        self, occupancy_grid_rosmsg: ROSMessage
    ):
        data = occupancy_grid_rosmsg.data
        data.pop("info")
        with pytest.raises(ValueError, match="missing required keys"):
            OccupancyGridAdapter.from_dict(data)

    @pytest.mark.parametrize("typestore", ROS_TYPESTORE_TO_TEST)
    def test_to_ros_occupancy_grid(
        self, occupancy_grid: OccupancyGrid, typestore: Typestore
    ):
        ros_msg = OccupancyGridAdapter.to_ros(
            occupancy_grid, typestore, "nav_msgs/msg/OccupancyGrid"
        )
        assert_occupancy_grid(occupancy_grid, asdict(ros_msg))

    @pytest.mark.parametrize("typestore", ROS_TYPESTORE_TO_TEST)
    def test_to_ros_occupancy_grid_message(
        self, occupancy_grid_msg: Message, typestore: Typestore
    ):
        occupancy_grid = occupancy_grid_msg.get_data(OccupancyGrid)
        ros_msg = OccupancyGridAdapter.to_ros(
            occupancy_grid_msg, typestore, "nav_msgs/msg/OccupancyGrid"
        )
        assert (
            occupancy_grid_msg.timestamp_ns
            == Time(
                seconds=ros_msg.header.stamp.sec,
                nanoseconds=ros_msg.header.stamp.nanosec,
            ).to_nanoseconds()
        )
        assert occupancy_grid_msg.frame_id == ros_msg.header.frame_id
        assert_occupancy_grid(occupancy_grid, asdict(ros_msg))

    @pytest.mark.parametrize("typestore", ROS_TYPESTORE_TO_TEST)
    def test_to_ros_default_type(
        self, occupancy_grid: OccupancyGrid, typestore: Typestore
    ):
        ros_msg = OccupancyGridAdapter.to_ros(occupancy_grid, typestore)
        assert_occupancy_grid(occupancy_grid, asdict(ros_msg))

    def test_to_ros_invalid_rosmsg_type(self, occupancy_grid: OccupancyGrid):
        ros_msg = OccupancyGridAdapter.to_ros(
            occupancy_grid, get_typestore(Stores.LATEST), "nav_msgs/msg/Bogus"
        )
        assert ros_msg is None

    def test_to_ros_invalid_mosaico_type(self, invalid_ms_msg):
        with pytest.raises(TypeError):
            OccupancyGridAdapter.to_ros(invalid_ms_msg, get_typestore(Stores.LATEST))
