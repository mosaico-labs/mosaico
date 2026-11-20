from dataclasses import asdict

import pytest
from rosbags.typesys.store import Typestore
from rosbags.typesys.stores import Stores, get_typestore

from mosaicolabs import (
    MotionState,
    Pose,
    RobotPath,
    Time,
    Velocity,
)
from mosaicolabs.bridges.ros.adapters import (
    GridCellsAdapter,
    MapMetadataAdapter,
    OccupancyGridAdapter,
    OdometryAdapter,
    RobotPathAdapter,
)
from mosaicolabs.bridges.ros.ros_message import ROSMessage
from mosaicolabs.models.futures import GridCells, MapMetadata, OccupancyGrid
from testing.unit.bridges.ros.adapters.helper import (
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
def motion_state_rosmsg(ros_header):
    return ROSMessage(
        bag_timestamp_ns=100,
        topic="/odometry",
        msg_type="nav_msgs/msg/Odometry",
        data={
            "header": ros_header,
            "pose": {
                "pose": {
                    "position": {"x": 1.0, "y": 2.0, "z": 0.0},
                    "orientation": {"x": 0, "y": 0, "z": 0, "w": 1},
                },
                "covariance": [0.0] * 36,
            },
            "twist": {
                "twist": {
                    "linear": {"x": 0.0, "y": 0.0, "z": 0.0},
                    "angular": {"x": 0.0, "y": 0.0, "z": 0.0},
                },
                "covariance": [0.0] * 36,
            },
            "child_frame_id": "base_link",
        },
    )


@pytest.fixture
def motion_state_w_header(motion_state, ms_header):
    motion_state.header = ms_header
    return motion_state


class TestOdometryAdapter:
    def test_translate_motion_state(self, motion_state_rosmsg: ROSMessage):
        ms_msg = OdometryAdapter.translate(motion_state_rosmsg)

        assert_motion_state(
            ms_msg.get_data(MotionState), motion_state_rosmsg.data_field
        )

    def test_translate_raise_missing_required_key(
        self, motion_state_rosmsg: ROSMessage
    ):
        data = motion_state_rosmsg.data_field
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
    def test_to_ros_motion_state_w_header(
        self, motion_state: MotionState, typestore: Typestore
    ):
        ros_msg = OdometryAdapter.to_ros(
            motion_state, typestore, "nav_msgs/msg/Odometry"
        )

        assert_motion_state(motion_state, asdict(ros_msg))

    @pytest.mark.parametrize("typestore", ROS_TYPESTORE_TO_TEST)
    def test_to_ros_default_type(self, motion_state: MotionState, typestore: Typestore):
        ros_msg = OdometryAdapter.to_ros(motion_state, typestore)

        assert_motion_state(motion_state, asdict(ros_msg))

    def test_to_ros_invalid_rosmsg_type(self, motion_state: MotionState):

        with pytest.raises(
            TypeError,
            match=f"Adapter {OdometryAdapter.__name__} does not support nav_msgs/msg/Bogus",
        ):
            OdometryAdapter.to_ros(
                motion_state, get_typestore(Stores.LATEST), "nav_msgs/msg/Bogus"
            )

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
                    "pose": {
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
                }
                for pose in robot_path.poses
            ],
        },
    )


@pytest.fixture
def path_w_header(robot_path, ms_header):
    robot_path.header = ms_header
    return robot_path


class TestRobotPathAdapter:
    def test_translate_path(self, path_rosmsg: ROSMessage):
        ms_msg = RobotPathAdapter.translate(path_rosmsg)

        assert_path(ms_msg.get_data(RobotPath), path_rosmsg.data_field)

    def test_translate_raise_missing_required_key(self, path_rosmsg: ROSMessage):
        data = path_rosmsg.data_field
        data.pop("poses")
        with pytest.raises(ValueError, match="missing required keys"):
            RobotPathAdapter.from_dict(data)

    @pytest.mark.parametrize("typestore", ROS_TYPESTORE_TO_TEST)
    def test_to_ros_path(self, robot_path: RobotPath, typestore: Typestore):
        ros_msg = RobotPathAdapter.to_ros(robot_path, typestore, "nav_msgs/msg/Path")
        assert_path(robot_path, asdict(ros_msg))

    @pytest.mark.parametrize("typestore", ROS_TYPESTORE_TO_TEST)
    def test_to_ros_path_w_header(self, path_w_header: RobotPath, typestore: Typestore):
        ros_msg = RobotPathAdapter.to_ros(path_w_header, typestore, "nav_msgs/msg/Path")

        assert_path(path_w_header, asdict(ros_msg))

    @pytest.mark.parametrize("typestore", ROS_TYPESTORE_TO_TEST)
    def test_to_ros_default_type(self, robot_path: RobotPath, typestore: Typestore):
        ros_msg = RobotPathAdapter.to_ros(robot_path, typestore)

        assert_path(robot_path, asdict(ros_msg))

    def test_to_ros_invalid_rosmsg_type(self, robot_path: RobotPath):

        with pytest.raises(
            TypeError,
            match=f"Adapter {RobotPathAdapter.__name__} does not support nav_msgs/msg/Bogus",
        ):
            RobotPathAdapter.to_ros(
                robot_path, get_typestore(Stores.LATEST), "nav_msgs/msg/Bogus"
            )

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
def grid_cells_w_header(grid_cells, ms_header):
    grid_cells.header = ms_header
    return grid_cells


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

        assert_grid_cells(gc_msg.get_data(GridCells), grid_cells_rosmsg.data_field)

    def test_translate_raise_missing_required_key(self, grid_cells_rosmsg: ROSMessage):
        data = grid_cells_rosmsg.data_field
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
    def test_to_ros_grid_cells_w_header(
        self, grid_cells_w_header: GridCells, typestore: Typestore
    ):
        ros_msg = GridCellsAdapter.to_ros(
            grid_cells_w_header, typestore, "nav_msgs/msg/GridCells"
        )

        assert_grid_cells(grid_cells_w_header, asdict(ros_msg))

    @pytest.mark.parametrize("typestore", ROS_TYPESTORE_TO_TEST)
    def test_to_ros_default_type(self, grid_cells: GridCells, typestore: Typestore):
        ros_msg = GridCellsAdapter.to_ros(grid_cells, typestore)

        assert_grid_cells(grid_cells, asdict(ros_msg))

    def test_to_ros_invalid_rosmsg_type(self, grid_cells: GridCells):

        with pytest.raises(
            TypeError,
            match=f"Adapter {GridCellsAdapter.__name__} does not support nav_msgs/msg/Bogus",
        ):
            GridCellsAdapter.to_ros(
                grid_cells, get_typestore(Stores.LATEST), "nav_msgs/msg/Bogus"
            )

    def test_to_ros_invalid_mosaico_type(self, invalid_ms_msg):
        with pytest.raises(TypeError):
            GridCellsAdapter.to_ros(invalid_ms_msg, get_typestore(Stores.LATEST))


###############################################################################
########################### TestMapMetadataAdapter ############################
###############################################################################


@pytest.fixture
def map_metadata(pose):
    return MapMetadata(
        map_load_time=Time(seconds=100000, nanoseconds=1000),
        resolution=0.05,
        width=100,
        height=100,
        origin=pose,
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

        assert_map_metadata(
            mm_msg.get_data(MapMetadata), map_metadata_rosmsg.data_field
        )

    def test_translate_raise_missing_required_key(
        self, map_metadata_rosmsg: ROSMessage
    ):
        data = map_metadata_rosmsg.data_field
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
    def test_to_ros_default_type(self, map_metadata: MapMetadata, typestore: Typestore):
        ros_msg = MapMetadataAdapter.to_ros(map_metadata, typestore)
        assert_map_metadata(map_metadata, asdict(ros_msg))

    def test_to_ros_invalid_rosmsg_type(self, map_metadata: MapMetadata):

        with pytest.raises(
            TypeError,
            match=f"Adapter {MapMetadataAdapter.__name__} does not support nav_msgs/msg/Bogus",
        ):
            MapMetadataAdapter.to_ros(
                map_metadata, get_typestore(Stores.LATEST), "nav_msgs/msg/Bogus"
            )

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
def occupancy_grid_w_header(occupancy_grid, ms_header):
    occupancy_grid.header = ms_header
    return occupancy_grid


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

        assert_occupancy_grid(
            og_msg.get_data(OccupancyGrid), occupancy_grid_rosmsg.data_field
        )

    def test_translate_raise_missing_required_key(
        self, occupancy_grid_rosmsg: ROSMessage
    ):
        data = occupancy_grid_rosmsg.data_field
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
        self, occupancy_grid_w_header: OccupancyGrid, typestore: Typestore
    ):
        ros_msg = OccupancyGridAdapter.to_ros(
            occupancy_grid_w_header, typestore, "nav_msgs/msg/OccupancyGrid"
        )
        assert_occupancy_grid(occupancy_grid_w_header, asdict(ros_msg))

    @pytest.mark.parametrize("typestore", ROS_TYPESTORE_TO_TEST)
    def test_to_ros_default_type(
        self, occupancy_grid: OccupancyGrid, typestore: Typestore
    ):
        ros_msg = OccupancyGridAdapter.to_ros(occupancy_grid, typestore)
        assert_occupancy_grid(occupancy_grid, asdict(ros_msg))

    def test_to_ros_invalid_rosmsg_type(self, occupancy_grid: OccupancyGrid):
        with pytest.raises(
            TypeError,
            match=f"Adapter {MapMetadataAdapter.__name__} does not support nav_msgs/msg/Bogus",
        ):
            MapMetadataAdapter.to_ros(
                occupancy_grid, get_typestore(Stores.LATEST), "nav_msgs/msg/Bogus"
            )

    def test_to_ros_invalid_mosaico_type(self, invalid_ms_msg):
        with pytest.raises(TypeError):
            OccupancyGridAdapter.to_ros(invalid_ms_msg, get_typestore(Stores.LATEST))
