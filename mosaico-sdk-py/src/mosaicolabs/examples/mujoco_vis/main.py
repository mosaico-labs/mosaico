"""
Mosaico SDK: Retrieving and Visualizing a UR Robot Sequence.

Please ensure that you have run the ros_injestion example (https://docs.mosaico.dev/SDK/examples/ros_injection/) before this one

This script demonstrates a complete workflow:
    1. Connect & retrieve sequence metadata: get from backend a specific sequence.
    2. Stream joint state topic
    3. Plot joint trajectories
    4. Replay in MuJoCo

Run the example via:
```bash

mosaicolabs.examples mujoco_vis

```
"""

import logging as log
import sys

from rich.console import Console
from rich.panel import Panel
from rich.table import Table

# Mosaico SDK Imports
from mosaicolabs import MosaicoClient, QuerySequence, QueryTopic, RobotJoint

# Example Imports
from ..config import (
    API_KEY,
    ENABLE_TLS,
    MOSAICO_HOST,
    MOSAICO_PORT,
)

# Initialize Rich Console for beautiful terminal output
console = Console()

# Try importing non-standard mosaico dependencies (mujoco, matplotlib):
try:
    pass
except Exception:
    console.print_exception()
    console.print("[bold red]Please run:[/bold red] poetry add mujoco")
    sys.exit(1)

try:
    import matplotlib.pyplot as plt
except Exception:
    console.print_exception()
    console.print("[bold red]Please run:[/ red bold] poetry add matplotlib ")
    sys.exit(1)

# NVIDIA R2B Dataset 2024 - Verified compatible with Mosaico: https://catalog.ngc.nvidia.com/orgs/nvidia/teams/isaac/resources/r2bdataset2024?version=1
# This sequence has been injested during ros_injestion example (https://docs.mosaico.dev/SDK/examples/ros_injection/)
ROBOT_SEQUENCE_NAME = "r2b_robotarm_0"


def main():
    """
    Executes the multi-phase Mosaico workflow.

    The phases are:
    1. Connect & retrieve sequence metadata: get from backend a specific sequence with a query.
    2. Stream joint state topic: from query response, check that the expected data exist
    3. Plot joint trajectories: # TODO
    4. Replay in MuJoCo: # TODO
    """

    # --- PHASE 1: Connect & retrieve sequence metadata ---
    # Connect to the client using a context manager to ensure resource cleanup + query creation.
    console.print(
        Panel(
            "[bold green]Phase 1: Connect & retrieve sequence metadata {ROBOT_SEQUENCE_NAME}[/bold green]"
        )
    )

    # Dict containing all the joint timeseries. Organised as the following
    # {
    #   t1: [j1, j2, ..., j6],
    #   t2: [j1, j2, ..., j6],
    #   ...
    #   tn: [j1, j2, ..., j6],
    # }
    robot_joints_timeseries: dict[int, list[RobotJoint]] = {}

    with MosaicoClient.connect(
        host=MOSAICO_HOST,
        port=MOSAICO_PORT,
        enable_tls=ENABLE_TLS,
        api_key=API_KEY,
    ) as client:
        result = client.query(
            QuerySequence().with_name(ROBOT_SEQUENCE_NAME),
            QueryTopic().with_ontology_tag(RobotJoint.ontology_tag()),
        )

        if result is None:
            console.print(
                f"[bold red] ERROR: could not find sequence called {ROBOT_SEQUENCE_NAME} [/bold red]"
            )
            console.print(
                "[bold yellow] Please ensure you run ros_injection example (https://docs.mosaico.dev/SDK/examples/ros_injection/) before this one! [/bold yellow]"
            )
            sys.exit(1)

        # --- PHASE 2: Stream joint state topic ---
        for items in result:
            console.print(
                f"[bold green] Sequence {items.sequence.name} contains {len(items.topics)} topics of type {RobotJoint.ontology_tag()} [/bold green]"
            )
            for topic in items.topics:
                top_handler = client.topic_handler(items.sequence.name, topic.name)

                if top_handler is None:
                    console.print("Topic handler is None")
                    continue

                if top_handler.ontology_tag != RobotJoint.ontology_tag():
                    console.print(
                        f"Topic handler is not of {RobotJoint.ontology_tag()} type but of type {top_handler.ontology_tag}"
                    )
                    continue

                rob_joints_stream = top_handler.get_data_streamer()

                for joint_msg in rob_joints_stream:
                    relative_ts = joint_msg.timestamp_ns - top_handler.timestamp_ns_min
                    robot_joints_timeseries.update(
                        {relative_ts: joint_msg.get_data(RobotJoint)}
                    )

                rob_joints_stream.close()

        # To begin with, visualise them on a table
        MAX_TABLE_ROWS = 100
        table = Table(title="Joint position values")
        table.add_column("timestep relative [s]")
        for j_name in list(robot_joints_timeseries.values())[0].names:
            table.add_column(j_name)

        for timestep, rob_joint in list(robot_joints_timeseries.items())[
            :MAX_TABLE_ROWS
        ]:
            timestep_s = timestep / 1.0e9
            table.add_row(f"{timestep_s}", *[f"{j:.8f}" for j in rob_joint.positions])

        console.print(table)

        # --- PHASE 3: TODO ---

        timestamps = [t for t in robot_joints_timeseries.keys()]
        joint_values = list(robot_joints_timeseries.values())
        joint_names = joint_values[0].names

        fig, ax = plt.subplots(2, 3, figsize=(14, 7))
        fig.suptitle("Robot Joint Positions over Time", fontsize=14, fontweight="bold")

        for idx, (row, col) in enumerate(
            [(0, 0), (0, 1), (0, 2), (1, 0), (1, 1), (1, 2)]
        ):
            ax[row, col].plot(timestamps, [rj.positions[idx] for rj in joint_values])
            ax[row, col].set_title(joint_names[idx])
            ax[row, col].set_xlabel("time [s]")
            ax[row, col].set_ylabel("position [rad]")
            ax[row, col].grid(True)

        fig.tight_layout()
        plt.show()


if __name__ == "__main__":
    # Setup simple logging for background SDK processes
    log.basicConfig(level=log.INFO)
    main()
