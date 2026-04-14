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
    pass
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

    robot_joints_timeseries: list[RobotJoint] = []
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
                    robot_joints_timeseries.append(joint_msg.get_data(RobotJoint))

                rob_joints_stream.close()

        # To begin with, visualise them on a table
        MAX_TABLE_ROWS = 50
        table = Table(title="Joint position values")
        for j_name in robot_joints_timeseries[0].names:
            table.add_column(j_name)

        for rob_joint in robot_joints_timeseries[:MAX_TABLE_ROWS]:
            table.add_row(*[f"{j:.8f}" for j in rob_joint.positions])

        console.print(table)

        # --- PHASE 3: TODO ---


if __name__ == "__main__":
    # Setup simple logging for background SDK processes
    log.basicConfig(level=log.INFO)
    main()
