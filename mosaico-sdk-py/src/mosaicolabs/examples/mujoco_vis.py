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

import json
import logging as log
import sys
import urllib.request
from pathlib import Path

from rich.console import Console
from rich.panel import Panel

# Mosaico SDK Imports
from mosaicolabs import MosaicoClient, QuerySequence, QueryTopic, RobotJoint

# Example Imports
from .config import (
    API_KEY,
    ASSET_DIR,
    ENABLE_TLS,
    MOSAICO_HOST,
    MOSAICO_PORT,
)

# Initialize Rich Console for beautiful terminal output
console = Console()

# Try importing non-standard mosaico dependencies (mujoco, gitdir, matplotlib):
try:
    import mujoco as mujoco
    import mujoco.viewer
except Exception:
    console.print_exception()
    console.print("[bold red]Please run:[/bold red] pip install mujoco")
    sys.exit(1)

# NVIDIA R2B Dataset 2024 - Verified compatible with Mosaico: https://catalog.ngc.nvidia.com/orgs/nvidia/teams/isaac/resources/r2bdataset2024?version=1
# This sequence has been injested during ros_injestion example (https://docs.mosaico.dev/SDK/examples/ros_injection/)
ROBOT_SEQUENCE_NAME = "r2b_robotarm_0"

# Path to mujoco scene
MUJOCO_MENAGERIE_URL = str(
    "https://github.com/google-deepmind/mujoco_menagerie/tree/main/universal_robots_ur10e"
)
MUJOCO_XML_SCENE_PATH = str(Path(ASSET_DIR) / "universal_robots_ur10e/scene.xml")


def download_assets(url: str, output_dir: str) -> None:
    """
    Downloads all files from a GitHub folder URL into output_dir,
    preserving the directory structure.

    Args:
        url:        GitHub tree URL, e.g.
                    https://github.com/owner/repo/tree/branch/path/to/folder
        output_dir: Local directory where files will be saved.
    """
    # Expected format: https://github.com/<owner>/<repo>/tree/<branch>/<path>
    parts = url.rstrip("/").replace("https://github.com/", "").split("/")
    owner, repo = parts[0], parts[1]
    branch = parts[3]
    folder_path = "/".join(parts[4:])

    output_root = Path(output_dir)
    output_root.mkdir(parents=True, exist_ok=True)

    _download_folder(owner, repo, branch, folder_path, output_root)


def _download_folder(
    owner: str,
    repo: str,
    branch: str,
    folder_path: str,
    output_dir: Path,
) -> None:
    api_url = (
        f"https://api.github.com/repos/{owner}/{repo}"
        f"/contents/{folder_path}?ref={branch}"
    )
    req = urllib.request.Request(
        api_url,
        headers={"Accept": "application/vnd.github+json"},
    )

    with urllib.request.urlopen(req) as response:
        items = json.loads(response.read().decode())

    for item in items:
        item_output = output_dir / item["name"]

        if item["type"] == "file":
            print(f"Downloading {item['path']} …")
            with urllib.request.urlopen(item["download_url"]) as response:
                item_output.write_bytes(response.read())

        elif item["type"] == "dir":
            item_output.mkdir(parents=True, exist_ok=True)
            _download_folder(owner, repo, branch, item["path"], item_output)


def main():
    """
    Executes the multi-phase Mosaico workflow.

    The phases are:
    1. Connect & retrieve sequence metadata: get from backend a specific sequence with a query.
    2. Stream joint state topic: from query response, check that the expected data exist
    3. Plot joint trajectories: use matplotlib to plot the trajectory for each robot joint
    4. Replay in MuJoCo: replay robot motion in Mujoco accordingly to timestamps
    """

    # --- PHASE 1: Connect & retrieve sequence metadata ---
    # Connect to the client using a context manager to ensure resource cleanup + query creation.
    console.print(
        Panel(
            f"[bold green]Phase 1: Connect & retrieve sequence metadata {ROBOT_SEQUENCE_NAME}[/bold green]"
        )
    )

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

                if not Path(MUJOCO_XML_SCENE_PATH).exists():
                    console.print(
                        "[bold yellow]Downloading MuJoCo assets[/bold yellow]"
                    )
                    download_assets(
                        MUJOCO_MENAGERIE_URL, ASSET_DIR + "/universal_robots_ur10e"
                    )
                else:
                    console.print(
                        "[bold yellow]MuJoCo assets already present. Skipping download... [/bold yellow]"
                    )

                model = mujoco.MjModel.from_xml_path(MUJOCO_XML_SCENE_PATH)
                data = mujoco.MjData(model)

                mujoco.mj_step(model, data)

                with mujoco.viewer.launch_passive(model, data) as viewer:
                    for joint_msg in rob_joints_stream:
                        relative_ts = (
                            joint_msg.timestamp_ns - top_handler.timestamp_ns_min
                        ) / 1.0e9

                        joints = joint_msg.get_data(RobotJoint)

                        # Set robot to initial configuration (only at start)
                        initial_config = False

                        if not viewer.is_running():
                            continue

                        with viewer.lock():
                            while data.time < relative_ts:
                                for jn, jp in zip(joints.names, joints.positions):
                                    id = mujoco.mj_name2id(
                                        model, mujoco.mjtObj.mjOBJ_JOINT, jn
                                    )

                                    if not initial_config:
                                        initial_config = True
                                        data.qpos[id] = jp

                                    data.ctrl[id] = jp

                                mujoco.mj_step(model, data)

                        viewer.sync()

                    rob_joints_stream.close()


if __name__ == "__main__":
    # Setup simple logging for background SDK processes
    log.basicConfig(level=log.INFO)
    main()
