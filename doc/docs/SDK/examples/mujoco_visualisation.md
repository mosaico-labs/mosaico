---
title: Mujoco Visualisation
description: Example how-to for reproducing and visualising ingested data in MuJoCo
---

In this example, you will learn how to query a specific Topic from a specific Sequence and visualise its data through MuJoCo simulation. In particular, robot joint trajectories will be reproduced in [MuJoCo](https://mujoco.org/).
By following this guide, you will learn how to:

1.  **Connect & retrieve sequence metadata**: get a specific sequence from the backend.
2.  **Stream joint state topic**: from the query response, check that the expected data exists.
3.  **Replay in MuJoCo**: replay robot motion in MuJoCo according to timestamps.

!!! info "Prerequisites"
    1. This tutorial assumes you have already ingested data into your Mosaico instance, using the example described in the [ROS Ingestion](./ros_injection.md) example.
    2. This tutorial requires some additional dependencies that are not included by default. To avoid polluting Poetry dependencies, please run: `pip install mujoco`

!!! example "Experiment Yourself"
    This guide is **fully executable**.

    1. **[Start the Mosaico Infrastructure](../../daemon/install.md)**
    2. **Run the [ROS Ingestion](./ros_injection.md) example**
    ```bash
    mosaicolabs.examples ros_injection
    ```
    3. **Run the example**
    ```bash
    mosaicolabs.examples mujoco_vis
    ```

!!! abstract "Full Code"
    The full code of the example is available [**here**](https://github.com/mosaico-labs/mosaico/blob/main/mosaico-sdk-py/src/mosaicolabs/examples/mujoco_vis/main.py).

??? question "In Depth Explanation"
    * **[API Reference: Query Builders](../API_reference/query/builders.md)**
    * **[API Reference: Query Response](../API_reference/query/response.md)**
    * **[Documentation: The Reading Workflow](../handling/reading.md)**

## Step 1: Connect & retrieve sequence metadata

The combination of [`QuerySequence`][mosaicolabs.models.query.builders.QuerySequence] and [`QueryTopic`][mosaicolabs.models.query.builders.QueryTopic] builders allows you to search for specific data channels by Sequence name and Topic ontology [RobotJoint][mosaicolabs.models.sensors.robot.RobotJoint].

```python
with MosaicoClient.connect(
    host=MOSAICO_HOST,
    port=MOSAICO_PORT,
) as client:
    result = client.query(
        QuerySequence().with_name(ROBOT_SEQUENCE_NAME), # (1)!
        QueryTopic().with_ontology_tag(RobotJoint.ontology_tag()), # (2)!
    )

    if result is None:
        console.print(
            f"[bold red] ERROR: could not find sequence called {ROBOT_SEQUENCE_NAME} [/bold red]"
        )
```

1. The [`with_name`][mosaicolabs.models.query.builders.QueryTopic.with_name] method allows you to filter for sequences that match an exact pattern.

2. The [`ontology_tag()`][mosaicolabs.models.Serializable.ontology_tag] method returns the unique identifier for the ontology class.

## Step 2: Stream joint state topic

As detailed in the [data inspection](data_inspection.md) example, it is possible to narrow down the search for a specific sensor stream through its metadata, without downloading all the data associated with the other sequence's topics. We use [`TopicHandler`][mosaicolabs.handlers.TopicHandler] plus [RobotJoint][mosaicolabs.models.sensors.robot.RobotJoint] ontology for granular inspection.

```python
top_handler = client.topic_handler(items.sequence.name, topic.name)

if top_handler is None:
    console.print("Topic handler is None")
    continue

if top_handler.ontology_tag != RobotJoint.ontology_tag():
    console.print(
        f"Topic handler is not of {RobotJoint.ontology_tag()} type but of type {top_handler.ontology_tag}"
    )
    continue
```

> Notice that checking the topic ontology tag again is redundant since the initial query already filters out all the topics that are not a [RobotJoint][mosaicolabs.models.sensors.robot.RobotJoint] ontology.

Finally, data is streamed from the Mosaico server and the timestamps are converted relative to the trajectory start time.

```python
for joint_msg in rob_joints_stream:
    relative_ts = (
        joint_msg.timestamp_ns - top_handler.timestamp_ns_min
    ) / 1.0e9

    joints = joint_msg.get_data(RobotJoint)
```

## Step 3: Replay in MuJoCo

Finally, it is possible to reproduce the robot's trajectory in a simulated environment like MuJoCo:

![alt text](../../assets/robot_replay.gif "Robot joints MuJoCo")

