---
title: Diagnostics
sidebar_position: 2
description: "Diagnostic tools compatible with Mosaico, including ros2_medkit for fault-triggered recording."
---

Mosaico stores sequences with arbitrary metadata, which makes it compatible with any tool that produces structured `.mcap` snapshots or injects records via the SDK.

## ros2_medkit

[ros2_medkit](https://github.com/selfpatch/ros2_medkit) is a ROS 2 node that monitors `/diagnostics` topics, holds recent sensor data in a ring buffer, and writes an `.mcap` snapshot when a fault is confirmed. Only the window around the fault event is persisted.

The snapshot is ingested into Mosaico with structured metadata: robot ID, fault code, severity, and timestamp. It is stored as a regular sequence and can be queried using the standard SDK.

For a walkthrough, see [ros2_medkit and Mosaico: Fault Forensics](https://www.selfpatch.ai/articles/en/medkit-mosaico-fault-forensics).

