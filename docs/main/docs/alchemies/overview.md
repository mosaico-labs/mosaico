---
title: Overview
description: What Mosaico Alchemy is and why it exists.
sidebar_position: 1
---

[**Mosaico Alchemy**](https://github.com/mosaico-labs/mosaico-alchemy) is a collection of
ready-to-use data ingestion pipelines for Physical AI and Robotics. It is organized in
**Packs**, where each Pack targets a specific domain (like
[**Robotic Manipulation**](./packs/manipulation.mdx)) and translates heterogeneous dataset
formats into the ontology used by the [**Mosaico SDK**](https://docs.mosaico.dev/python-sdk/).

## Why we built it

If you have ever tried to combine datasets from different sources, you already know how
this goes. One dataset is a ROS `.bag` from a lab experiment. Another is an HDF5 archive
from a simulation run. A third comes from a hardware vendor with a custom binary format
and a PDF that vaguely describes the schema. They all contain robot data, but getting them
into a shape where you can actually compare or join them is a project in itself.

Most of that work is not interesting. It is timestamp reconciliation, coordinate frame
alignment, figuring out whether that `vel` field is in m/s or mm/s, and writing throwaway
scripts you will never look at again.

The deeper point is that this problem is not really about file formats. It is about the
fact that the same physical concept, a robot's pose, a joint angle, a camera frame, gets
expressed differently by every team and every tool. Alchemy's goal is to show that it is
possible to bring all of that under a single, coherent representation. Once data from
different sources speaks the same ontology, you can run the same queries against all of
it, build datasets that span multiple collection pipelines, and stop worrying about the
plumbing every time you add a new source.

Each Pack is also a concrete entry point into Mosaico's data pipeline. If you are working
with a known dataset format, like those covered in the Robotic Manipulation pack, you
can use Alchemy to load it directly into the platform without writing any ingestion code
yourself. From that point on, the full Mosaico SDK is available: query across sequences,
stream specific topics, filter by sensor values, and feed data straight into your training
pipeline.
