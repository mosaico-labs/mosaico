---
title: Overview
description: Why Mosaico is the PDF for Robotics and the ultimate solution to the data plumbing nightmare.
sidebar_position: 1
---

[**Mosaico Alchemy**](https://github.com/mosaico-labs/mosaico-alchemy) is a set of ready-to-use data ingestion pipelines designed specifically for Physical AI and Robotics. 

It is organized in **Packs**, where each *Pack* is focused on a specific use-case (like [**Robotic Manipulation**](./packs/manipulation.mdx)) and translates heterogeneous dataset formats into the same ontology used by the [**Mosaico SDK**](https://docs.mosaico.dev/python-sdk/). This allows you to instantly run powerful cross-dataset queries on deeply heterogeneous formats using the Mosaico SDK, completely eliminating the need to write custom parsers.


## The plumbing nightmare we accepted as normal

We like writing software, but doing data plumbing is not writing software. Today, the Physical AI sector is plagued by a silent, massive roadblock.

Every research team and hardware platform records data differently. We have legacy ROS `.bag` files that seem to belong to another era. We have complex HDF5 blocks, nested Parquets, and heavy TFRecords with obscure structures. When engineers try to consolidate these datasets to train universal foundation models, they end up spending 80% of their time writing ingestion scripts. Dealing with corrupted timestamps. Fighting mismatched coordinate frames. Unraveling chaotic serialization schemas.

This became the daily routine. The focus shifted from actual machine learning, to endless data wrangling. We accepted this complexity as normal, but it's not.

## Mosaico as the "PDF for Robotics"

In the early days of personal computers, sharing a document was a chaotic mess of proprietary formats. You know the story. Then the PDF was invented. It didn't matter what tool created the document; once it was compiled to a PDF, any machine could read it flawlessly.

Mosaico does exactly this for robotics and IoT, serving as the universal data protocol for Physical AI.

Traditional architectures struggle with the massive volume and variety of modern sensor suites, often limiting themselves to simple visualization. Mosaico instead provides a standardized, extremely high-performance underlying data representation that unifies all modalities into a foundation for petabyte-scale integration and orchestration.

Beyond simple observability, it enables true data-driven debugging, certifiable pipelines backed by native data lineage, and advanced multimodal search. Once your robotic telemetry is ingested into Mosaico, it becomes universally queryable. Instantly streamable. Completely agnostic to the custom sensors that originally generated it. This is not just a nice abstraction: it is a fundamental paradigm shift.