---
title: Data Discovery and Inspection
description: Example how-to for Reading a Sequence and its Topics
---

This guide demonstrates how to programmatically explore the Mosaico Data Platform to discover ingested sequences and inspect their internal structures.

By following this guide, you will learn how to:

1. **List all sequences** available on a remote server.
2. **Access high-level metadata** (size, creation time, duration) for a specific recording session.
3. **Drill down into individual topics** to identify sensor types and sampling spans.

!!! info "Prerequisites"
    This tutorial assumes you have already ingested data into your Mosaico instance, using the example described in the [ROS Injection guide](./ros_injection.md).

!!! example "Experiment Yourself"
    This guide is **fully executable**.

    1. **[Start the Mosaico Infrastructure](../../daemon/install.md)**
    2. **Run the example**
    ```bash
    mosaicolabs.examples data_inspection
    ```

!!! abstract "Full Code"
    The full code of the example is available [**here**](https://github.com/mosaico-labs/mosaico/blob/main/mosaico-sdk-py/src/mosaicolabs/examples/data_inspection.py).

??? question "In Depth Explanation"
    * **[Documentation: The Reading Workflow](../handling/reading.md)**
    * **[API Reference: Data Retrieval](../API_reference/handlers/reading.md)**


## Step 1: Connecting to the Catalog

The first step is establishing a control connection. We use the [`MosaicoClient`][mosaicolabs.comm.MosaicoClient] within a context manager to ensure that network resources are gracefully released when the script finishes.

```python
from mosaicolabs import MosaicoClient

# Connect to the gateway of the Mosaico Data Platform
with MosaicoClient.connect(host=MOSAICO_HOST, port=MOSAICO_PORT) as client:
    # Retrieve a simple list of all unique sequence identifiers
    seq_list = client.list_sequences()
    print(f"Discovered {len(seq_list)} sequences on the server.")
```


## Step 2: Inspecting Sequence Metadata

A **Sequence** represents a holistic recording session. To inspect its metadata without downloading bulk data, we request a [`SequenceHandler`][mosaicolabs.handlers.SequenceHandler]. This object acts as a "lazy" proxy to the server-side resource.

```python
# Assuming we are iterating through the seq_list from Step 1
for sequence_name in seq_list:
    shandler = client.sequence_handler(sequence_name)
    
    if shandler:
        # Access physical and logical diagnostics
        size_mb = shandler.total_size_bytes / (1024 * 1024)
        print(f"Sequence: {shandler.name}")
        print(f"• Remote Size: {size_mb:.2f} MB")
        print(f"• Created At:  {shandler.created_datetime}")
        
        # Determine the global temporal bounds of the entire session
        start, end = shandler.timestamp_ns_min, shandler.timestamp_ns_max
        print(f"• Time Span:   {start} to {end} ns")
```


## Step 3: Inspecting Individual Topics

Inside a sequence, data is partitioned into **Topics**, each corresponding to a specific sensor stream or data channel. We can use the [`SequenceHandler`][mosaicolabs.handlers.SequenceHandler] to spawn a [`TopicHandler`][mosaicolabs.handlers.TopicHandler] for granular inspection.

```python
# Iterate through all data channels in this sequence
for topic_name in shandler.topics:
    # Obtain a handler for the specific sensor stream
    thandler = shandler.get_topic_handler(topic_name)
    
    # Identify the semantic type of the data (e.g., 'imu', 'image')
    ontology = thandler.ontology_tag 
    
    # Calculate duration for this specific sensor
    duration_sec = 0
    if thandler.timestamp_ns_min and thandler.timestamp_ns_max:
        duration_sec = (thandler.timestamp_ns_max - thandler.timestamp_ns_min) / 1e9
        
    print(f"  - [{ontology}] {topic_name}: {duration_sec:.2f}s of data")
```

## Comparisons
### Sequence vs. Topic Handlers

| Feature | Sequence Handler | Topic Handler |
| --- | --- | --- |
| **Scope** | Entire Recording Session | Single Sensor |
| **Metadata** | Mission-wide (e.g., driver, weather) | Sensor-specific (e.g., model, serial) |
| **Time Bounds** | Global min/max of all topics | Min/max for that specific stream |
| **Topics** | List of all available streams | N/A |

### Catalog Layer vs. Data Layer (Handlers vs Streamers)

| Feature | Handlers (Catalog Layer) | Streamers (Data Layer) |
| :--- | :--- | :--- |
| **Primary Use** | Metadata inspection & discovery | High-volume data retrieval |
| **Object Type** | [`SequenceHandler`][mosaicolabs.handlers.SequenceHandler] / [`TopicHandler`][mosaicolabs.handlers.TopicHandler] | [`SequenceDataStreamer`][mosaicolabs.handlers.SequenceDataStreamer] / [`TopicDataStreamer`][mosaicolabs.handlers.TopicDataStreamer] |
| **Data Scope** | Size, Timestamps, Ontology Tags | Raw sensor messages |
