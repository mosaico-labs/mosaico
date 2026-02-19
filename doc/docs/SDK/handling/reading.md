---
title: The Reading Workflow
description: Handlers, Writers, and Streamers.
---

The **Reading Workflow** in Mosaico is architected to separate resource discovery from high-volume data transmission. This is achieved through two distinct layers: **Handlers**, which serve as metadata proxies, and **Streamers**, which act as the high-performance data engines.

### Handlers: The Catalog Layer

Handlers are lightweight objects that represent a server-side resource. Their primary role is to provide immediate access to system information and user-defined metadata **without downloading the actual sensor data**. They act as the "Catalog" layer of the SDK, allowing you to inspect the contents of the platform before committing to a high-bandwidth data stream.

Mosaico provides two specialized handler types: `SequenceHandler` and `TopicHandler`.

#### `SequenceHandler`
API Reference: [`mosaicolabs.handlers.SequenceHandler`][mosaicolabs.handlers.SequenceHandler].

Represents a complete recording session. It provides a holistic view, allowing you to inspect all available topic names, global sequence metadata, and the overall temporal bounds (earliest and latest timestamps) of the session.

This example demonstrates how to use a Sequence handler to inspect metadata.

```python
import sys
from mosaicolabs import MosaicoClient

with MosaicoClient.connect("localhost", 6726) as client:
    # Use a Handler to inspect the catalog
    seq_handler = client.sequence_handler("mission_alpha")
    if seq_handler:     
        print(f"Sequence: {seq_handler.name}")
        print(f"\t| Topics: {seq_handler.topics}")
        print(f"\t| User metadata: {seq_handler.user_metadata}")
        print(f"\t| Timestamp span: {seq_handler.timestamp_ns_min} - {seq_handler.timestamp_ns_max}")
        print(f"\t| Created {seq_handler.sequence_info.created_datetime}")
        print(f"\t| Size (MB) {seq_handler.sequence_info.total_size_bytes/(1024*1024)}")

        # Once done, close the reading channel (recommended)
        seq_handler.close()
```


#### `TopicHandler`
API Reference: [`mosaicolabs.handlers.TopicHandler`][mosaicolabs.handlers.TopicHandler].

Represents a specific data channel within a sequence (e.g., a single IMU or Camera). It provides granular system info, such as the specific ontology model used and the data volume of that individual stream.

This example demonstrates how to use a Topic handler to inspect metadata.

```python
import sys
from mosaicolabs import MosaicoClient

with MosaicoClient.connect("localhost", 6726) as client:
    # Use a Handler to inspect the catalog
    top_handler = client.topic_handler("mission_alpha", "/front/imu")
    # Note that the same handler can be retrieve via the SequenceHandler of the parent sequence:
    # seq_handler = client.sequence_handler("mission_alpha")
    # top_handler = seq_handler.get_topic_handler("/front/imu")
    if top_handler:
        print(f"Sequence:Topic: {top_handler.sequence_name}:{top_handler.name}")
        print(f"\t| User metadata: {top_handler.user_metadata}")
        print(f"\t| Timestamp span: {top_handler.timestamp_ns_min} - {top_handler.timestamp_ns_max}")
        print(f"\t| Created {top_handler.topic_info.created_datetime}")
        print(f"\t| Size (MB) {top_handler.topic_info.total_size_bytes/(1024*1024)}")

        # Once done, close the reading channel (recommended)
        top_handler.close()
```

### Streamers: The Data Engines
Both handlers serve as **factories**; once you have identified the resource you need, the handler is used to spawn the appropriate Streamer to begin data consumption.
Streamers are the active components that manage the physical data exchange between the server and your application. They handle the complexities of network buffering, batch management, and the de-serialization of raw bytes into Mosaico `Message` objects.

#### `SequenceDataStreamer` (Unified Replay)
API Reference: [`mosaicolabs.handlers.SequenceDataStreamer`][mosaicolabs.handlers.SequenceDataStreamer].

The **`SequenceDataStreamer`** is a unified engine designed specifically for sensor fusion and full-system replay. It allows you to consume multiple data streams as if they were a single, coherent timeline.

To achieve this, the streamer employs the following technical mechanisms:

* **K-Way Merge Sorting**: The streamer monitors the timestamps across all requested topics simultaneously. On every iteration, it "peeks" at the next available message from each topic and yields the one with the lowest timestamp.
* **Strict Chronological Order**: This sorting ensures that messages are delivered in exact acquisition order, effectively normalizing topics that may operate at vastly different frequencies (e.g., high-rate IMU vs. low-rate GPS).
* **Temporal Slicing**: You can request a "windowed" extraction by specifying `start_timestamp_ns` and `end_timestamp_ns`. This is highly efficient as it avoids downloading the entire sequence, focusing only on the specific event or time range of interest.
* **Smart Buffering**: To maintain memory efficiency, the streamer retrieves data in memory-limited batches. As you iterate, processed batches are discarded and replaced with new data from the server, allowing you to stream sequences that exceed your available RAM.

This example demonstrates how to initiate and use the Sequence data stream.

```python
import sys
from mosaicolabs import MosaicoClient 

with MosaicoClient.connect("localhost", 6726) as client:
    # Use a Handler to inspect the catalog
    seq_handler = client.sequence_handler("mission_alpha")
    if seq_handler:
        # Start a Unified Stream (K-Way Merge) for multi-sensor replay
        # We only want GPS and IMU data for this synchronized analysis
        streamer = seq_handler.get_data_streamer(
            topics=["/gps", "/imu"], # Optionally filter topics
            # Optionally set the time window to extract
            start_timestamp_ns=1738508778000000000,
            end_timestamp_ns=1738509618000000000
        )

        # Check the start message timestamp
        print(f"Recording starts at: {streamer.next_timestamp()}")

        for topic, msg in streamer:
            # Processes GPS and IMU in perfect chronological order
            print(f"[{topic}] at {msg.timestamp_ns}: {type(msg.data).__name__}")

        # Once done, close the reading channel (recommended)
        seq_handler.close()
```


#### `TopicDataStreamer` (Targeted Access)
API Reference: [`mosaicolabs.handlers.TopicDataStreamer`][mosaicolabs.handlers.TopicDataStreamer].

The **`TopicDataStreamer`** provides a dedicated, high-throughput channel for interacting with a single data resource. By bypassing the complex synchronization logic required for merging multiple topics, it offers the lowest possible overhead for tasks requiring isolated data streams, such as training models on specific camera frames or IMU logs.

To ensure efficiency, the streamer supports the following features:

* **Temporal Slicing**: Much like the `SequenceDataStreamer`, you can extract data in a time-windowed fashion by specifying `start_timestamp_ns` and `end_timestamp_ns`. This ensures that only the relevant portion of the stream is retrieved rather than the entire dataset.
* **Smart Buffering**: Data is not downloaded all at once; instead, the SDK retrieves information in memory-limited batches, substituting old data with new batches as you iterate to maintain a constant, minimal memory footprint.

This example demonstrates how to initiate and use the Topic data stream.

```python
import sys
from mosaicolabs import MosaicoClient, IMU

with MosaicoClient.connect("localhost", 6726) as client:
    # Retrieve the topic handler using (e.g.) MosaicoClient
    top_handler = client.topic_handler("mission_alpha", "/front/imu")
    if top_handler:
        # Start a Targeted Stream for single-sensor replay
        imu_stream = top_handler.get_data_streamer(
            # Optionally set the time window to extract
            start_timestamp_ns=1738508778000000000,
            end_timestamp_ns=1738509618000000000
        )

        # Peek at the start time
        print(f"Recording starts at: {streamer.next_timestamp()}")

        # Direct, low-overhead loop
        for imu_msg in imu_stream:
            process_sample(imu_msg.get_data(IMU)) # Some custom process function

        # Once done, close the reading channel (recommended)
        top_handler.close()
```

