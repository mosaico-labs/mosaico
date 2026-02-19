# Streaming Data from Sequences and Topics

!!! info "Prerequisites"
    To fully grasp the following How-To, we recommend you to read the **[Reading a Sequence and its Topics](../howto/reading.md) How-To**.

This guide demonstrates how to interact with the Mosaico Data Platform to retrieve the data stream that has been previously ingested. You will learn how to use the Mosaico SDK to:

* **Obtain a [`SequenceDataStreamer`][mosaicolabs.handlers.SequenceDataStreamer]** to consume recordings from a sequence.
* **Obtain a [`TopicDataStreamer`][mosaicolabs.handlers.TopicDataStreamer]** to consume recordings from a topic.

For a more in-depth explanation:

* **[Documentation: The Reading Workflow](../handling/reading.md)**
* **[API Reference: Data Retrieval](../API_reference/handlers/reading.md)**

### Unified Multi-Sensor Replay

A [`SequenceDataStreamer`][mosaicolabs.handlers.SequenceDataStreamer] is designed for sensor fusion and full-system replay. It allows you to consume synchronized multiple data streams—such as high-rate IMU data and low-rate GPS fixes—as if they were a single, coherent timeline.

```python
from mosaicolabs import MosaicoClient 

with MosaicoClient.connect("localhost", 6726) as client:
    seq_handler = client.sequence_handler("mission_alpha")
    
    if seq_handler:
        # Initialize a Unified Stream for synchronized multi-sensor analysis
        streamer = seq_handler.get_data_streamer(
            # Filter specific topics
            topics=["/gps", "/imu"],
            # Define the optional temporal window: Only data in this range will be streamed
            start_timestamp_ns=1738508778000000000,
            end_timestamp_ns=1738509618000000000,
        )


        print(f"Streaming starts at: {streamer.next_timestamp()}")

        # Consume the stream. The loop yields messages from both topics in perfect chronological order
        for topic, msg in streamer:
            print(f"[{topic}] at {msg.timestamp_ns}: {type(msg.data).__name__}")

        # Finalize the reading channel to release server resources
        seq_handler.close()

```

### Targeted Access

A [`TopicDataStreamer`][mosaicolabs.handlers.TopicDataStreamer] provides a dedicated channel for interacting with a single data resource.

```python
from mosaicolabs import MosaicoClient, IMU

with MosaicoClient.connect("localhost", 6726) as client:
    # Access a specific topic handler directly via the client
    top_handler = client.topic_handler("mission_alpha", "/front/imu")
    
    if top_handler:
        # Start a Targeted Stream for isolated, low-overhead replay
        imu_stream = top_handler.get_data_streamer(
            # Define the optional temporal window: Only data in this range will be streamed
            start_timestamp_ns=1738508778000000000,
            end_timestamp_ns=1738509618000000000,
        )

        # Query the next timestamp, without consuming the message
        print(f"First sample at: {imu_stream.next_timestamp()}")

        # Direct loop for maximum efficiency
        for imu_msg in imu_stream:
            # Access the strongly-typed IMU data directly
            process_sample(imu_msg.get_data(IMU)) 

        # Finalize the topic channel
        top_handler.close()

```

### Streamer Comparison

| Feature | [`SequenceDataStreamer`][mosaicolabs.handlers.SequenceDataStreamer] | [`TopicDataStreamer`][mosaicolabs.handlers.TopicDataStreamer] |
| --- | --- | --- |
| **Primary Use Case** | Multi-sensor fusion & system-wide replay | Isolated sensor analysis & ML training |
| **Logic Overhead** | K-Way Merge Sorting | Direct Stream |
| **Output Type** | Tuple of `(topic_name, message)` | Single `message` object |
| **Temporal Slicing** | Supported | Supported |

