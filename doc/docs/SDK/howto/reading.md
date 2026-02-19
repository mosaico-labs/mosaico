# Reading a Sequence and its Topics

This guide demonstrates how to interact with the Mosaico Data Platform to inspect and retrieve data that has been previously ingested. You will learn how to use the Mosaico SDK to:

* **Connect to the catalog** to find existing recordings.
* **Inspect sequence metadata** and temporal bounds.
* **Access specific topic handlers** to analyze individual sensor streams.

For a more in-depth explanation:

* **[Documentation: The Reading Workflow](../handling/reading.md)**
* **[API Reference: Data Retrieval](../API_reference/handlers/reading.md)**

### Step 1: Connecting to the Catalog

To begin inspecting data, you must establish a connection via the [`MosaicoClient`][mosaicolabs.comm.MosaicoClient]. Reading is managed through a context manager to ensure all network resources are cleanly released.

```python
from mosaicolabs import MosaicoClient

# Establish a secure connection to the Mosaico server
with MosaicoClient.connect("localhost", 6726) as client:
    # Use a Handler to inspect the catalog for a specific recording session
    seq_handler = client.sequence_handler("multi_sensor_ingestion")
    
    if not seq_handler:
        print("Sequence not found in the catalog.")
    else:
        # Proceed to inspect metadata (Step 2)
        pass

```

### Step 2: Inspecting Sequence Metadata

A [`SequenceHandler`][mosaicolabs.handlers.SequenceHandler] provides a view of a complete recording session without transferring the actual bulk data yet. This "lazy" inspection allows you to verify session parameters, such as the total size on disk and global user metadata.

```python
    """Inside the `if seq_handler:` block"""
    
    # Print sequence metadata
    print(f"Sequence: {seq_handler.name}")
    print(f"• Registered Topics: {seq_handler.topics}")
    print(f"• User Metadata: {seq_handler.user_metadata}")
    
    # Analyze temporal bounds (earliest and latest timestamps across all sensors)
    # Timestamps are consistently handled in nanoseconds
    start, end = seq_handler.timestamp_ns_min, seq_handler.timestamp_ns_max
    print(f"• Duration (ns): {end - start}")
    
    # Access structural info from the server
    size_mb = seq_handler.sequence_info.total_size_bytes / (1024 * 1024)
    print(f"• Total Size: {size_mb:.2f} MB")
    print(f"• Created At: {seq_handler.sequence_info.created_datetime}")

```

### Step 3: Accessing Individual Topics

While a sequence represents a "mission," a [`TopicHandler`][mosaicolabs.handlers.TopicHandler] represents a specific data channel within that mission (e.g., a single IMU or GPS).

```python
    """Inside the `if seq_handler:` block"""

    # Retrieve a specific handler for the IMU sensor
    imu_handler = seq_handler.get_topic_handler("sensors/imu")
    
    if imu_handler:
        print(f"Inspecting Topic: {imu_handler.name}")
        print(f"• Sensor Metadata: {imu_handler.user_metadata}")
        
        # Check topic-specific temporal bounds
        print(f"• Topic Span: {imu_handler.timestamp_ns_min} to {imu_handler.timestamp_ns_max}")
        
        # Topic-specific size on the server
        topic_mb = imu_handler.topic_info.total_size_bytes / (1024 * 1024)
        print(f"• Topic Size: {topic_mb:.2f} MB")

```

### Comparison: Sequence vs. Topic Handlers

| Feature | Sequence Handler | Topic Handler |
| --- | --- | --- |
| **Scope** | Entire Recording Session | Single Sensor Stream |
| **Metadata** | Mission-wide (e.g., driver, weather) | Sensor-specific (e.g., model, serial) |
| **Time Bounds** | Global min/max of all topics | Min/max for that specific stream |
| **Topics** | List of all available streams | N/A |

## The full example code

```python
from mosaicolabs import MosaicoClient

# Establish a secure connection to the Mosaico server
with MosaicoClient.connect("localhost", 6726) as client:
    # Use a Handler to inspect the catalog for a specific recording session
    seq_handler = client.sequence_handler("multi_sensor_ingestion")
    
    if not seq_handler:
        print("Sequence not found in the catalog.")
    else:
        # Proceed to inspect metadata (Step 2)
        pass

    # Print sequence metadata
    print(f"Sequence: {seq_handler.name}")
    print(f"• Registered Topics: {seq_handler.topics}")
    print(f"• User Metadata: {seq_handler.user_metadata}")
    
    # Analyze temporal bounds (earliest and latest timestamps across all sensors)
    # Timestamps are consistently handled in nanoseconds
    start, end = seq_handler.timestamp_ns_min, seq_handler.timestamp_ns_max
    print(f"• Duration (ns): {end - start}")
    
    # Access structural info from the server
    size_mb = seq_handler.sequence_info.total_size_bytes / (1024 * 1024)
    print(f"• Total Size: {size_mb:.2f} MB")
    print(f"• Created At: {seq_handler.sequence_info.created_datetime}")

    # Retrieve a specific handler for the IMU sensor
    imu_handler = seq_handler.get_topic_handler("sensors/imu")
    
    if imu_handler:
        print(f"Inspecting Topic: {imu_handler.name}")
        print(f"• Sensor Metadata: {imu_handler.user_metadata}")
        
        # Check topic-specific temporal bounds
        print(f"• Topic Span: {imu_handler.timestamp_ns_min} to {imu_handler.timestamp_ns_max}")
        
        # Topic-specific size on the server
        topic_mb = imu_handler.topic_info.total_size_bytes / (1024 * 1024)
        print(f"• Topic Size: {topic_mb:.2f} MB")
```