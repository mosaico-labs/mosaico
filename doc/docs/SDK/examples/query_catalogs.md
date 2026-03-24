---
title: Querying Catalogs
description: Example how-to for Querying Catalogs
---


By following this guide, you will learn how to:

1.  **Perform Fuzzy Searches**: Find topics based on partial name matches.
2.  **Filter by Sensor Type**: Isolate all topics belonging to a specific ontology (e.g., all IMUs).
3.  **Execute Multi-Domain Queries**: Correlate sensor names with physical events (e.g., "Find the front camera IMU data where acceleration on the y-axis exceeded 1.0 m/s^2").

!!! info "Prerequisites"
    This tutorial assumes you have already ingested data into your Mosaico instance, using the example described in the [ROS Injection guide](./ros_injection.md).

!!! example "Experiment Yourself"
    This guide is **fully executable**.

    1. **[Start the Mosaico Infrastructure](../../daemon/install.md)**
    2. **Run the example**
    ```bash
    mosaicolabs.examples query_catalogs
    ```

??? question "In Depth Explanation"
    * **[Documentation: Querying Catalogs](../query.md)**
    * **[API Reference: Query Builders](../API_reference/query/builders.md)**
    * **[API Reference: Query Response](../API_reference/query/response.md)**

## The Query Philosophy: Chaining & Builders

Mosaico uses specialized **Query Builders** that provide a "fluent" interface. When you pass multiple builders to the [`client.query()`][mosaicolabs.comm.MosaicoClient.query] method, the platform joins them with a logical **AND** condition—returning only results that satisfy every criteria simultaneously.


## Step 1: Finding Topics by Name Match

The [`QueryTopic`][mosaicolabs.models.query.builders.QueryTopic] builder allows you to search for data channels without knowing their exact full path.

```python
from mosaicolabs import MosaicoClient, QueryTopic

with MosaicoClient.connect(host="localhost", port=6726) as client:
    # Use a convenience method for fuzzy name matching (equivalent to SQL %match%)
    results = client.query(
        QueryTopic().with_name_match("image_raw") # (1)!
    )

    if results:
        for item in results:
            print(f"Sequence: {item.sequence.name}")
            for topic in item.topics:
                print(f"  - Matched: {topic.name}")
```

1. The [`with_name_match()`][mosaicolabs.models.query.builders.QueryTopic.with_name_match] method allows you to filter for topics that match a pattern. In this case, we are filtering for topics that contain *"image_raw"* in their name. This is equivalent to using the SQL `LIKE` operator with a wildcard.

### Understanding the Response Structure

The `query()` method returns a [`QueryResponse`][mosaicolabs.models.query.QueryResponse] object, which is hierarchically grouped to make data management easy:

| Object | Purpose |
| :--- | :--- |
| **`item.sequence`** | Contains the name of the sequence where the match was found. |
| **`item.topics`** | A list of only the topics within that sequence that satisfied the criteria. |
| **`topic.timestamp_range`** | Provides the `start` and `end` nanoseconds for the matching event. |

!!! info "Result Normalization"
    The `topic.name` returns the relative topic path (e.g., `/front/camera/image`), which is immediately compatible with other SDK methods like [`MosaicoClient.topic_handler()`][mosaicolabs.comm.MosaicoClient.topic_handler].


## Step 2: Filtering by Ontology (Sensor Type)

Instead of hardcoding strings, Mosaico allows you to query by the **semantic type** of the data. This ensures that if you rename a topic, your analysis scripts won't break as long as the sensor type remains the same.

```python
from mosaicolabs import IMU, QueryTopic

# Retrieve the unique ontology tag dynamically from the class
results = client.query(
    QueryTopic().with_ontology_tag(IMU.ontology_tag()) # (1)!
)

# This returns every IMU topic across all sequences in the database.
```

1. The [`ontology_tag()`][mosaicolabs.models.Serializable.ontology_tag] method returns the unique identifier for the ontology class.

## Step 3: Multi-Domain Queries (Physics + Metadata)

This is the most powerful feature of the platform. We use the [**`.Q` Query Proxy**](../ontology.md#querying-data-ontology-with-the-query-q-proxy), a type-safe bridge that allows you to construct filters using standard Python dot notation.

In this example, we identify specific time segments where a specific IMU sensor recorded a lateral acceleration (y-axis) greater than or equal to 1.0 m/s^2.

```python
from mosaicolabs import IMU, QueryOntologyCatalog, QueryTopic

results = client.query(
    # Layer 1: Physics (Ontology Catalog)
    QueryOntologyCatalog(
        IMU.Q.acceleration.y.geq(1.0),
        include_timestamp_range=True # (1)!
    ),
    # Layer 2: Configuration (Topic Name)
    QueryTopic().with_name("/front_stereo_imu/imu") # (2)!
)
```

1. The `include_timestamp_range=True` flag tells the server to return the first and last timestamps of the queried condition within a topic, allowing you to slice data accurately for further analysis.
2. The [`with_name()`][mosaicolabs.models.query.builders.QueryTopic.with_name] method allows you to filter by topic name. In this case, we are (exactly) filtering for the `/front_stereo_imu/imu` topic. You can also use the [`with_name_match()`][mosaicolabs.models.query.builders.QueryTopic.with_name_match] method to filter for topics that match a pattern.

**Architect's Note:** Notice how we pass two different builders. The server will only return the `/front_stereo_imu/imu` topic, and only if it contains values >= 1.0.


## Practical Application: Replaying the "Impact"

Once you have the results, you can immediately slice the data for playback:

```python
if results:
    for item in results:
        for topic in item.topics:
            # Slice the sequence to only the relevant 2 seconds of the event
            streamer = client.sequence_handler(item.sequence.name).get_data_streamer(
                topics=[topic.name],
                start_timestamp_ns=topic.timestamp_range.start - 1_000_000_000,
                end_timestamp_ns=topic.timestamp_range.end + 1_000_000_000
            )
            # Replay the high-acceleration event...
```
