# Mosaico SDK

The Mosaico SDK is a Python interface designed specifically for managing **Physical AI and Robotics data**. Its purpose is to handle the complete lifecycle of information, from the moment it is captured by a sensor to the moment it is used to train a neural network or analyze a robot's behavior.

The SDK is built on the philosophy that robotics data is **unique**. Whether it comes from a autonomous car, a drone, or a factory arm, this data is multi-modal, highly frequent, and deeply interconnected in space and time. The Mosaico SDK provides the infrastructure to treat this data as a *first-class citizen* rather than just a collection of generic numbers. It understands the geometric and physical semantics of complex data types such as LIDAR point clouds, IMU readings, high-resolution camera feeds, and rigid-body transformations.

## Overview

The SDK is built on the following core principles:

### Middleware Independence

Mosaico is middleware-agnostic. While the SDK provides robust tools for ROS, it exists because robotics data itself is complex, regardless of the collection method. The platform serves as a standardized hub that can ingest data from:

* **Existing Frameworks**: Such as ROS 1, ROS 2, `.mcap` and `.db3` files.
* **Custom Collectors**: Proprietary data loggers or direct hardware drivers.
* **Simulators**: Synthetic data generated in virtual environments.

### Ontology

The [Mosaico Data Ontology](ontology.md) acts as the abstraction layer between your specific data collection system and your storage. Instead of saving "Topic A from Robot B" you save a `Pose`, an `IMU` reading, or an `Image`. 
Once data is in the platform, its origin becomes secondary to its universal, semantic format. Moreover, the ontology is designed to be extensible with no effort, to meet the needs of any domain; the custom types are automatically validatable, serializable, and queryable alongside standard types.

### High-Performance

Leveraging [Apache Arrow](https://arrow.apache.org/) for zero-copy performance, the SDK moves massive data volumes from the network to analysis tools without the CPU overhead of traditional data conversion. Every piece of data is time-synchronized, allowing the SDK to *replay* a session from dozens of sensors in the exact chronological order they occurred.

## Key Operations

### Data Ingestion

You can push data into Mosaico through two primary pathways, both designed to ensure your data is validated and standardized before storage:

**Native Ontology Ingestion**. This approach allows you to stream data directly from your application, providing the highest level of control over serialization and real-time performance.

**Ecosystem Adapters & Bridges**. Use specialized adapters to translate data from existing middleware and log formats into Mosaico sequences. Mosaico currently supports ROS 1 bags (`.bag`) and more recent formats like `.mcap` and `.db3`.


<figure markdown="span">
    ![Ingestion flow](../assets/ingestion_flow.svg){ width="500" }
</figure>

### Data Retrieval

Retrieving data goes beyond simple downloading. It is possible to stream and merge multiple topics into a single, time-ordered timeline, which is essential for sensor fusion. 
Connect directly to a specific sensor, such as just the front-facing camera, to save bandwidth and memory. 
The SDK fetches data in batches, allowing you to process datasets that are much larger than your computer's RAM.

### Querying & Discovery

Mosaico allows you to find data based on *what* happened, not just *when* it happened. You can search for specific sequences by metadata tags (like `robot_id` or `location`) or query the actual contents of the sensor data (e.g., *"Find all sequences where the vehicle acceleration exceeded 4 m/s^2"*).

### Machine Learning & Analytics

The [ML Module](bridges/ml.md) transforms raw, sparse sensor streams into the tabular formats required by modern AI:

* **Flattening**: Converts nested sensor data into organized tables (e.g. [`pandas.DataFrames`](https://pandas.pydata.org/docs/reference/api/pandas.DataFrame.html)).
* **Temporal Resampling**: Aligns sensors running at different speeds (e.g., a 100Hz IMU and a 5Hz GPS) onto a uniform time grid with custom frame-rate for model training.
