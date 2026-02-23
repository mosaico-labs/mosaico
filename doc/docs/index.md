
<style>
  .md-typeset h1,
  .md-content__button {
    display: none;
  }
</style>

![Mosaico logo](assets/doc_title.png)

**Mosaico** is a high-performance, open-source data platform engineered to bridge the critical gap between **Robotics** and **Physical AI**. 

Traditional robotic workflows often struggle with monolithic file formats like [ROS bag](https://wiki.ros.org/Bags), which are linear and difficult to search, index, or stream efficiently. Mosaico replaces these linear files with a structured, queryable archive powered by Rust and Python, designed specifically for the high-throughput demands of multi-modal sensor data.

The platform adopts a strictly **code-first approach**. We believe engineers shouldn't have to learn a proprietary SQL-like sublanguage to move data around. Instead, Mosaico provides native Python SDK that allows you to query, upload, and manipulate data using the programming languages you already know and love.

## Streamlining Data for Physical AI
The transition from classical robotics to Physical AI represents a fundamental shift in data requirements.

![Mosaico Bridge to Physical AI](assets/ros_physical_ai.png)

**Classical Robotics** operates in an event-driven world. Data is asynchronous, sparse, and stored in monolithic sequential files (like ROS bags). A Lidar might fire at 10Hz, an IMU at 100Hz, and a camera at 30Hz, all drifting relative to one another.


**Physical AI** requires synchronous, dense, and tabular data. Models expect fixed-size tensors arriving at a constant frequency (e.g., a batch of state vectors at exactly 50Hz).

Mosaico’s [ML module](SDK/bridges/ml.md) automates this tedious *data plumbing*. It ingests raw, unsynchronized data and transforms it on the fly into the aligned, flattened formats ready for model training, eliminating the need for massive intermediate CSV files.

## Core Concepts

To effectively use Mosaico, it is essential to understand the three pillars of its architecture: **Ontology**, **Topic**, and **Sequence**. These concepts transform raw binary streams into semantic, structured assets.

### The Ontology

The Ontology is the structural backbone of Mosaico. 
It serves as a semantic representation of all data used within your application, whether that consists of simple sensor readings or the complex results of an algorithmic process.

In Mosaico, all data is viewed through the lens of **time series**. 
Even a single data point is treated as a singular case of a time series. 
The ontology defines the *shape* of this data. It can represent base types (such as integers, floats, or strings) as well as complex structures (such as specific sensor arrays or processing results).

This abstraction allows Mosaico to understand what your data *is*, rather than just storing it as raw bytes. 
By using an ontology to inject and index data, you enable the platform to perform ad-hoc processing, such as custom compression or semantic indexing, tailored specifically to the type of data you have ingested.

Mosaico provides a series of [Ontology Models](SDK/ontology.md) for all the main sensors and applications in robotics. These are specific data structures representing a single data type. For example, a GPS sensor might be modeled as follows:

```python
class GPS:
    latitude: float
    longitude: float
    altitude: float
```

An image classification algorithm can be represented with an ontology model like:

```python
class SimpleImageClassification:
    top_left_corner: mosaicolabs.Vector2d
    bottom_right: mosaicolabs.Vector2d
    label: str
    confidence: float
```

Users can easily extend the platform by defining their own [Ontology Models](SDK/ontology.md). 

### Topics and Sequences

Once you have an Ontology Model, you need a way to instantiate it and store actual data. This is where the **Topic** comes in. 
*A Topic is a concrete instance of a specific ontology model.*
It functions as a container for a particular time series holding that specific data model. There is a strict one-to-one relationship here: one Topic corresponds to exactly one Ontology Model. This relationship allows you to query specific topics within the platform based on their semantic structure.

However, data rarely exists in isolation. Topics are usually part of a larger context. In Mosaico, this context is provided by the **Sequence**. A Sequence is a collection of logically related Topics.

To visualize this, think of a *ROS bag* or a recording of a robot's run. The recording session itself is the Sequence. Inside that Sequence, you have readings from a Lidar sensor, a GPS unit, and an accelerometer. Each of those individual sensor streams is a Topic, and each Topic follows the structure defined by its Ontology Model. Both Topics and Sequences can hold metadata to further describe their contents.

## Architecture

Mosaico follows a client-server architecture where users interact with the platform through the Python SDK to query, read, and write data. The SDK communicates with the Mosaico daemon a.k.a. `mosaicod`, a high-performance server written in Rust, using [Apache Arrow](https://arrow.apache.org/) for efficient columnar data exchange without serialization overhead.

`mosaicod` daemon handles all core data operations including [ingestion](daemon/ingestion.md), [retrieval](daemon/retrieval.md), and [query](daemon/query.md). It uses a database instance to accelerate metadata queries, manage system state, and implement an event queue for processing asynchronous tasks. Data files themselves are stored in an [object store](https://en.wikipedia.org/wiki/Object_storage) (such as [S3](https://aws.amazon.com/s3/), [MinIO](https://www.min.io/), or local filesystem) for durable, long-term persistence and scalability.

This design enables Mosaico to efficiently manage complex multi-modal sensor data while providing a simple, code-first interface for developers.

<figure markdown="span">
  ![Mosaico general flow](assets/general_flow.svg)
</figure>

