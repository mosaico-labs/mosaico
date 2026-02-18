
<style>
  .md-typeset h1,
  .md-content__button {
    display: none;
  }
</style>

![Mosaico logo](assets/doc_title.png)

**Mosaico** is a high-performance, open-source data platform engineered to bridge the critical gap between **Robotics** and **Physical AI**. Traditional robotic workflows often struggle with monolithic file formats like ROS bags, which are linear and difficult to search, index, or stream efficiently. Mosaico replaces these linear files with a structured, queryable archive powered by Rust and Python, designed specifically for the high-throughput demands of multi-modal sensor data.

The platform adopts a strictly **code-first approach**. We believe engineers shouldn't have to learn a proprietary SQL-like sublanguage to move data around. Instead, Mosaico provides native Python SDK that allows you to query, upload, and manipulate data using the programming languages you already know and love.

## Streamlining Data for Physical AI
The transition from classical robotics to Physical AI represents a fundamental shift in data requirements.

![Mosaico Bridge to Physical AI](assets/ros_physical_ai.png)

**Classical Robotics** operates in an event-driven world. Data is asynchronous, sparse, and stored in monolithic sequential files (like ROS bags). A Lidar might fire at 10Hz, an IMU at 100Hz, and a camera at 30Hz, all drifting relative to one another.


**Physical AI** requires synchronous, dense, and tabular data. Models expect fixed-size tensors arriving at a constant frequency (e.g., a batch of state vectors at exactly 50Hz).

Mosaico’s [ML module](SDK/bridges/ml.md) automates this tedious "data plumbing." It ingests raw, unsynchronized data and transforms it on the fly into the aligned, flattened formats ready for model training, eliminating the need for massive intermediate CSV files.

## Core Concepts

To effectively use Mosaico, it is essential to understand the three pillars of its architecture: **Ontology**, **Topic**, and **Sequence**. These concepts transform raw binary streams into semantic, structured assets.

### The Ontology

The Ontology is the structural backbone of Mosaico. 
It serves as a semantic representation of all data used within your application, whether that consists of simple sensor readings or the complex results of an algorithmic process.

In Mosaico, all data is viewed through the lens of **time series**. 
Even a single data point is treated as a singular case of a time series. 
The ontology defines the "shape" of this data. It can represent base types (such as integers, floats, or strings) as well as complex structures (such as specific sensor arrays or processing results).

This abstraction allows Mosaico to understand what your data *is*, rather than just storing it as raw bytes. 
By using an ontology to inject and index data, you enable the platform to perform ad-hoc processing, such as custom compression or semantic indexing, tailored specifically to the type of data you have ingested.

Users can easily extend the platform by defining their own [Ontology Models](SDK/ontology.md). These are specific data structures representing a single data type. For example, a GPS sensor might be modeled as follows:

``` python
class GPS:
    latitude:  Float
    longitude: Float
    altitude:  Float
```

### Topics and Sequences

Once you have an Ontology Model, you need a way to instantiate it and store actual data. This is where the **Topic** comes in. A Topic is a concrete instance of a specific Ontology Model. It functions as a container for a particular time series holding that specific data model. There is a strict one-to-one relationship here: one Topic corresponds to exactly one Ontology Model. This relationship allows you to query specific topics within the platform based on their semantic structure.

However, data rarely exists in isolation. Topics are usually part of a larger context. In Mosaico, this context is provided by the **Sequence**. A Sequence is a collection of logically related Topics.

To visualize this, think of a **ROS bag** or a recording of a robot's run. The recording session itself is the Sequence. Inside that Sequence, you have readings from a Lidar sensor, a GPS unit, and an accelerometer. Each of those individual sensor streams is a Topic, and each Topic follows the structure defined by its Ontology Model. Both Topics and Sequences can hold metadata to further describe their contents.

### Data Lifetime and Integrity

Maintaining a rigorous data lineage is a priority in Mosaico. To ensure that the history of your data remains pristine, Sequences and Topics are **immutable** once fully uploaded. This means that after the upload process is finalized, no data within that sequence can be altered.

The lifecycle of data in Mosaico follows a specific locking protocol to manage this immutability:

1.  **Creation**: when you begin an upload (e.g., uploading a new dataset or ROS bag), Mosaico creates a new Sequence.
2.  **Upload**: as you push data, the Topics are created and populated. During this phase, the Topics are also <Badge text="unlocked"/>. This is the only window in which data can be deleted if an error occurs.
3.  **Finalization** once the client confirms that all data has been uploaded successfully, it sends a command to lock the Sequence. At this point the sequence is locked.

A locked status signifies that the data is now permanent and immutable. An unlocked status implies the data is still in a transient state and can be deleted.:w

