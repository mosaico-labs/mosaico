# Customizing the Data Ontology

This guide walks you through the process of extending the Mosaico Data Platform with custom data models. While Mosaico provides a rich default ontology for robotics (IMU, GPS, Images, etc.), specialized hardware often requires proprietary data structures.

By the end of this guide, you will be able to:

* **Define** strongly-typed data models using Python and Apache Arrow.
* **Register** these models so they are recognized by the Mosaico Ecosystem.
* **Integrate** them into the ingestion and retrieval pipelines.

For a more in-depth explanation:

* **[Documentation: Data Models & Ontology](../ontology.md)**
* **[API Reference: Base Models and Mixins](../API_reference/models/base.md)**

### Step 1: Define the Custom Data Model

In Mosaico, data models are defined by inheriting from the **[`Serializable`][mosaicolabs.models.Serializable]** base class. This ensures that your model can be automatically translated into the platform's high-performance storage format.

For this example, we will create a model for **`EncoderTicks`**, found in the NVIDIA Isaac-related datasets.

```python
import pyarrow as pa
from mosaicolabs import HeaderMixin, Serializable

class EncoderTicks(
    Serializable, # Automatically registers the model via `Serializable.__init_subclass__`
    HeaderMixin,  # Injects standard metadata (timestamp, frame_id, seq)
):
    """
    Custom model for hardware-level encoder tick readings.
    """

    # --- Wire Schema Definition (Apache Arrow) ---
    # This defines the high-performance binary storage format on the server.
    __msco_pyarrow_struct__ = pa.struct([
        pa.field("left_ticks", pa.uint32(), nullable=False),
        pa.field("right_ticks", pa.uint32(), nullable=False),
        pa.field("encoder_timestamp", pa.uint64(), nullable=False),
    ])

    # --- Data Fields ---
    # Names and types must strictly match the Apache Arrow schema above.
    left_ticks: int
    right_ticks: int
    encoder_timestamp: int

```

### Step 2: Ensure "Discovery" via Module Import

It is a common pitfall to define a class and expect the platform to "see" it immediately. Mosaico utilizes the [`Serializable.__init_subclass__`][mosaicolabs.models.Serializable] hook to perform **automatic registration** the moment the class is loaded into memory by the Python interpreter.

For your custom type to be available in your application (especially during ingestion or when using the [`ROSBridge`][mosaicolabs.ros_bridge.ROSBridge]), you **must** ensure the module containing the class is imported.

#### Best Practice: The Registry Pattern

Create a dedicated `models.py` or `ontology/` package for your project and import it at your application's entry point.

```python
# app/main.py
import my_project.ontology.encoders as encoders # <-- This triggers the registration
from mosaicolabs import MosaicoClient

def run_ingestion():
    with MosaicoClient.connect(...) as client:
        # Now 'EncoderTicks' is a valid ontology_type for topic creation
        with client.sequence_create(name="test") as sw:
            tw = sw.topic_create("ticks", ontology_type=encoders.EncoderTicks)
            # ...

```

### Step 3: Verifying Registration

If you are unsure whether your model has been correctly "seen" by the ecosystem, you can check the internal registry of the `Serializable` class.

```python
from mosaicolabs import Serializable
import my_project.ontology.encoders as encoders # <-- This triggers the registration

if encoders.EncoderTicks.is_registered():
    print("Registration successful!")

```
