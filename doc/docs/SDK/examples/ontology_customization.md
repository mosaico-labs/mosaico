---
title: Customizing the Data Ontology
description: Example how-to for Customizing the Data Ontology
---

This guide demostrates how to extend the Mosaico Data Platform with custom data models. While Mosaico provides a rich default ontology for robotics (IMU, GPS, Images, etc.), specialized hardware often requires proprietary data structures.

By following this guide, you will be able to:

* **Define** strongly-typed data models using Python and Apache Arrow.
* **Register** these models so they are recognized by the Mosaico Ecosystem.
* **Integrate** them into the ingestion and retrieval pipelines.

!!! abstract "Full Code"
    The full code of the example is available [**here**](https://github.com/mosaico-labs/mosaico/blob/main/mosaico-sdk-py/src/mosaicolabs/examples/ros_injection/custom_ontology).

??? question "In Depth Explanation"
    * **[Documentation: Data Models & Ontology](../ontology.md)**
    * **[API Reference: Base Models and Mixins](../API_reference/models/base.md)**

### Step 1: Define the Custom Data Model

In Mosaico, data models are defined by inheriting from the **[`Serializable`][mosaicolabs.models.Serializable]** base class. This ensures that your model can be automatically translated into the platform's high-performance storage format.

For this example, we will create a model for **`EncoderTicks`**, found in the [NVIDIA R2B Dataset 2024](https://catalog.ngc.nvidia.com/orgs/nvidia/teams/isaac/resources/r2bdataset2024?version=1).

```python
from mosaicolabs import MosaicoField, MosaicoType, Serializable

class EncoderTicks(Serializable):
    """
    Custom Mosaico model for NVIDIA Isaac Nova Encoder Ticks.

    This model represents raw wheel encoder counts and their hardware-specific
    timestamps, providing the base data for dead-reckoning and odometry calculations.

    Attributes:
        left_ticks: Cumulative tick count for the left wheel.
        right_ticks: Cumulative tick count for the right wheel.
        encoder_timestamp: Timestamp of the encoder ticks.
    """

    # --- Pydantic Fields ---
    left_ticks: MosaicoType.uint32 = MosaicoField(
        description="Cumulative counts from the left wheel encoder."
    )
    """Cumulative tick count for the left wheel."""

    right_ticks: MosaicoType.uint32 = MosaicoField(
        description="Cumulative counts from the right wheel encoder."
    )
    """Cumulative tick count for the right wheel."""

    encoder_timestamp: MosaicoType.uint64 = MosaicoField(
        description="Timestamp of the encoder ticks."
    )
    """Timestamp of the encoder ticks."""

```

### Step 2: Ensure "Discovery" via Module Import

It is a common pitfall to define a class and expect the platform to "see" it immediately. Mosaico utilizes the [`Serializable.__pydantic_init_subclass__`][mosaicolabs.models.Serializable] hook to perform **automatic schema generation and registration** in the moment the class is loaded into memory by the Python interpreter.

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
