# Machine Learning & Analytics Module

The **Mosaico ML** module serves as the primary bridge between the Mosaico Data Platform and the modern data science ecosystem. While Mosaico is optimized for high-performance ingestion and raw message streaming, the ML module provides high-level abstractions to transform these streams into tabular formats compatible with **Physical AI**, **Deep Learning**, and **Predictive Analytics**.

## Philosophy: Data for Physical AI

Working with robotics and multi-modal sensor data presents unique challenges for Machine Learning:
- **Heterogeneous Sampling:** Sensors (LIDAR, IMU, GPS) operate at different frequencies.
- **High Volume:** Data often exceeds available RAM.
- **Nested Structures:** Robotics data is often deeply nested (e.g., transformations, covariance matrices).

The ML module is designed to solve these issues by providing tools for **Flattening**, **Windowing**, and **Tabular Conversion**, allowing researchers to focus on model architecture rather than data plumbing.

## Current Components

| Component | Description | Status |
| :--- | :--- | :--- |
| [**DataFrameExtractor**](./dataframe_extractor.md) | High-performance conversion of sequences into sparse, flattened DataFrames. | Developed |
| **SyncTransformer** | Temporal synchronization and interpolation kernels (Linear, ZOH, Spline). | *In Development* |

## Ecosystem Integration

The ML module is built to be "Zero-Copy" where possible, leveraging Apache Arrow to move data from the Mosaico Flight streams into:
- **Pandas:** For exploratory data analysis (EDA) and traditional ML (Scikit-Learn).
- **Polars:** ***(Coming Soon)*** For high-speed, multi-threaded data processing.
- **PyTorch/TensorFlow:** ***(Planned)*** Ready for ingestion into custom `Dataset` and `DataLoader` classes.