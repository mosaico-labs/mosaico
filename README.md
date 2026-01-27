<div align="center">
  <picture>
    <source media="(prefers-color-scheme: light)" srcset="logo/mono_black.svg">
    <source media="(prefers-color-scheme: dark)" srcset="logo/mono_white.svg">
    <img alt="Mosaico logo" src="logo/color_black.svg" height="100">
  </picture>
</div>

<br/>

<p align="center">
  <a href="https://github.com/mosaico-labs/mosaico/actions"><img src="https://img.shields.io/github/actions/workflow/status/mosaico-labs/mosaico/ci.yml?branch=main&label=main" alt="Github Actions Badge"></a>
  <a href="https://discord.gg/mwQtFnsckE"><img src="https://shields.io/discord/1413199575442784266" alt="discord" /></a>
</p>

## The Open-Source Data Platform for Robotics & Physical AI

Mosaico is a *blazing-fast* data platform designed to bridge the gap between Robotics and Physical AI. It streamlines data management, compression, and search by replacing monolithic files with a structured archive powered by Rust and Python.

## What you'll find
This repo contains both the Python SDK (`mosaico-sdk-py`) and the Rust backend (`mosaicod`). We have chosen to keep the code in a monorepo configuration to simplify the testing and reduce compatibility issues.

Mosaico takes a strictly code-first approach. 
We didn't want to force you to learn yet another SQL-like sublanguage just to move data around. 
Instead, we provide native SDKs (starting with Python) so you can query and upload data using the programming language you are already comfortable with. You can start exploring the [Python SDK documentation](./mosaico-sdk-py/README.md).

Under the hood, the system operates on a standard client-server model. 
The server daemon, [`mosaicod`](mosaicod/README.md), acts as the central hub that takes care of the heavy lifting, like data conversion, compression, and organized storage. 
On the other side, the client SDK is what you actually import into your scripts; it manages the communication logic and abstracts away the implementation details to keep your API usage stable, even as the platform evolves in the background.

For a deep dive into the platform's architecture read the [Core Concepts Guide](CORE_CONCEPTS.md).

### Streamlining Data for Physical AI

<p align="center">
  <img alt="ros to physical ai" src="doc/ros_physical_ai.png" />
</p>

Mosaico is designed to streamline the transition from raw robotic data to model-ready datasets. 

Mosaico enables the ingestion of standard ROS sequences, which often consist of non-synchronized, linear data streams, and automatically transforms them  into synchronized, randomized dataframes specifically structured for Physical AI applications, see [SDK ML](mosaico-sdk-py/doc-md/ml/ml.md) module for more details. By allowing users to define a custom sampling step, enables precise data windowing for fine-tuning, ensuring that the resulting algorithms are significantly more robust against the temporal jitter commonly found in real-world hardware.

Efficiency is built directly into the architecture, as data batches are streamed directly from the Mosaico data platform. This eliminate the need to download massive datasets locally before beginning work.

### Quick Start

You can start experimenting with Mosaico using the provided Docker Compose configuration (directory `docker/quick_start`).

```bash
cd docker/quick_start
docker compose up -d
```

This starts a Postgres instance on the default port `5432`, compiles Mosaico from source into a new Docker image, and runs it.

> [!NOTE]
>
> The default Mosaico configuration uses non persistent storage. 
> This means that if the container is destroyed, all stored data will be lost.
> Since Mosaico is still under active development, we provide this simple, volatile setup by default. 
> For persistent storage, the standard `compose.yml` file can be easily extended to utilize a Docker volume.

To install the provided Python SDK and its dependencies, first navigate in the repository root directory and then run the following commands:

```bash
cd mosaico-sdk-py
poetry install
```

### Cite Us

If you use Mosaico for a publication, please cite it as:

```bibtex
@software{MosaicoLabs,
  author = {{Mosaico Team}},
  title = {{Mosaico: The Open-Source Data Platform for Robotics and Physical AI.}},
  url = {https://mosaico.dev},
  version = {0.0},
  year = {2025},
  month = {12},
  address = {Online},
  note = {Available from https://mosaico.dev/ and https://github.com/mosaico-labs/mosaico}
}
```
