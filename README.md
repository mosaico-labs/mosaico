<div align="center">
  <picture>
    <a href="https://mosaico.dev"><img alt="Mosaico logo" src="https://mosaico.dev/mosaico-loop@920px-25fps-crop.gif" width="600px"></a>
  </picture>
</div>
<br/>

<p align="center">
  <a href="https://discord.gg/mwQtFnsckE"><img src="https://shields.io/discord/1413199575442784266?color=%235865f2" alt="discord" /></a>
  <a href="https://www.python.org/downloads/"><img src="https://img.shields.io/badge/python-3.10+-blue.svg" alt="python version"/></a>
  <a href="https://docs.mosaico.dev"><img src="https://img.shields.io/badge/-%20Documentation-%23bd38b4?style=flat&logo=readthedocs&logoColor=white&labelColor=gray" alt="documentation"></a>
</p>

<p align="center">
  <a href="https://github.com/mosaico-labs/mosaico/pkgs/container/mosaicod"><img src="https://img.shields.io/badge/mosaicod-v0.4.0-orange" /></a>
  <a href="https://pypi.org/project/mosaicolabs"><img src="https://img.shields.io/pypi/v/mosaicolabs?color=blue" /></a>
</p>

<p align="center">
  <a target="_blank" href="https://discord.gg/mwQtFnsckE"><img src="https://img.shields.io/badge/Discord-Join%20Server-5865F2?style=for-the-badge&logo=discord" alt="Discord" /></a>
</p>

# The Data Platform for Robotics and Physical AI

**Mosaico** is a *blazing-fast* data platform designed to bridge the gap between Robotics and Physical AI. It streamlines data management, compression, and search by replacing monolithic files with a structured archive powered by Rust and Python.

## Streamlining Data for Physical AI
The transition from classical robotics to Physical AI represents a fundamental shift in data requirements.

![Mosaico Bridge to Physical AI](doc/docs/assets/ros_physical_ai.png)

**Classical Robotics** operates in an event-driven world. Data is asynchronous, sparse, and stored in monolithic sequential files (like ROS bags). A Lidar might fire at 10Hz, an IMU at 100Hz, and a camera at 30Hz, all drifting relative to one another.


**Physical AI** requires synchronous, dense, and tabular data. Models expect fixed-size tensors arriving at a constant frequency (e.g., a batch of state vectors at exactly 50Hz).

Mosaico’s [ML module](https://docs.mosaico.dev/SDK/bridges/ml/) automates this tedious *data plumbing*. It ingests raw, unsynchronized data and transforms it on the fly into the aligned, flattened formats ready for model training, eliminating the need for massive intermediate CSV files.

## What you'll find
This repo contains both the Python SDK (`mosaico-sdk-py`) and the Rust backend (`mosaicod`). We have chosen to keep the code in a monorepo configuration to simplify the testing and reduce compatibility issues.

Mosaico takes a strictly code-first approach. 
We didn't want to force you to learn yet another SQL-like sublanguage just to move data around. 
Instead, we provide native SDKs (starting with [Python](https://docs.mosaico.dev/SDK)) so you can query and upload data using the programming language you are already comfortable with.

Under the hood, the system operates on a standard client-server model. 
The server daemon, [`mosaicod`](https://docs.mosaico.dev/daemon), acts as the central hub that takes care of the heavy lifting, like data conversion, compression, and organized storage. 
On the other side, the client SDK is what you actually import into your scripts; it manages the communication logic and abstracts away the implementation details to keep your API usage stable, even as the platform evolves in the background.

## Documentation
For a comprehensive description, please visit our [documentation](https://docs.mosaico.dev). If you are building with AI, you can find specialized technical guides in the [Agent-ready](https://docs.mosaico.dev/agents_docs) section.

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
