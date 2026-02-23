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

## The Data Platform for Robotics & Physical AI

Mosaico is a *blazing-fast* data platform designed to bridge the gap between Robotics and Physical AI. It streamlines data management, compression, and search by replacing monolithic files with a structured archive powered by Rust and Python.

## What you'll find
This repo contains both the Python SDK (`mosaico-sdk-py`) and the Rust backend (`mosaicod`). We have chosen to keep the code in a monorepo configuration to simplify the testing and reduce compatibility issues.

Mosaico takes a strictly code-first approach. 
We didn't want to force you to learn yet another SQL-like sublanguage just to move data around. 
Instead, we provide native SDKs (starting with [Python](https://docs.mosaico.dev/SDK)) so you can query and upload data using the programming language you are already comfortable with.

Under the hood, the system operates on a standard client-server model. 
The server daemon, [`mosaicod`](https://docs.mosaico.dev/daemon), acts as the central hub that takes care of the heavy lifting, like data conversion, compression, and organized storage. 
On the other side, the client SDK is what you actually import into your scripts; it manages the communication logic and abstracts away the implementation details to keep your API usage stable, even as the platform evolves in the background.

## Documentation
For full documentation, see our [Documentation](https://docs.mosaico.dev).

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
