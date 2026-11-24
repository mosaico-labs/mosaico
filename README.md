<div align="center">
  <a href="https://mosaico.dev">
    <picture>
      <source media="(prefers-color-scheme: dark)" srcset="https://mosaico.dev/ext/github/hero-dark.webp">
      <source media="(prefers-color-scheme: light)" srcset="https://mosaico.dev/ext/github/hero-light.webp">
      <img alt="Mosaico logo" src="https://mosaico.dev/ext/github/hero-light.webp" width="600px">
    </picture>
  </a>
</div>
<br/>

<p align="center">
  <a href="https://github.com/mosaico-labs/mosaico/pkgs/container/mosaicod"><img src="https://img.shields.io/github/v/tag/mosaico-labs/mosaico?color=orange&sort=semver&label=mosaicod&filter=v*.*.*" alt="mosaicod release" /></a>
  <a href="https://pypi.org/project/mosaicolabs"><img src="https://img.shields.io/pypi/v/mosaicolabs?color=blue" alt="PyPI" /></a>
  <a href="https://docs.mosaico.dev"><img src="https://img.shields.io/badge/-%20Documentation-%23bd38b4?style=flat&logo=readthedocs&logoColor=white&labelColor=gray" alt="documentation" /></a>
  <a href="https://discord.gg/mwQtFnsckE"><img src="https://shields.io/discord/1413199575442784266?color=%235865f2" alt="discord" /></a>
</p>

# Robot data, on demand.

Mosaico is an open-source data platform for robotics and Physical AI.
You ingest a recording once. From then on, you just ask for the signals and time windows you need.
Storage format, indexing and synchronization are Mosaico's job, not yours.

<p align="center">
  <img alt="Terminal: an NVIDIA recording is downloaded, ingested with the mosaico CLI and listed in the catalog" src="https://mosaico.dev/ext/github/terminal.webp" width="800">
</p>

## Streamlining data for Physical AI

If you work with robot data, this is probably familiar: you need ten seconds of IMU from a two-hour drive, and you end up parsing the whole `.bag` or `.mcap`.
Then, to train on it, you write another sync script and export another large CSV.
We ran into this often enough that we decided to fix it.

Mosaico stores the data differently.
Every topic is indexed and typed against an ontology, which means you can ask for one signal, one time window, or every moment the acceleration went above 5 m/s², and you get back exactly that.

<p align="center">
  <img src="https://mosaico.dev/ext/github/stack.webp" width="360" />
</p>

Sensors rarely share a clock: lidar at 10 Hz, IMU at 100 Hz, camera at 30 Hz.
Mosaico can line them up into fixed-rate tables that drop straight into a training loop.
There's no query language to learn: you work from native SDKs. Today Python, others are on the way.

## Data grows in layers, never overwritten

With files, adding data to a recording means producing a new file, and the old one and the new one can silently disagree.
In Mosaico a recording is a sequence, and every upload is a session: an immutable layer of topics added to that sequence.

<p align="center">
  <picture>
    <source media="(prefers-color-scheme: dark)" srcset="https://mosaico.dev/ext/github/sessions-dark.webp">
    <source media="(prefers-color-scheme: light)" srcset="https://mosaico.dev/ext/github/sessions-light.webp">
    <img alt="A sequence grows in sessions: each upload is an immutable layer of topics, and a failed update rolls back only its own session" src="https://mosaico.dev/ext/github/sessions-light.webp" width="800">
  </picture>
</p>

New labels, a recomputed odometry or a sensor added later go into a new session of the same sequence.
Earlier sessions are never touched, so every query on them returns the same result over time.
If an update goes wrong, only that session is rolled back.

There are no versions to pick between: the sequence simply grows.
See [updating a sequence](https://docs.mosaico.dev/learn/updating_sequences) for details.

## Quickstart

```bash
# Install the daemon (Linux and macOS; on Windows use WSL2)
curl -fsSL https://get.mosaico.dev | sh

# Install the SDK and the CLI, then connect
pip install "mosaicolabs[cli]"
mosaico profile add dev --no-interactive --host localhost

# Ingest your first recording: here r2b_galileo (about 470 MB) from the NVIDIA R2B Dataset 2024
curl -L -o r2b_galileo.mcap "https://api.ngc.nvidia.com/v2/resources/org/nvidia/team/isaac/r2bdataset2024/1/files?redirect=true&path=r2b_galileo/r2b_galileo_0.mcap"
mosaico inject ros r2b_galileo.mcap --name r2b_galileo --log warning

# List the sequences in your catalog
mosaico sequence ls
```

The CLI works the same for you and for your coding agent: plain commands, tables in the terminal, CSV and JSON Lines when piped, so either of you can explore and work on the data without writing code.

To try it without your own data, run `mosaicolabs.examples --help`.

## How it works

A Rust daemon, `mosaicod`, handles conversion, compression and storage.
The client SDKs (Python today, others in development) are what you import in your code.
Under the hood, a columnar, zero-copy format gives you direct access to any signal.
The full story is in the [architecture docs](https://docs.mosaico.dev/daemon).

<p align="center">
  <img src="https://mosaico.dev/ext/github/stack.svg" width="320" />
</p>

## Where to go next

The [docs](https://docs.mosaico.dev) cover everything this page skips.
If you work with a coding agent, point it at the [agent-ready version](https://docs.mosaico.dev/llms-full.txt).

We're a small team and we read everything, so come say hi on [Discord](https://discord.gg/mwQtFnsckE) or open a thread in [GitHub Discussions](https://github.com/mosaico-labs/mosaico/discussions).

## Cite us

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
