# Online Documentation

This repository contains the source code and Markdown files for the official Mosaico documentation, hosted at [https://docs.mosaico.dev](https://docs.mosaico.dev).

## Setup and Installation

This project uses uv for dependency management and MkDocs to generate the static site. Ensure you have Python 3.11 or higher and [uv](https://docs.astral.sh/uv/getting-started/installation/) (0.11.23 or newer) installed on your system.

To set up the environment, clone the repository and run:

```bash
# From the repository root
cd docs/py
uv sync --locked
```
## Run locally

To start a local instance of the documentation run
```bash
uv run --locked mkdocs serve
```

## Local Development

To start the local development server and preview your changes in real-time, execute:

```bash
uv run --locked mkdocs serve --livereload -w ../../mosaico-sdk-py/src
```

The documentation will be available for viewing at [http://localhost:8000](https://localhost:8000).

## Deployment

The documentation is automatically built and deployed to [https://docs.mosaico.dev](https://docs.mosaico.dev) through GitHub Actions upon merging changes into the `main` branch.
Ensure all new pages are registered in the navigation section of the configuration file.
