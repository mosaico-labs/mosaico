# Setup 

The SDK is currently available via source distribution. We use [Poetry](https://python-poetry.org/) for robust dependency management and packaging.

## Prerequisites

* **Python:** Version **3.13** or newer is required.
* **Poetry:** For package management.

### Install Poetry

If you do not have Poetry installed, use the official installer:

```bash
curl -sSL https://install.python-poetry.org | python3 -
```

Ensure the poetry binary is in your path by verifying the version:

```bash
poetry --version
```

## Install SDK

Clone the repository and navigate to the SDK directory:

```bash
cd mosaico/mosaico-sdk-py
```

Install the dependencies. This will automatically create a virtual environment and install all required libraries (PyArrow, NumPy, ROSBags, etc.):

```bash
poetry install
```

### Activate Environment

You can spawn a shell within the configured virtual environment to work interactively:

```bash
eval $(poetry env activate)
```

Alternatively, you can run one-off commands without activating the shell:

```bash
poetry run python any_script.py
```
