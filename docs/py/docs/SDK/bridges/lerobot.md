---
title: LeRobot
description: LeRobot-Mosaico Bridge
---

The **LeRobot Bridge** module connects [LeRobot](https://github.com/huggingface/lerobot) datasets and the Mosaico Data Platform. It ingests a LeRobot v3.0 dataset folder into Mosaico, **one sequence per episode**, through `LeRobotInjector`. The dataset is read directly with `pyarrow`: the `lerobot` package is not needed.

This page covers ingestion.

!!! info "API-Keys"
    When the connection is established via the authorization middleware (i.e. using an [API-Key](../client.md#2-authentication-api-key)), the LeRobot Ingestion employs the mosaico [Writing Workflow](../handling/writing.md), which is allowed only if the key has the `write` permission.

!!! warning "Supported datasets"
    Only the LeRobot dataset format `v3.0` is supported: a dataset whose `codebase_version` in `meta/info.json` is anything else is refused.

    Camera data is ingested only when it is recorded as images (`dtype: image`, embedded in the data files). Video features (`dtype: video`, stored under `videos/`) are not supported yet: they are skipped with a warning, so a dataset recorded with video loses its camera data, and only its other features are ingested. LeRobot records cameras as video by default: to ingest them, record the dataset as images (`--dataset.video=false` with `lerobot-record`, or `use_videos=False` in `LeRobotDataset.create()`).

## Architecture

### The Loader (`LeRobotLoader`)

**`LeRobotLoader`** is the abstraction layer over the dataset folder. It reads the folder with `pyarrow`, without the `lerobot` package, and opens it lazily, on first access. It reads:

* **`meta/info.json`** (required): the format version, the frame rate (`fps`), the features, the number of episodes and the template of the data file paths.
* **`meta/episodes/**/*.parquet`** (required): one row per episode, with its `length`, its `tasks`, the data file holding it and, when present, its per-feature statistics.
* **`meta/tasks.parquet`** (required): the dataset tasks, in task index order.
* **`data/`**: the Parquet files holding the frames. A file can hold several episodes: only the rows of the episode being ingested are kept.

The per-episode statistics (the `stats/<feature>/<stat>` columns of `meta/episodes`) are stored as queryable sequence metadata. The dataset-wide statistics of `meta/stats.json` are not stored: the file is optional, but if present it must hold a JSON object, otherwise the dataset cannot be opened (see [Metadata: Reserved Keys & Custom Fields](#metadata-reserved-keys-custom-fields)).

Iterating over the loader yields one `LeRobotEpisodeLoader` per episode, in episode index order. Each episode becomes the sequence `<sequence_name>_episode_<episode index>`, with the episode index zero-padded to 6 digits (e.g. `pusht_episode_000042`). If `sequence_name` is empty, the name of the dataset folder is used. The episode loader streams its episode as batches: the `/timeseries` batches first, then the batches of each camera, one camera at a time. Only the data file holding the episode is opened, and only the columns of the topic being read: the images are never loaded while the timeseries are read.

* **Responsibilities:** reading the `meta/` and `data/` folders, column filtering (supporting glob patterns like `observation.images.*`), and splitting the dataset into episodes.
* **Error Handling:** columns excluded by `column_names` are reported as `Filtered` in the dry-run report, and video features are skipped with a warning. A dataset that cannot be opened (a missing `meta/` file, an unsupported version, malformed metadata) raises an `IOError`, and a filter that leaves no column raises a `RuntimeError`: both stop the run. A read error inside an episode is not raised by the loader: it is returned with the batch, the remaining batches of that topic are skipped, and the injector fails that episode only.

!!! note "Column names and topic names"
    `column_names` refers to the LeRobot feature names (e.g. `observation.images.front`), while `topic_metadata` and `topics_on_error` refer to the Mosaico topic names (e.g. `/observation/images/front`, `/timeseries`).

### The Orchestrator (`LeRobotInjector`)

`LeRobotInjector` is the entry point for embedding LeRobot ingestion in a Python application or script. It is driven by a `LeRobotInjectionConfig`, and orchestrates the [`LeRobotLoader`](#the-loader-lerobotloader) (dataset access), the [`LeRobotReader`](#the-adaptation-layer-lerobotreader) (data adaptation) and the `MosaicoClient` (network transmission).

#### Core Workflow Execution: `run()`

1. **Handshake**: connects to the Mosaico server and opens the dataset, resolving which columns are ingested.
2. **Episode Checks**: checks every episode before the first upload (see [Episode Checks](#episode-checks)). An episode that fails a check is skipped, and its sequence is never created.
3. **Upload**: for each remaining episode, creates its sequence with its metadata, then pushes the `/timeseries` batches followed by the batches of each camera, one camera at a time. Each topic is created on its first batch.
4. **Summary**: once every episode is processed, logs the list of the episodes that failed.

An episode that fails to upload does not stop the ingestion. Its sequence applies the `on_error` policy, which by default (`SessionLevelErrorPolicy.Delete`) deletes the partial sequence, and the next episode is processed.

`run()` returns an `InjectionStatus` (from `mosaicolabs.bridges.base_injector`):

* `COMPLETED` once every episode is ingested.
* `PARTIAL` if some episodes were skipped or failed: their indexes are in `LeRobotInjector.failed_episodes`.
* `CANCELLED` if the run was interrupted (`KeyboardInterrupt`).
* `DRY_RUN` in [dry-run](#configuring-the-ingestion) mode.

If every episode fails, nothing is ingested and `run()` raises a `RuntimeError`. A fatal error outside an episode (connecting to the server, opening the dataset, no column left after filtering) is logged and re-raised.

#### Episode Checks

Before any upload, the `timestamp` column of every episode is checked, with a tolerance of `1e-4` seconds (the LeRobot default):

* no null or `NaN` timestamp;
* timestamps strictly increasing;
* consecutive timestamps `1 / fps` apart;
* the first timestamp at `0`.

An episode whose data file is missing, or whose number of rows in that file differs from its `length` in `meta/episodes`, is reported as unreadable. Each skipped episode is logged as a warning with the problems found, and is counted among the failed episodes.

#### Configuring the Ingestion

`LeRobotInjectionConfig` shares its connection and error-handling fields with the other bridges (`host`, `port`, `mosaico_api_key`, `tls_cert_path`, `enable_tls`, `topics_on_error`, `log_level`). The fields specific to, or with a different meaning in, the LeRobot Bridge are:

| Field | Default | Description |
| :--- | :--- | :--- |
| `file_path` | required | Root folder of the LeRobot v3.0 dataset. |
| `sequence_name` | `""` | Prefix of the sequence names. If empty, the name of the dataset folder is used. |
| `column_names` | `None` (all columns) | Glob patterns on the image and timeseries features of `meta/info.json` (e.g. `"observation.images.*"`). A pattern starting with `!` excludes. Patterns are evaluated in order; with exclusions only, every other column is kept. |
| `metadata` | `{}` | Metadata attached to every sequence, next to the `_lerobot_` block. |
| `topic_metadata` | `None` | Mapping of exact Mosaico topic name (e.g. `/timeseries`, `/observation/images/front`) to metadata for that topic. |
| `on_error` | `SessionLevelErrorPolicy.Delete` | What happens to the sequence of an episode that fails to upload: `Delete` it or `Report` the error and keep it. |
| `dry_run` | `False` | Report the columns, topics and sequences that would be ingested and the episodes that would be skipped, without connecting to the server. |

`topics_on_error` takes Mosaico topic names as keys, like `topic_metadata`. With `TopicLevelErrorPolicy.Raise` (the default) a write error fails the episode; with `Ignore` or `Finalize` the episode completes, but its sequence may miss some batches.

`update_if_exists` and `serialization_formats` are not used by the LeRobot Bridge and are ignored.

#### Practical Example: Programmatic Usage

```python
from pathlib import Path

from mosaicolabs.bridges.base_injector import InjectionStatus
from mosaicolabs.bridges.lerobot import LeRobotInjectionConfig, LeRobotInjector
from mosaicolabs.enum import SessionLevelErrorPolicy


def run_injection():
    config = LeRobotInjectionConfig(
        file_path=Path("datasets/pusht"),
        sequence_name="pusht",
        # Every column except the images
        column_names=["!observation.images.*"],
        metadata={"operator": "John"},
        topic_metadata={"/timeseries": {"robot": "so100"}},
        # Keep the partial sequence of a failed episode, to inspect it
        on_error=SessionLevelErrorPolicy.Report,
        host="localhost",
        port=6726,
    )

    injector = LeRobotInjector(config)

    # Fatal errors (connection, unreadable dataset, every episode failed) are raised
    try:
        status = injector.run()
    except Exception as e:
        print(f"Injection job failed: {e}")
        return

    if status is InjectionStatus.PARTIAL:
        print(f"Episodes not ingested: {injector.failed_episodes}")


if __name__ == "__main__":
    run_injection()
```

### The Adaptation Layer (`LeRobotReader`)

**`LeRobotReader`** turns the data columns of an episode into the batches pushed with `TopicWriter.push_batch`. There are no adapters to register: the topic and the ontology of a column follow from the type of its feature. Each batch is as large as possible within the maximum batch size accepted by the server.

Inside each sequence, the data columns are split into topics:

| Data column | Mosaico topic | Ontology |
| :--- | :--- | :--- |
| Each image feature (e.g. `observation.images.front`) | Its own topic, with dots turned into slashes (e.g. `/observation/images/front`) | `CompressedImage` |
| Every other feature (e.g. `action`, `observation.state`, `task_index`) | `/timeseries`, all together | `Unmodeled`, with tag `LeRobotTimeseries` |

The `timestamp` column gives the time of every message: LeRobot stores it in seconds from the start of the episode, and the bridge converts it to integer nanoseconds, rounded. The timestamps of a sequence therefore start at `0`, not at the wall-clock time of the recording.

* **Images** are pushed as they are stored in the data files, without decoding them: `data` holds the image file content, `format` the lowercase extension of the image file name (e.g. `png`), and `header` is null. A frame without an image gets empty `data`.
* **Timeseries** are pushed as a single struct per frame. The Mosaico server rejects field names containing `.`, so dotted feature names are nested: `observation.state` and `observation.force` become the fields `state` and `force` of the struct `observation`. A name that is both a feature and the prefix of another one (e.g. `observation` and `observation.state`) cannot be nested, and the dataset cannot be opened.

#### Columns That Are Not Ingested

The position of each frame (`index`, `frame_index` and `episode_index`) is not pushed as data. These columns are not offered to the column filter either, and their statistics are not stored.

#### Tasks: `task_index` Becomes `task`

A task index only means something next to the `meta/tasks.parquet` of its own dataset. The bridge therefore replaces the `task_index` column with a `task` column holding the task text it refers to, as the last field of `/timeseries`. A task index not defined in `meta/tasks.parquet` fails the episode.

Because `task` is the name the bridge writes, a dataset that has a `task_index` column and also a feature named `task` (or nested under it, such as `task.id`) cannot be opened. Exclude one of them with `column_names`: `"!task_index"` keeps your `task` feature (the episode tasks stay in the [sequence metadata](#metadata-reserved-keys-custom-fields)), while `"!task"` or `"!task.*"` keeps `task_index`. A dataset with a `task` feature and no `task_index` is ingested as is.

#### Metadata: Reserved Keys & Custom Fields

!!! info "Internal behavior"
    This section documents implementation detail useful for interpreting or querying ingested metadata, not something you need to configure to use the bridge.

Every sequence created by the bridge carries a reserved `_lerobot_` block, encapsulated by `LeRobotSchemaMetadata`. Topics carry no `_lerobot_` metadata. The block holds:

* `episode`: the `length`, `tasks` and `stats` of the episode, from its row in `meta/episodes`. Its indexes (episode index, data file location) are not stored. `stats` only covers the ingested features: the features excluded by `column_names`, the video features, `timestamp`, `index`, `frame_index`, `episode_index` and `task_index` are left out.
* `info`: the content of `meta/info.json`, without the features dropped on ingestion (the video features and the columns excluded by `column_names`).
* `source_file`: the name of the dataset folder.

The dataset-wide statistics of `meta/stats.json` are not stored: only the per-episode ones, in `episode.stats`.

The Mosaico server rejects metadata keys containing `.` and non-finite numbers, so the block is stored in a server-compatible form:

* the feature names in `episode.stats` are nested on `.`: the mean of `observation.state` is stored at `_lerobot_.episode.stats.observation.state.mean`;
* `NaN` and infinite values are stored as `null`;
* `info.features` is stored as a JSON string, so it cannot be queried field by field.

`_lerobot_` is fully reserved: the user `metadata` is applied first, then the `_lerobot_` block on top, so a `_lerobot_` key in `metadata` is overwritten. Every other key is user-owned. `topic_metadata` goes only to its topics.

!!! tip "Querying these keys"
    The fields of `_lerobot_` are ordinary metadata for the query engine: they can be queried with `QuerySequence().with_user_metadata()` like any other field (e.g. `QuerySequence().with_user_metadata("_lerobot_.episode.length", gt=100)`). See the [Query Workflow](../query.md) guide for the full API.

#### CLI Usage

LeRobot datasets can be injected from the terminal with the `mosaico` CLI (installed with `pip install "mosaicolabs[cli]"`), which reaches the server through the active [connection profile](https://docs.mosaico.dev/cli/profiles). The full list of options can be retrieved by running `mosaico inject lerobot --help`, and is described in the [CLI reference](https://docs.mosaico.dev/cli/commands#lerobot).

```bash
# Basic Usage: one sequence per episode, named pusht_episode_000000, pusht_episode_000001, ...
mosaico inject lerobot ./datasets/pusht --name pusht

# Advanced Usage: filtering columns and adding metadata
mosaico inject lerobot ./datasets/pusht \
  --name pusht \
  --column-names 'observation.*' --column-names '!observation.images.*' \
  --metadata ./metadata.json \
  --topic-metadata '{"/timeseries": {"operator": "John Doe"}}'

# Dry Run: report columns, topics, sequences and skipped episodes,
# without connecting to the server (no connection profile needed)
mosaico inject lerobot ./datasets/pusht --name pusht --dry-run
```

The CLI always uses the default `on_error` policy (`Delete`) and the default `topics_on_error` policy (`Raise`). If any episode is skipped or fails, the command still processes every other episode, then exits with code `1`.

!!! note "Development script"
    The SDK also installs the `mosaicolabs.lerobot_injector` script, mainly meant for development. It takes the same options, except that the connection is given with explicit flags (`--host`, `--port`, `--api-key`, `--tls-cert`) instead of a profile, `--column-names` takes several patterns after a single flag, and `--log` defaults to `WARNING`. It also adds the dataset folder name to the sequence metadata under the `lerobot_dataset` key. Run `mosaicolabs.lerobot_injector -h` for details.

### Known Issues & Limitations

* **Video features**: skipped with a warning. Only images embedded in the data files are ingested.
* **Format version**: only LeRobot `v3.0` datasets are supported.
* **Feature shapes**: a non-image feature must be a scalar `(1,)`, a vector `(n,)` or an array of 2 to 5 dimensions; any other shape stops the dataset from being opened.
* **Frame position**: `index`, `frame_index` and `episode_index` are not stored as data (see [Columns That Are Not Ingested](#columns-that-are-not-ingested)).
* **Row size**: a single frame (e.g. one image) larger than the maximum batch size accepted by the server fails its episode.
