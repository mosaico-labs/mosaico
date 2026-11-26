---
title: LeRobot
description: LeRobot-Mosaico Bridge
---

The **LeRobot Bridge** module is the bidirectional gateway between [LeRobot](https://github.com/huggingface/lerobot) datasets and the Mosaico Data Platform: **ingesting** a LeRobot v3.0 dataset folder into Mosaico, one sequence per episode (via `LeRobotInjector`), and **exporting** Mosaico sequences back out as a LeRobot v3.0 dataset folder, one episode per sequence (via `LeRobotSequencesExtractor`). The dataset is read and written directly with `pyarrow`: the `lerobot` package is not needed.

!!! info "API-Keys"
    When the connection is established via the authorization middleware (i.e. using an [API-Key](../client.md#2-authentication-api-key)), the LeRobot Ingestion employs the mosaico [Writing Workflow](../handling/writing.md), which is allowed only if the key has the `write` permission. The LeRobot Export only reads sequences, and needs at least the `read` permission.

!!! warning "Supported datasets"
    Only the LeRobot dataset format `v3.0` is supported: a dataset whose `codebase_version` in `meta/info.json` is anything else is refused.

    Camera data is ingested only when it is recorded as images (`dtype: image`, embedded in the data files). Video features (`dtype: video`, stored under `videos/`) are not supported yet: they are skipped with a warning, so a dataset recorded with video loses its camera data, and only its other features are ingested. LeRobot records cameras as video by default: to ingest them, record the dataset as images (`--dataset.video=false` with `lerobot-record`, or `use_videos=False` in `LeRobotDataset.create()`).

    The export only reads sequences ingested by the LeRobot Bridge, because it rebuilds the dataset from their `_lerobot_` metadata: any other sequence is dropped with a warning (see [Reading Back the Sequences](#reading-back-the-sequences-mosaicotolerobotloader)). A sequence ingested from a dataset recorded with video holds no camera data, so its exported episode has none either.

## Architecture

The module is composed of collaborating components that handle both directions of the pipeline, LeRobot dataset → Mosaico (ingestion) and Mosaico → LeRobot dataset (export), from dataset and server access down to the shared adaptation layer.

### The Loaders (`LeRobotLoader` & `MosaicoToLeRobotLoader`)

Each direction has its own loader:

* **`LeRobotLoader`** (ingestion) reads the metadata of a dataset from its `meta/` folder, and streams each episode from its data file.
* **`MosaicoToLeRobotLoader`** (export) is the mirror image: it rebuilds the metadata of a dataset from the [`_lerobot_` metadata](#metadata-reserved-keys-custom-fields) stored on the sequences at ingestion time, and streams each episode back out of its sequence.

#### Reading a Dataset Folder (`LeRobotLoader`)

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

#### Reading Back the Sequences (`MosaicoToLeRobotLoader`)

**`MosaicoToLeRobotLoader`** reads the `_lerobot_` metadata of each sequence in `sequence_names`, in that order: the `info`, the episode `length` and the episode `tasks`. The `info` of the first accepted sequence becomes the `info` of the exported dataset. Each next sequence only has its `info` checked against it, and its episode is appended after the previous ones.

A sequence is **dropped** with a warning, and the export goes on without it, if:

* it does not exist;
* it has no topics;
* its `_lerobot_` metadata is missing or malformed (e.g. a sequence not ingested by the LeRobot Bridge);
* it is missing a topic its episode is rebuilt from: `/timeseries`, or the topic of an image feature of its `info`;
* it was already given earlier in the list;
* its `info` is not compatible with the reference one: the features differ (key, `dtype`, `shape` or `names`), or the `fps` or the `robot_type` differ.

Sequences ingested from different datasets can therefore be exported together, as long as they record the same features at the same frame rate with the same robot. The video features are dropped from every `info` before the comparison, with a warning.

The accepted episodes are renumbered from `0`, in the order of the sequences. Iterating over the loader yields one `LeRobotEpisodeStreamer` per accepted sequence, the mirror of `LeRobotEpisodeLoader`: it streams the episode back out of its topics.

### The Orchestrator (`LeRobotInjector`)

`LeRobotInjector` is the entry point for embedding LeRobot ingestion in a Python application or script. It is driven by a `LeRobotInjectionConfig`, and orchestrates the [`LeRobotLoader`](#reading-a-dataset-folder-lerobotloader) (dataset access), the [`LeRobotReader`](#the-adaptation-layer-lerobotreader-lerobotwriter) (data adaptation) and the `MosaicoClient` (network transmission).

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

### The Adaptation Layer (`LeRobotReader` & `LeRobotWriter`)

The two directions share one mapping between data columns and topics, applied by a component on each side:

* **`LeRobotReader`** (ingestion) turns the data columns of an episode into the batches pushed with `TopicWriter.push_batch`. Each batch is as large as possible within the maximum batch size accepted by the server.
* **`LeRobotWriter`** (export) is the mirror image: it turns the topics of each sequence back into the data columns of the dataset, with the types LeRobot writes them with (see [Rebuilding the Data Columns](#rebuilding-the-data-columns)).

There are no adapters to register: the topic and the ontology of a column follow from the type of its feature.

Inside each sequence, the data columns are split into topics:

| Data column | Mosaico topic | Ontology |
| :--- | :--- | :--- |
| Each image feature (e.g. `observation.images.front`) | Its own topic, with dots turned into slashes (e.g. `/observation/images/front`) | `CompressedImage` |
| Every other feature (e.g. `action`, `observation.state`, `task_index`) | `/timeseries`, all together | `Unmodeled`, with tag `LeRobotTimeseries` |

The `timestamp` column gives the time of every message: LeRobot stores it in seconds from the start of the episode, and the bridge converts it to integer nanoseconds, rounded. The timestamps of a sequence therefore start at `0`, not at the wall-clock time of the recording.

* **Images** are pushed as they are stored in the data files, without decoding them: `data` holds the image file content, `format` the lowercase extension of the image file name (e.g. `png`), and `header` is null. A frame without an image gets empty `data`.
* **Timeseries** are pushed as a single struct per frame. The Mosaico server rejects field names containing `.`, so dotted feature names are nested: `observation.state` and `observation.force` become the fields `state` and `force` of the struct `observation`. A name that is both a feature and the prefix of another one (e.g. `observation` and `observation.state`) cannot be nested, and the dataset cannot be opened.

#### Rebuilding the Data Columns

On export, `LeRobotWriter` applies the same mapping in reverse. Each slice streamed by the loader becomes a row group of the data file, so an episode is never held in memory as a whole.

| Data column | Rebuilt from |
| :--- | :--- |
| `timestamp` | The message timestamps, from integer nanoseconds back to seconds. |
| `index`, `frame_index`, `episode_index` | Generated: the position of each frame in the exported dataset (see [Columns That Are Not Ingested](#columns-that-are-not-ingested)). |
| `task_index` | The `task` field of `/timeseries` (see [Tasks](#tasks-task_index-becomes-task-and-back)). |
| Each image feature | Its own topic: `data` becomes the image bytes, and the path is rebuilt as LeRobot names embedded images, `frame-<frame index>.<format>` (e.g. `frame-000012.png`). A frame ingested with empty `data` is null. |
| Every other feature | Its field in `/timeseries`, un-nested: the field `state` of the struct `observation` becomes `observation.state`. |

The episodes are written to a single data file and their metadata to a single episodes file (chunk `0`, file `0`), at the paths given by the `data_path` template of the reference `info`.

#### Columns That Are Not Ingested

The position of each frame (`index`, `frame_index` and `episode_index`) is not pushed as data. These columns are not offered to the column filter either, and their statistics are not stored. On export, they are generated again from the position of each frame in the exported dataset, so the episodes can be renumbered.

#### Tasks: `task_index` Becomes `task`, and Back

A task index only means something next to the `meta/tasks.parquet` of its own dataset. The bridge therefore replaces the `task_index` column with a `task` column holding the task text it refers to, as the last field of `/timeseries`. A task index not defined in `meta/tasks.parquet` fails the episode.

For example, with this `meta/tasks.parquet`:

| task | task_index |
| :--- | --- |
| `Pick up a sharpie` | `0` |
| `Place it in the cup` | `1` |

a frame with `task_index=1` is pushed to `/timeseries` with `task="Place it in the cup"`.

Because `task` is the name the bridge writes, a dataset that has a `task_index` column and also a feature named `task` (or nested under it, such as `task.id`) cannot be opened. Exclude one of them with `column_names`: `"!task_index"` keeps your `task` feature (the episode tasks stay in the [sequence metadata](#metadata-reserved-keys-custom-fields)), while `"!task"` or `"!task.*"` keeps `task_index`. A dataset with a `task` feature and no `task_index` is ingested as is.

On export, the tasks of the exported dataset are the distinct `tasks` of its episodes, in the order they first appear: this order is their task index in the new `meta/tasks.parquet`. The `task` of each frame is turned back into its index in that list. A task index of the source dataset is never reused, so episodes from different datasets get consistent task indexes.

For example, exporting the sequences `sharpie_episode_000003` (episode `tasks`: `Place it in the cup`) and `apple_episode_000000` (episode `tasks`: `Grasp an apple`, `Place it in the cup`), in this order, writes this `meta/tasks.parquet`:

| task | task_index |
| :--- | --- |
| `Place it in the cup` | `0` |
| `Grasp an apple` | `1` |

`Place it in the cup` had task index `1` in its source dataset (see the ingestion example above) and gets `0`, the same index for the frames of both episodes.

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

On export, `LeRobotSchemaMetadata.restore` reverses these transformations, and the [export loader](#reading-back-the-sequences-mosaicotolerobotloader) builds the new `meta/` folder from them:

* `info` becomes the new `info.json`. Its totals (`total_episodes`, `total_frames`, `total_tasks`) and its `splits` (a single `train` split over every episode) are computed for the exported episodes, and the video features are left out.
* `episode.length` and `episode.tasks` give the length and the tasks of each exported episode.
* `episode.stats` is not reused, because it describes the source dataset. The statistics of each exported episode are computed from the data file written, as LeRobot computes them (images are decoded and downsampled), and `meta/stats.json` is combined from them. They can therefore differ slightly from those of the source dataset.

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

### The Extractor (`LeRobotSequencesExtractor`)

**`LeRobotSequencesExtractor`** runs the ingestion pipeline in reverse: it reads back the sequences created by [`LeRobotInjector`](#the-orchestrator-lerobotinjector) and writes them as a LeRobot v3.0 dataset folder, one episode per sequence, using the `_lerobot_` metadata recorded at ingestion time to rebuild the `meta/` folder. It is the entry point for exporting from a Python application or script. It is driven by a `LeRobotExtractorConfig`, and orchestrates the `MosaicoClient` (network transmission), the [`MosaicoToLeRobotLoader`](#reading-back-the-sequences-mosaicotolerobotloader) (sequence access) and the [`LeRobotWriter`](#the-adaptation-layer-lerobotreader-lerobotwriter) (data adaptation).

#### Core Workflow Execution: `run()`

1. **Output Check**: before connecting, checks the dataset folder, `saving_path / dataset_name`. If it exists and `overwrite` is `False`, `run()` raises a `FileExistsError`. If it exists, is not empty and is not a LeRobot dataset (a folder holding `meta/info.json`, `meta/episodes/` and `data/`), `run()` raises a `ValueError` even with `overwrite`.
2. **Handshake**: connects to the Mosaico server and resolves the sequences (see [Reading Back the Sequences](#reading-back-the-sequences-mosaicotolerobotloader)). If no sequence can be exported, `run()` raises a `ValueError`.
3. **Data**: streams each episode from its sequence and writes it to the data file, in episode order.
4. **Statistics**: computes the statistics of each episode from the data file written, then the dataset-wide statistics from those of the episodes.
5. **Metadata**: writes the `meta/` folder.

The dataset is written to a hidden temporary folder in `saving_path`, and moved to the dataset folder only once complete. A failure, or an interruption with `Ctrl+C`, deletes the temporary folder and leaves an existing dataset folder untouched.

!!! warning "Killed runs leave a temporary folder"
    A process killed before it can clean up (e.g. with `kill -9`, or by the system when it runs out of memory) leaves the temporary folder behind, in `saving_path`. It is hidden: list it with `ls -a` and delete it by hand.

Unlike the ingestion, an episode that fails to be written stops the whole export. This happens when the topics of its sequence do not hold the same timestamps, when their number of rows differs from the episode `length`, or when a frame holds a task that is not among the `tasks` of the exported episodes. Only the sequences dropped by the loader are skipped.

`run()` returns an `ExtractionStatus` (from `mosaicolabs.bridges.lerobot.sequences_extractor`):

* `COMPLETED` once every sequence is exported.
* `PARTIAL` if some sequences were dropped: their names are in `LeRobotSequencesExtractor.dropped_sequences`, and are logged again at the end of the run.

Errors are not a status: `run()` raises them.

#### Configuring the Export

`LeRobotExtractorConfig` shares its connection fields with the other bridges (`host`, `port`, `mosaico_api_key`, `tls_cert_path`, `enable_tls`, `log_level`). The fields specific to the LeRobot Export are:

| Field | Default | Description |
| :--- | :--- | :--- |
| `saving_path` | required | Folder where the dataset folder is created. |
| `sequence_names` | required | The sequences to export, one episode each, in episode order. |
| `dataset_name` | `"LeRobotDataset"` | Name of the dataset folder, created under `saving_path`. |
| `overwrite` | `False` | Replace the dataset folder if it already exists. Only an empty folder or a LeRobot dataset is replaced. |

#### Practical Example: Programmatic Usage

```python
from pathlib import Path

from mosaicolabs.bridges.lerobot.sequences_extractor import (
    ExtractionStatus,
    LeRobotExtractorConfig,
    LeRobotSequencesExtractor,
)


def run_extraction():
    config = LeRobotExtractorConfig(
        # The dataset is written to exports/pusht
        saving_path=Path("exports"),
        dataset_name="pusht",
        # Episode 0 and episode 1 of the exported dataset, in this order
        sequence_names=["pusht_episode_000000", "pusht_episode_000001"],
        # Replace a previous export of the same dataset
        overwrite=True,
        host="localhost",
        port=6726,
    )

    extractor = LeRobotSequencesExtractor(config)

    # Errors (existing output, no sequence to export, an episode not written) are raised
    try:
        status = extractor.run()
    except Exception as e:
        print(f"Extraction job failed: {e}")
        return

    if status is ExtractionStatus.PARTIAL:
        print(f"Sequences not exported: {extractor.dropped_sequences}")


if __name__ == "__main__":
    run_extraction()
```

#### CLI Usage

Sequences can be exported from the terminal with `mosaico extract lerobot`, which reaches the server through the active [connection profile](https://docs.mosaico.dev/cli/profiles). The full list of options can be retrieved by running `mosaico extract lerobot --help`, and is described in the [CLI reference](https://docs.mosaico.dev/cli/commands#lerobot-1).

The CLI writes the dataset to `<saving-path>/<name>`: `--saving-path` (default `.`) is the parent folder, and `--name` (default `LeRobotDataset`) is the name of the dataset folder.

```bash
# Basic Usage: writes ./LeRobotDataset, one episode per sequence, in the order given
mosaico extract lerobot pusht_episode_000000 pusht_episode_000001

# Advanced Usage: writes ./exports/pusht, replacing a previous export
mosaico extract lerobot \
  --saving-path ./exports \
  --name pusht \
  --overwrite \
  pusht_episode_000000 pusht_episode_000001
```

If any sequence is dropped, the command still exports every other sequence, then exits with code `1`.

!!! note "Development script"
    The SDK also installs the `mosaicolabs.lerobot_extractor` script, mainly meant for development. It takes the same options, except that the parent folder is given with `--lerobot-path`, which is required, instead of `--saving-path`. The connection is given with explicit flags (`--host`, `--port`, `--api-key`, `--tls-cert`, `--enable-tls`) instead of a profile. Run `mosaicolabs.lerobot_extractor -h` for details.

### Known Issues & Limitations

* **Video features**: skipped with a warning. Only images embedded in the data files are ingested, and the export writes no `videos/` folder.
* **Format version**: only LeRobot `v3.0` datasets are supported.
* **Feature shapes**: a non-image feature must be a scalar `(1,)`, a vector `(n,)` or an array of 2 to 5 dimensions; any other shape stops the dataset from being opened.
* **Frame position**: `index`, `frame_index` and `episode_index` are not stored as data (see [Columns That Are Not Ingested](#columns-that-are-not-ingested)).
* **Row size**: a single frame (e.g. one image) larger than the maximum batch size accepted by the server fails its episode.
* **Export output**: every episode goes to a single data file and a single episodes file, and the original image file names are not kept (they are rebuilt as `frame-<frame index>.<format>`).
* **Interrupted export**: a killed export leaves a hidden temporary folder next to the output folder (see [Core Workflow Execution](#core-workflow-execution-run_1)).
