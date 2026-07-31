---
title: Setup
sidebar_position: 2
description: "How to download, install, and start mosaicod. Covers binary distribution via GitHub Releases, platform support, initial configuration, and verifying the daemon is running."
---

## Precompiled Binaries

Precompiled binaries for `mosaicod` are available for several platforms and can be downloaded directly from the [GitHub Releases page](https://github.com/mosaico-labs/mosaico/releases).

## Running with Containers

For rapid prototyping, we provide a standard compose configuration. This creates an isolated network environment containing the `mosaicod` server and its required PostgreSQL database.

```yaml title="compose.yml", {24,34-37,50}
name: "mosaico"

# Shared configuration for every `mosaicod` service. The server and the
# cleanup routine run the same image against the same database, object store
# and network, so we declare that common setup once here and merge it into
# each service below with `<<: *mosaicod-common`. This keeps the two services
# in sync and avoids duplicating the environment variables.
x-mosaicod-common: &mosaicod-common
  # There are other available predefined tags that you can use.
  image: ghcr.io/mosaico-labs/mosaicod:latest
  networks:
    - mosaico
  # We configure the daemon to use the local filesystem for storage, mounted
  # to the `/data` directory in the container. This persists data across
  # container restarts and makes it easy to access from the host machine.
  # Additional environment variables can be set here to configure the daemon's
  # behavior.
  environment:
    MOSAICOD_DB_URL: postgresql://postgres:password@db:5432/mosaico
    MOSAICOD_STORE_ENDPOINT: file:///
    MOSAICOD_STORE_BUCKET: data
  volumes:
    - mosaico-data:/data
  depends_on:
    database:
      condition: service_healthy

services:

  database:
    image: postgres:18
    container_name: postgres
    hostname: db
    environment:
      POSTGRES_USER: postgres
      POSTGRES_PASSWORD: password 
      POSTGRES_DB: mosaico
    networks:
      - mosaico
    volumes:
      - pg-data:/var/lib/postgresql
    healthcheck:
      test: ["CMD-SHELL", "pg_isready -U  postgres"]
      interval: 5s
      timeout: 5s
      retries: 5

  # The daemon itself: serves the API and handles read/write requests.
  # Inherits the shared image, environment, volumes, network and database
  # dependency from `x-mosaicod-common`.
  mosaicod-server:
    <<: *mosaicod-common
    container_name: mosaicod
    # Here you can list any additional command line options for `mosaicod`.
    command: |
      server --host 0.0.0.0 --port 6726 --log-level info
    ports:
    # Remove `127.0.0.1` to expose this service to external networks.
    # By default, this configuration restricts access to the local machine for security reasons.
    # If you need to access the server from other machines on the network,
    # you can modify the port mapping to allow external connections.
      - "127.0.0.1:6726:6726"

  # Background maintenance: periodically deletes obsolete files from the store.
  # It shares the same image and configuration as the server (via
  # `x-mosaicod-common`) but runs the `cleanup` subcommand instead of `server`.
  mosaicod-cleanup:
    <<: *mosaicod-common
    container_name: mosaicod-cleanup
    # `--time-interval` is the delay, in seconds, between two cleanup runs
    #   (`0`, the default, runs a single cleanup and then exits; any value > 0
    #   loops forever). Here we run the routine once per hour (3600s).
    # `--retention-duration` is how long, in seconds, an obsolete file is kept
    #   before being permanently deleted (default: 86400, i.e. one day).
    command: |
      cleanup --time-interval 3600 --retention-duration 86400

volumes:
  pg-data:
  mosaico-data:

networks:
  mosaico:
```

This configuration provisions both Postgres and mosaicod within a private network. Only the daemon instance is exposed to the host.

:::warning
    In this basic prototyping setup, TLS and API key management are disabled.

    The port mapping is restricted to `127.0.0.1`. If you need to access this from an external network, consider configuring `mosaicod` to [enable TLS](tls.md) or use a reverse proxy to handle SSL termination.
:::

PostgreSQL data is persisted in the `pg-data` volume. To remove it along with the containers, run:

```bash
docker compose down -v
```

### Container tags

`mosaicod` images provides four types of container tags.

| Tag Type | Description |
| :--- | :--- |
| **`latest`** | Always points to the most recent official stable release. This is the default choice for general use. |
| **`x.y`** | Points to the latest minor release, like `0.3`. Use this to receive critical patches within a specific version series while avoiding major breaking changes. |
| **`x.y.z`** | Points to a specific, immutable stable release, like `0.3.12`. This is the recommended choice for production environments requiring maximum consistency. |
| **`nightly`** | Updated daily with the latest code from the main branch. Use this to test new features and bug fixes before they are officially released. |

## Building from Source

While Docker images are available for each release, you can compile `mosaicod` from source if you need a specific version not available as a pre-built image. Building from source requires a Rust toolchain. The project uses `sqlx` for compile-time query verification, which normally requires a live database connection. However, Mosaico supports a simpler offline build mode that uses cached query metadata from the `.sqlx` directory, removing the need for a database during compilation.

### Offline Build

You can run a offline build using cached sqlx queries with a single command.

```bash
SQLX_OFFLINE=true cargo build --release
```

### Online Build

If you need to modify the database schema, a running PostgreSQL instance is required. This allows `sqlx` to verify queries against a live database during compilation. You can use the provided Docker Compose file in `docker/devel` which sets up an instance of [MinIO](https://www.min.io/) and a PostgreSQL database.

First, start the development environment:
```bash
cd docker/devel

# Start the services in the background
docker compose up -d

# To stop and remove the volumes (which clears all data), run:
docker compose down -v
```

Apply database migrations to the running PostgreSQL instance. This ensures that the database schema is up-to-date and allows `sqlx` to verify queries during compilation.

Next, from the root of the `mosaicod` workspace, install the necessary tools, configure the environment, and run the build.
```bash
cd mosaicod

# Install the SQLx command-line tool
cargo install sqlx-cli

# Copy the development environment variables for the database connection
cp env.devel .env

# Apply the database migrations
cd crates/mosaicod-db
cargo sqlx migrate run 

# And finally you can build mosaicod 
cargo build --release --bin mosaicod
```

## Configuration

The server supports S3-compatible object storage by default but can be configured for local storage via command line options.

### Database

Mosaico requires a connection to a running **PostgreSQL** instance, which is defined via the `MOSAICOD_DB_URL` environment variable.

### Remote Storage Configuration

For production deployments, `mosaicod` should be configured to use an S3-compatible object store (such as AWS S3, Google Cloud Storage, Hetzner Object Store, etc) for durable, long-term storage. This is configured setting the proper [environment variables](env/#store) for your object store provider.

### Local Storage Configuration

This command will start a `mosaicod` instance using the local filesystem as storage layer.

```sh
export MOSAICOD_STORE_ENDPOINT=file:///some/local/directory
export MOSAICOD_STORE_BUCKET=bucket-name
```

and run `mosaicod server`.

## Advanced

### Bare Metal Deployment

When running `mosaicod` on a bare metal server, a few tuning knobs can significantly improve resource efficiency and network throughput.

#### Limit Tokio Worker Threads

By default, the async runtime used by `mosaicod` spawns one worker thread per logical CPU core. On machines with many cores but limited memory, this can lead to excessive memory usage and unnecessary thread scheduling overhead. Use `TOKIO_WORKER_THREADS` to cap the thread pool to a number appropriate for your workload:

```sh
export TOKIO_WORKER_THREADS=8
```

Start with the number of physical cores and adjust based on observed CPU utilisation.

#### Tune Allocator Memory Release

`mosaicod` uses [mimalloc](https://github.com/microsoft/mimalloc) as its allocator. By default, mimalloc defers returning freed memory pages to the OS for several seconds. On a long-running server this can make resident memory appear higher than the actual working set. Set the following variables to release pages back to the OS immediately after they are freed:

```bash
export MIMALLOC_PURGE_DELAY=0
export MIMALLOC_PURGE_DECOMMITS=1
```

#### Enable BBR Congestion Control

On high-throughput bare metal servers, switching the TCP congestion control algorithm from the [default](https://en.wikipedia.org/wiki/CUBIC_TCP) (`cubic`) to [BBR](https://www.ietf.org/archive/id/draft-cardwell-iccrg-bbr-congestion-control-01.html) reduces latency and improves throughput, especially under load. BBR measures actual bottleneck bandwidth instead of reacting to packet loss, so it keeps the link fully utilised without overfilling buffers, this matters most when streaming large payloads over high-latency or mildly lossy connections. Run the following commands as root to enable it system-wide:

```shell
# Load the BBR kernel module
modprobe tcp_bbr

# Set BBR as the active congestion control algorithm
sysctl -w net.ipv4.tcp_congestion_control=bbr

# Enable the FQ packet scheduler, which BBR requires
sysctl -w net.core.default_qdisc=fq
```

To make the settings persistent across reboots, add them to `/etc/sysctl.d/99-bbr.conf`:

```ini title="/etc/sysctl.d/99-bbr.conf"
net.core.default_qdisc=fq
net.ipv4.tcp_congestion_control=bbr
```

