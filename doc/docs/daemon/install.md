# Setup


For rapid prototyping, we provide a Docker Compose configuration. This sets up a volatile environment that includes both the Mosaico server and a [PostgreSQL](https://www.postgresql.org/) database.

```bash
# Navigate to the quick start directory form the root folder
cd docker/quick_start
# Startup the infra in background
docker compose up -d
```
This launches PostgreSQL on port `5432` and `mosaicod` on its **default port** `6726`

!!! warning "Volatile storage"

    The default Mosaico configuration uses non persistent storage. This means that if the container is destroyed, all stored data will be lost. Since Mosaico is still under active development, we provide this simple, volatile setup by default. For persistent storage, the standard `compose.yml` file can be easily extended to utilize a Docker volume.


## Building from Source

To build Mosaico for production, you need a Rust toolchain. Mosaico uses `sqlx` for compile-time query verification, which typically requires a live database connection. However, we support an offline build mode using cached metadata (`.sqlx` folder).

### Offline Build - Recommended

```bash
SQLX_OFFLINE=true cargo build --release
```
The binary will be located at `target/release/mosaicod`.

### Live Migrations

If you need to modify the database schema, a running PostgreSQL instance is required. This allows `sqlx` to verify queries against a live database during compilation. You can use the provided Docker Compose file in `docker/devel` which sets up an instance of [MinIO](https://www.min.io/) and a PostgreSQL database.

First, start the development environment. From inside the `docker/devel` directory, run:
```bash
# Start the services in the background
docker compose up -d

# To stop and remove the volumes (which clears all data), run:
docker compose down -v
```

Next, from the root of the `mosaicod` workspace, install the necessary tools, configure the environment, and run the build.
```bash
# Install the SQLx command-line tool
cargo install sqlx-cli

# Copy the development environment variables for the database connection
cp env.devel .env

# Apply the database migrations
cargo sqlx migrate run 

# Finally, compile the project
cargo build --release
```

## Configuration

The server supports S3-compatible object storage by default but can be configured for local storage via command line options.

### Database

Mosaico requires a connection to a running **PostgreSQL** instance, which is defined via the `MOSAICO_REPOSITORY_DB_URL` environment variable.

### Remote Storage Configuration

For production deployments, `mosaicod` should be configured to use an S3-compatible object store (such as AWS S3, Google Cloud Storage, Hetzner Object Store, etc) for durable, long-term storage. This is configured through the following environment variables:

| Environment Variable       | Description                                                                                                                                                           |
| -------------------------- | --------------------------------------------------------------------------------------------------------------------------------------------------------------------- |
| <code class="nowrap">`MOSAICO_STORE_BUCKET`</code>     | The name of the S3 bucket where Mosaico will store all data blobs. This bucket must be created before starting the server.                                   |
| <code class="nowrap">`MOSAICO_STORE_ENDPOINT`</code>   | The full URL endpoint for the S3-compatible service. This is necessary for non-AWS providers (e.g., `http://localhost:9000` for a local MinIO instance).                 |
| <code class="nowrap">`MOSAICO_STORE_ACCESS_KEY`</code> | The access key ID for authenticating with your object storage service.                                                                                                |
|  <code class="nowrap">`MOSAICO_STORE_SECRET_KEY`</code> | The secret access key that corresponds to the provided access key ID, used for authentication.                                                                        |

### Local Storage Configuration

This command will start a `mosaicod` instance using the local filesystem as storage layer. 
```bash

mosaicod run --local-store /tmp/mosaicod
```
