# Setup

## Running with Containers

For rapid prototyping, we provide a standard Docker Compose configuration. This creates an isolated network environment containing the `mosaicod` server and its required PostgreSQL database.

```yaml title="Daemon compose file", hl_lines="29-30"
name: "mosaico"
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
    healthcheck: # (3)!
      test: ["CMD-SHELL", "pg_isready -U  postgres"]
      interval: 5s
      timeout: 5s
      retries: 5

  mosaicod:
    image: ghcr.io/mosaico-labs/mosaicod
    container_name: mosaicod:latest # (4)!
    networks:
      - mosaico
    environment: # (1)!
      MOSAICOD_DB_URL: postgresql://postgres:password@db:5432/mosaico
      MOSAICOD_STORE_ENDPOINT: file:///
      MOSAICOD_STORE_BUCKET: data
    volumes:
      - mosaico-data:/data
    command: | 
      run --host 0.0.0.0 --port 6726 --log-level info 
    depends_on:
      database:
        condition: service_healthy
    ports:
      - "127.0.0.1:6726:6726" # (2)!

volumes:
  pg-data:
  mosaico-data:

networks:
  mosaico:
```

1. Here you can list any additional command line options for `mosaicod`. In this example, we configure the server to use the local filesystem for storage, which is mounted to the `/data` directory in the container. This allows you to persist data across container restarts and easily access it from the host machine. Additional environment variables can be set here to configure the daemon's behavior, see [environment variables](env.md) for a complete list of options.

2. Remove `127.0.0.1` to expose this service to external networks. By default, this configuration restricts access to the local machine for security reasons. If you need to access the server from other machines on the network, you can modify the port mapping to allow external connections.

3. The `healthcheck` ensures that the `mosaicod` service only starts after the PostgreSQL database is ready to accept connections. This prevents startup errors related to database connectivity.

4. There are other available [predefined tags](#container-tags) that you can use.

This configuration provisions both Postgres and mosaicod within a private Docker network. Only the daemon instance is exposed to the host.

??? warning "Security"
    In this basic prototyping setup, TLS and API key management are disabled.

    The port mapping is restricted to `127.0.0.1`. If you need to access this from an external network, consider configuring `mosaicod` to [enable TLS](tls.md) or use a reverse proxy to handle SSL termination.

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

For production deployments, `mosaicod` should be configured to use an S3-compatible object store (such as AWS S3, Google Cloud Storage, Hetzner Object Store, etc) for durable, long-term storage. This is configured setting the proper [environment variables](env.md/#store) for your object store provider.

### Local Storage Configuration

This command will start a `mosaicod` instance using the local filesystem as storage layer.

```bash
export MOSAICOD_STORE_ENDPOINT=file:///some/local/directory
export MOSAICOD_STORE_BUCKET=bucket-name
```

and run `mosaicod run`.

