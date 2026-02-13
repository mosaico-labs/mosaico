###  Usage

Start the server locally with verbose logging:

```bash
mosaicod run [OPTIONS]
```

| Option | Default | Description |
| :--- | :--- | :--- |
| `--host` | `false` | Listen on all addresses, including LAN and public addresses. |
| `--port <PORT>` | `6726` | Port to listen on. |
| `--local-store <PATH>` | `None` | Enable storage of objects on the local filesystem at the specified directory path. |

To enable logging during execution setup the `RUST_LOG` environment variable (e.g. `RUST_LOG=mosaico=trace`).

Mosaico requires a connection to a running **PostgreSQL** instance, which is defined via the `MOSAICO_REPOSITORY_DB_URL` environment variable.

You can start the server by pointing it to a directory on your machine:
```bash
# Setup the database endpoint
export MOSAICO_REPOSITORY_DB_URL="postgresql://user:password@localhost:5432/mosaico"

# Start mosaico
./mosaicod run --local-store /directory/on/your/machine
```
This command launches `mosaicod` and configures it to save all binary files directly to the specified local folder. 
If you need to set up remote storage (like S3) or tweak other settings, please refer to the [Configuration](#configuration) section.

## Workspace 

Mosaico utilizes a multi-crate workspace to maintain high modularity and clear boundaries between system components.


```text
mosaico/
├── bin/
│   └── mosaicod/        # Main daemon entry point
├── crates/
│   ├── mosaicod-repo/ 
│   ├── mosaicod-core/  
│   └── ...              # Other internal modules
└── tests/
    └── tests/           # Cross-crate platform tests

```

The core logic resides within the **crates** directory, which houses all internal libraries and shared functionalities. The **bin** directory contains the primary entry point for the **mosaicod** daemon, acting as the thin wrapper that orchestrates the internal crates. To ensure system stability, the **tests** directory is reserved for platform-level integration tests; unlike unit tests, these are explicitly permitted to depend on and call any combination of internal crates to verify complex, cross-component workflows.

### Build

Mosaico is written in Rust and uses `sqlx` for compile-time checked queries. 
This requires access to a live database during the build process to verify SQL schemas, unless you use cached data.

#### Option A: Using Cached Query Data (Recommended)

The repository includes an `.sqlx` folder containing cached query data. This allows you to compile without running a local Postgres instance.

```bash
SQLX_OFFLINE=true cargo build --release
```

Once compiled, the binary can be found at `target/release/mosaicod`.

#### Option B: Building with Live Migrations

If you are developing and changing the schema, follow these steps:

##### Configure the Environment
Create a `.env` file in the project root (or export the variable directly):

```env
DATABASE_URL=postgres://postgres:password@localhost:5432/mosaico
```
##### Install SQLx CLI
This tool is required for running migrations and preparing offline data.

```bash
cargo install sqlx-cli
```
##### Run Migrations
This sets up your local database schema.

```bash
cd creates/mosaicod-repo
cargo sqlx migrate run
```
##### Build
```bash
cargo build --release
```

-----

###  Configuration

#### Remote Storage (S3 Compatible)

To configure Mosaico to use a remote S3-compatible storage system, set the following environment variables in your `.env` file or export them before running the server.

| Variable | Description |
| :--- | :--- |
| **`MOSAICO_STORE_BUCKET`** | The name of the storage bucket. |
| **`MOSAICO_STORE_ENDPOINT`** | Endpoint URL of the object storage service. |
| **`MOSAICO_STORE_ACCESS_KEY`** | Public access key for the object storage. |
| **`MOSAICO_STORE_SECRET_KEY`** | Secret access key for the object storage. |


