---
name: mosaico-setup
description: Take a Python user from nothing to a verified connection to a Mosaico daemon — install the mosaicolabs SDK and the mosaico CLI in a virtual environment, create and select a CLI connection profile, prove the connection with read-only commands and a five-line MosaicoClient script, and diagnose why a connection fails (daemon down, wrong host or port, TLS mismatch, missing or invalid API key, SDK and daemon of different versions) or why mosaicod cannot log in to its database. Use when someone says "set up Mosaico", "install the SDK/CLI", "configure a profile", "connect to mosaicod", "mosaico sequence ls fails", "Server did not become available", "connection refused", or "no PostgreSQL user name specified in startup packet". Not for searching data (mosaico-sdk-query), building datasets (mosaico-ml-dataset) or explaining concepts and finding doc pages (mosaico-docs-guide).
---

# Setting up a Mosaico connection

Get the user to the point where both the CLI and the SDK list the sequences of their daemon. The
documentation at https://docs.mosaico.dev is the reference (it serves the latest release); this file says
how to use it and where it is wrong. The page paths here are hints: if one has moved, find it by title in
https://docs.mosaico.dev/llms.txt. For a daemon built from `main`, the same pages are under `docs/main/docs`
and `docs/py/docs` of its checkout. For what the SDK really does, read the installed package
(`python -c "import inspect, mosaicolabs as m; print(inspect.getsourcefile(m.MosaicoClient))"`).

Covers 0.8 and later, and `main`. `mosaicod` and the SDK share one version, so the SDK must have the
daemon's `0.X` (section 1). For a 0.6 or 0.7 daemon, see section 5.

Scope and limits:

- **Every check is read-only.** Use `sequence ls`, `topic ls|stat|mcat` and the script in section 3.
  Never create, delete or ingest anything to "test" a connection: no `mosaico inject` (it writes), no
  `mosaico examples <name>` (some examples ingest data).
- **Installing the daemon is not part of this skill.** If there is no daemon yet, point the user to
  the official installer, `curl -fsSL https://get.mosaico.dev | sh` (Linux and macOS; preview with
  `sh -s -- --dry-run`), and let them run it; do not run `sudo` or the
  installer yourself (https://docs.mosaico.dev/getting_started, https://docs.mosaico.dev/daemon/install).
- **Never print an API key.** Refer to it as "the key you were given". Never `cat`, `grep` or `printenv`
  a key file, `config.toml` or the environment; check presence only: `test -n "$MOSAICO_API_KEY"`,
  `test -s key.txt`. The one allowed read is `$(cat key.txt)` inside the command that uses the key.

Ask first: host, port (default `6726`), whether the daemon has TLS on (and a CA file if it is
self-signed), whether it requires an API key, and its version (the release or image tag it was
installed from, or the probe at the end of section 4).

## 1. Install the SDK and the CLI

Python 3.10 or newer, in a dedicated virtual environment. Check `python3 --version` first: macOS ships
3.9, so build the venv with a newer interpreter the user already has (`python3.12 -m venv .venv`, or
one from Homebrew, pyenv or uv) instead of the system one. The `cli` extra is what makes the `mosaico`
command work; without it every `mosaico` call ends in `ModuleNotFoundError: No module named 'typer'`.

**Install the SDK with the daemon's `0.X`.** `mosaicod` and the SDK are released together under one
`vX.Y.Z` tag: the daemon on GHCR and GitHub releases, the SDK on PyPI
(https://docs.mosaico.dev/dev/release_cycle). The SDK does not check
the daemon's version, and an SDK of another `0.X` may fail to connect at all (section 4). If the daemon's
version is unknown, `pip install pyarrow` alone and run the probe at the end of section 4 first.

```bash
python3.12 -m venv .venv          # any Python >= 3.10; not the system python3 if it is 3.9
.venv/bin/pip install "mosaicolabs[cli]==0.X.*"     # 0.X = the daemon's, e.g. 0.9.*
```

For a daemon built from `main`, install the SDK from the same commit:

```bash
.venv/bin/pip install "/path/to/checkout/mosaico-sdk-py[cli]"   # absolute path to the checkout the daemon was built from
.venv/bin/pip install "mosaicolabs[cli] @ git+https://github.com/mosaico-labs/mosaico@<commit>#subdirectory=mosaico-sdk-py"
```

Check: `.venv/bin/pip show mosaicolabs` prints the version (the CLI has no `--version`),
`.venv/bin/mosaico --help` prints the command list. The first command that reads data in a new venv
took 15-30 s in testing; later calls take under a second. Do not read that delay as a connection problem.

## 2. Create and select a profile

Profiles are described in https://docs.mosaico.dev/cli/profiles. Script host, port and TLS with
`--no-interactive`:

```bash
mosaico profile add dev --no-interactive --host localhost            # port defaults to 6726
mosaico profile add prod --no-interactive --host api.example.com --port 6726 --tls
mosaico profile ls                       # name,host,port,default: no TLS column, the key is never shown
mosaico profile default prod             # switch the default
mosaico --profile dev sequence ls        # one command against another profile
```

The key never goes in literally, where the shell history keeps it. Read it from a file the user saved
(mode 600), in one of three ways:

- At a terminal, the user runs `mosaico profile add prod --host ... --port ... --tls` and types the key
  at the hidden `API Key` prompt. Without `--no-interactive` the command also asks, for each field not
  given as a flag: host, port, `Enable TLS? [y/N]` (only without `--tls`), the CA path (only with TLS on
  and no `--cert-path`), and `Overwrite?` for an existing name. Piped stdin is not hidden.
- From a script: `--api-key "$(cat key.txt)"` with `--no-interactive` (add `--force` to replace a
  profile), inside `(umask 077; mosaico profile add ...)` and followed by
  `chmod 600 ~/.mosaico/config.toml`: the CLI writes the file with the default umask, readable by other
  local users. The history shows only `$(cat key.txt)`; the key is in the process list while the command
  runs, then in `config.toml`.
- Not stored at all: keep the profile key-less and set `MOSAICO_API_KEY="$(cat key.txt)"` on each
  command or in the shell. The variable overrides the profile at run time only; `export` before
  `profile add` does not put the key in the file.

Profile rules:

- The first profile ever added becomes the default; later ones do not unless you pass `--default`.
  Run `mosaico profile ls` before adding: if a default exists, add with `--default` or use
  `--profile <name>`, then confirm with `profile ls`.
- A port goes either in `--host localhost:6726` or in `--port`, not both (`Port given twice`). `host:port`
  works only with `--no-interactive`: interactively the port prompt fills in 6726 and the same error
  follows, so give a bare `--host` and `--port`.
- Re-adding an existing name needs `--force` in non-interactive mode.
- The file is `~/.mosaico/config.toml`, or `MOSAICO_CONFIG_PATH`. It stores the API key in clear text,
  so `chmod 600` it when a key is in there.
- `MOSAICO_PROFILE` selects the profile, like `--profile`.
- `MOSAICO_DAEMON_URL`, `MOSAICO_API_KEY`, `MOSAICO_TLS` and `MOSAICO_CERT_PATH` override single fields
  of the resolved profile, the quickest way to try a variation without editing it. Two quirks:
  - Up to 0.9.0, `MOSAICO_DAEMON_URL` without a port means port 6726, not the profile's port: always
    pass `host:port`. Later versions keep the profile's port (6726 only with no profile or no port in it).
  - `MOSAICO_TLS` is on only for `1`/`true`/`yes`, as the profiles page says; `mosaico --help` says "any
    value", but `on` leaves TLS off.
- The subcommands of `sequence` and `topic` refuse to run, even `sequence ls --help`, until a host
  resolves (a default profile, `--profile` / `MOSAICO_PROFILE`, or `MOSAICO_DAEMON_URL`):
  `Error: No default profile found`.

The SDK does not read profiles. `MosaicoClient.from_env(host, port)` reads only `MOSAICO_API_KEY` and
`MOSAICO_TLS_CERT_FILE` (a different name from the CLI's `MOSAICO_CERT_PATH`); host and port are always
arguments (https://docs.mosaico.dev/python-sdk/SDK/client/). It turns TLS on only when `MOSAICO_TLS_CERT_FILE` is set, so for a
TLS daemon with a publicly trusted certificate use `MosaicoClient.connect(host, port, enable_tls=True)`.

## 3. Prove the connection

CLI, against the default profile (https://docs.mosaico.dev/cli/commands):

```bash
mosaico sequence ls                                   # one line per sequence: name,ts_min,ts_max
mosaico topic ls --locator "/front/*"                 # topics whose NAME matches, in every sequence
mosaico topic ls | grep '^run_01/'                    # the topics of one sequence
mosaico topic ls --locator "/front/imu" | grep '^run_01/' | mosaico topic stat
mosaico topic mcat --count 3 "run_01/front/imu,<ts_min>,<ts_max>"
mosaico sequence ls --locator run_01 | mosaico sequence stat
```

`run_01` and `/front/imu` are placeholders: take the real names from `sequence ls` and `topic ls`.

CLI behaviour to know (the same code from 0.7.0 to `main`, except where a version is named):

- `topic ls --locator` matches the topic name (`/front/imu`), not `<sequence>/<topic>`:
  `"run_01/*"` matches nothing. Filter by sequence with `grep` on the output.
- `stat` and `mcat` take the full handler `name,ts_min,ts_max` (pipe it from `ls`); a bare name is
  `Invalid handler format`.
- `stat` reports size and time range but no message count. Count with `mcat` (one JSON line per
  message) piped to `wc -l`. Up to 0.9.0 `mcat` treats `ts_max` as exclusive, so the handler `ls`
  printed loses the last message: pass `ts_max + 1`. Later versions include it.
- Output order of `ls` is not stable; piped output is CSV, a terminal gets a table.

Python, with the same host and port:

```python
from mosaicolabs import MosaicoClient

HOST, PORT = "<host>", 6726  # the user's; 6726 is only the default port
with MosaicoClient.connect(HOST, PORT) as client:   # add enable_tls=True / tls_cert_path=..., api_key=...
    print("server", client.info().version)
    print("sequences", client.list_sequences())
```

The setup is done when the CLI and the script list the same sequences and the server's `0.X` matches
the installed SDK's (`pip show mosaicolabs`); a debug build of the daemon reports `X.Y.Z-devel`, which
counts as `X.Y.Z`. Take the server's version from `client.info()`: `mosaicod --version` prints `X.Y.Z-dev`
for any build not made from the release tag, while the server still reports `X.Y.Z`. Say so with the output, then hand over: `mosaico-sdk-query` to search the data,
`mosaico-ml-dataset` to build training sets, `mosaico-docs-guide` for concepts and the doc pages.

## 4. Diagnose a failure

The SDK retries the `info` call until `timeout` seconds (5 by default) have passed and then raises
`ConnectionError`; the CLI prints it as a long traceback and exits 1. The deadline is checked between
attempts, so the failure takes longer than 5 s: about 6 s for a refused port, about 20 s for an
unreachable host (one attempt waits for the TCP connect to give up). The cause is in the `Last error:`
line at the end (`err=$(mktemp); mosaico sequence ls > "$err" 2>&1; echo "exit $?"; tail -8 "$err"`,
outside the user's folder; a pipe into
`tail` would report tail's exit code 0 instead of the CLI's 1). Read it before guessing, and with `localhost` read the whole
output: gRPC tries `::1` and `127.0.0.1`, and `Last error` may be the `::1` refusal (a daemon published
on `127.0.0.1` only) while the real cause, such as an SSL handshake error, is logged above it. Retry with
`127.0.0.1` to get a single, stable error.

| `Last error` contains                                                   | Cause                                                      | Fix                                                                                        |
| ----------------------------------------------------------------------- | ---------------------------------------------------------- | ------------------------------------------------------------------------------------------ |
| `Connection refused`                                                    | nothing listens on that host:port: wrong port, daemon down | find the right port (`MOSAICOD_PORT`, `--port`, the compose port mapping), then the daemon |
| `tcp handshaker shutdown`, slow failure                                 | host unreachable: wrong address, firewall, VPN             | check the address and the network path                                                     |
| `Domain name not found`                                                 | the host name does not resolve                             | fix the name                                                                               |
| `Ssl handshake failed` … `wrong version number`                         | client uses TLS, daemon does not                           | `--no-tls` on the profile, unset `MOSAICO_TLS`, no `enable_tls` / `tls_cert_path`          |
| `Missing API key in request header` (permission denied)                 | daemon enforces keys, none was sent                        | add the key to the profile or `api_key=`                                                   |
| `API key does not exist.` / `API key is expired.`                       | wrong, revoked or expired key                              | ask the daemon admin for a valid key                                                       |
| `Bad request: body decode error`                                        | SDK 0.7 or older, daemon 0.8 or later                      | install the SDK with the daemon's `0.X` (section 1)                                        |
| `no available action`, `body deserialization error`, or no `Last error` | a 0.6 or 0.7 daemon or SDK                                 | run the probe below, then section 5                                                        |

The key rows come from the daemon source and were not reproduced live. A key without the `read`
permission connects fine (the `info` call needs no permission) and fails later: `sequence ls` with
`provided API key has not enough permissions to execute Query action.`, `list_sequences()` with
`provided API key does not have READ permissions.`; the fix is a key with
`read` (https://docs.mosaico.dev/daemon/api_key).

When the client has TLS off and the daemon has it on, the handshake fails without a readable cause:
ask whether the daemon runs with `MOSAICOD_TLS_ENABLED=true` (plus `MOSAICOD_TLS_CERT_FILE` and
`MOSAICOD_TLS_PRIVATE_KEY_FILE`; https://docs.mosaico.dev/daemon/env, section TLS). Since 0.7 there is no
`--tls` flag on `mosaicod server` (https://docs.mosaico.dev/daemon/migration, Server flags);
https://docs.mosaico.dev/daemon/tls still shows it and is out of date on that point. A self-signed daemon
needs its `ca.pem` as `--cert-path` / `tls_cert_path`; a missing file is a `FileNotFoundError` before any
network call.

**Prove the fix, do not infer it.** Once you name the cause, re-run the command that failed with only
the suspected value corrected (for example `MOSAICO_DAEMON_URL=<host>:<right port> mosaico sequence ls`)
and show that it lists the sequences. A port read from `docker ps` or a config file is a hypothesis
until that command succeeds.

Two results that look like success but are not proof:

- A wrong API key "works" when the daemon does not enforce keys (`MOSAICOD_API_KEY_ENABLED`, off by
  default, https://docs.mosaico.dev/daemon/env). Do not tell the user the key is valid; say enforcement
  is off.
  From the client side, a call that succeeds with a made-up key is the proof that it is off; only the
  daemon admin (`mosaicod api-key`) can confirm a key.
- A misspelled host that still connects was resolved by a DNS search domain. Retry with a trailing dot
  (`myhost.`) to see what the name really resolves to.

To tell "daemon down" from "SDK of another version", ask the daemon its version with the Arrow Flight
client that the SDK already installs (use `grpc+tls://` for a TLS daemon; with key enforcement on and no key, it
fails with `Missing API key`). An answer means the daemon is up at that address and the problem is
on the client:

```python
import pyarrow.flight as fl
c = fl.FlightClient("grpc+tcp://localhost:6726")
tries = (("info", b"", "0.8+"), ("info", b"{}", "0.7"), ("version", b"{}", "0.6"))
for action, body, line in tries:
    try:
        print(line, next(iter(c.do_action(fl.Action(action, body)))).body.to_pybytes())
        break
    except Exception as e:
        print(line, "->", e)
```

The answer carries the version in readable text (`X.Y.Z`, or `X.Y.Z-devel` for a debug build), and the
attempt that answered confirms the line. A `body deserialization error` after a successful connect is a
malformed metadata or query filter, not a version mismatch.

When the user runs the daemon and asks what to change, point to the setting, do not restart it
yourself: TLS through `MOSAICOD_TLS_*` (the certificate steps of https://docs.mosaico.dev/daemon/tls still
apply), keys in https://docs.mosaico.dev/daemon/api_key, every variable in https://docs.mosaico.dev/daemon/env
and its `config.toml` key in https://docs.mosaico.dev/daemon/config.

When `mosaicod` itself cannot log in to PostgreSQL:

- The URL carries no credentials: `MOSAICOD_DB_URL=postgresql://host:5432/db`, with `MOSAICOD_DB_USER` and
  `MOSAICOD_DB_PASSWORD` (or `MOSAICOD_DB_PASSWORD_FILE`, not both), or `database.user` /
  `database.password_file` in `config.toml`; a variable wins over the file (https://docs.mosaico.dev/daemon/env,
  section DBMS). mosaicod replaces the URL's user and password with these settings, empty when unset, so a
  `user:password@` inside the URL is not used.
- `no PostgreSQL user name specified in startup packet` (a PostgreSQL message) means the user is empty:
  neither `MOSAICOD_DB_USER` nor `database.user` is set, even when the URL has `user@`.
- mosaicod loads a `.env` file from the directory it is started in, or from the nearest parent directory
  that has one, and never overrides a variable already set in the shell. So it can work from the
  `mosaicod/` checkout, where `cp env.devel .env` put the credentials (https://docs.mosaico.dev/daemon/install),
  and fail from anywhere else; or a stale exported `MOSAICOD_DB_USER` can win over the `.env`. An exported
  but empty one counts as unset yet still blocks the `.env` value. In Docker/compose the variables come from
  the service's `environment:` / `env_file`, not from a `.env` in the user's shell directory. Ask where it was
  started and which `MOSAICOD_DB_*` are exported (presence only: never print the password).

## 5. Older daemons (0.6, 0.7)

An SDK of 0.8 or later cannot talk to a 0.6 or 0.7 daemon, and the reverse. Install the SDK with the
daemon's `0.X` here too: `mosaicolabs[cli]==0.7.*` or `==0.6.*`, both on PyPI.

- The probe at the end of section 4 places the daemon: a 0.7 daemon answers `info` with a JSON body, a
  0.6 daemon knows only `version`.
- `Last error` on connect: `no available action for string` … `info` is a 0.6 daemon with a newer SDK;
  `Bad request: body deserialization error` is a 0.7 daemon with an SDK of 0.8 or later.
- SDK 0.6.x has `client.version()` instead of `client.info()`, and fails on every connect problem, a
  daemon of another version included, with `Server did not become available within 5s` and no
  `Last error` line, which looks exactly like a daemon that is down. Run the probe before blaming the daemon.
- A 0.6 daemon turns TLS and key enforcement on with the `mosaicod server --tls` / `--api-key` flags, not
  `MOSAICOD_TLS_ENABLED` / `MOSAICOD_API_KEY_ENABLED`, and keeps the database login inside
  `MOSAICOD_DB_URL` (https://docs.mosaico.dev/daemon/migration, From 0.6 to 0.7).
