---
name: mosaico-setup
description: Take a Python user from nothing to a verified connection to a Mosaico daemon — install the mosaicolabs SDK and the mosaico CLI in a virtual environment, create and select a CLI connection profile, prove the connection with read-only commands and a five-line MosaicoClient script, and diagnose why a connection fails (daemon down, wrong host or port, TLS mismatch, missing or invalid API key, SDK older than the daemon). Use when someone says "set up Mosaico", "install the SDK/CLI", "configure a profile", "connect to mosaicod", "mosaico sequence ls fails", "Server did not become available", or "connection refused". Not for searching data (mosaico-sdk-query), building datasets (mosaico-ml-dataset) or explaining concepts (mosaico-onboarding).
---

# Setting up a Mosaico connection

Get the user to the point where both the CLI and the SDK list the sequences of their daemon. The
documentation at https://docs.mosaico.dev is the reference; this file says how to use it and where it is wrong.
Inside a checkout of mosaico-labs/mosaico the same pages are under `docs/main/docs` and `docs/py/docs`;
a page marked "not on the site yet" exists only there. For what the SDK really does, read the installed
package (`python -c "import inspect, mosaicolabs as m; print(inspect.getsourcefile(m.MosaicoClient))"`).

Covers the two lines in use: the 0.7 release (`mosaicolabs` 0.7.0 from PyPI with a daemon from the
`release/v0.7` branch) and `main` (0.8.0-dev after #817). They cannot talk to each other (0.6 and 0.7
speak JSON on the wire, `main` protobuf), so follow the line of the user's daemon: ask, or run the probe at
the end of section 4. On older versions: a 0.6 daemon knows the `version` action, not `info`, and
turns TLS and key enforcement on with the `mosaicod server --tls` / `--api-key` flags, not
`MOSAICOD_TLS_ENABLED` / `MOSAICOD_API_KEY_ENABLED`; SDK 0.6.3 has `client.version()` instead of
`client.info()` and no `Last error` line; before 0.7 the database login sits in `MOSAICOD_DB_URL`.

Scope and limits:

- **Every check is read-only.** Use `sequence ls`, `topic ls|stat|mcat` and the script in section 3.
  Never create, delete or ingest anything to "test" a connection.
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

```bash
python3.12 -m venv .venv          # any Python >= 3.10; not the system python3 if it is 3.9
.venv/bin/pip install "mosaicolabs[cli]"
```

**Install the SDK released alongside the daemon.** Daemon and SDK are versioned and released
independently; their tags are `vX.Y.Z` and `py/vX.Y.Z` (https://docs.mosaico.dev/dev/release_cycle). The SDK
does not check the daemon's version, and PyPI can lag behind the daemon: an SDK from an older release
may not talk to a newer daemon at all, and SDK 0.6.x fails on every connect problem, including a too-new daemon, with
`Server did not become available within 5s` and no `Last error` line, which looks exactly like a
daemon that is down. When PyPI has no release that goes with the daemon
(`pip index versions mosaicolabs --pre`), install from the repository: the latest SDK tag `py/vX.Y.*` with the daemon's `X.Y` (not the daemon's
tag, whose branch does not carry the SDK version); when no such tag exists yet (`git ls-remote --tags
https://github.com/mosaico-labs/mosaico "py/vX.Y.*"` prints nothing), the branch `@release/py/vX.Y`;
`@main` for a `-dev` daemon. Within `-dev` the version number is not enough: #817 switched the wire format from JSON to protobuf, and an SDK and a
daemon on different sides of it cannot talk although both report `0.8.0-dev`. To learn the daemon's
version before installing the SDK, `pip install pyarrow` alone and run the probe of section 4:

```bash
.venv/bin/pip install "mosaicolabs[cli] @ git+https://github.com/mosaico-labs/mosaico@py/vX.Y.Z#subdirectory=mosaico-sdk-py"
# or, from the root of a checkout of this repository:
.venv/bin/pip install "./mosaico-sdk-py[cli]"
```

Check: `.venv/bin/pip show mosaicolabs` prints the version (the CLI has no `--version`),
`.venv/bin/mosaico --help` prints the command list. The first command that reads data in a new venv
took about 15 s in testing; later calls take under a second. Do not read that delay as a connection problem.

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
- A port goes either in `--host localhost:6726` or in `--port`, not both (`Port given twice`). `host:port`
  works only with `--no-interactive`: interactively the port prompt fills in 6726 and the same error
  follows, so give a bare `--host` and `--port`.
- Re-adding an existing name needs `--force` in non-interactive mode.
- The file is `~/.mosaico/config.toml`, or `MOSAICO_CONFIG_PATH`. It stores the API key in clear text,
  so `chmod 600` it when a key is in there.
- `MOSAICO_PROFILE` selects the profile, like `--profile`.
- `MOSAICO_DAEMON_URL`, `MOSAICO_API_KEY`, `MOSAICO_TLS` and `MOSAICO_CERT_PATH` override single fields
  of the resolved profile, the quickest way to try a variation without editing it. Two quirks:
  - In 0.7.0, `MOSAICO_DAEMON_URL` without a port means port 6726, not the profile's port: always pass
    `host:port`. On `main` the profile's port is kept (6726 only with no profile or no port in it).
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
mosaico topic mcat --count 3 "run_01/front/imu,<ts_min>,<ts_max + 1>"
mosaico sequence ls --locator run_01 | mosaico sequence stat
```

`run_01` and `/front/imu` are placeholders: take the real names from `sequence ls` and `topic ls`.

Where the CLI differs from its documentation (observed on 0.8.0-dev; the CLI code is the same in SDK
0.7.0, and the `--locator` and `ts_max` points were reproduced against a 0.7.0 daemon):

- `topic ls --locator` matches the topic name (`/front/imu`), not `<sequence>/<topic>`:
  `"run_01/*"` matches nothing. Filter by sequence with `grep` on the output.
- `stat` and `mcat` take the full handler `name,ts_min,ts_max` (pipe it from `ls`); a bare name is
  `Invalid handler format`.
- `stat` reports size and time range but no message count. Count with `mcat` (one JSON line per
  message) piped to `wc -l`, and pass `ts_max + 1`: `mcat` treats `ts_max` as exclusive, so the
  handler `ls` printed loses the last message.
- Output order of `ls` is not stable; piped output is CSV, a terminal gets a table.

Python, with the same host and port:

```python
from mosaicolabs import MosaicoClient

HOST, PORT = "<host>", 6726  # the user's; 6726 is only the default port
with MosaicoClient.connect(HOST, PORT) as client:   # add enable_tls=True / tls_cert_path=..., api_key=...
    print("server", client.info().version)
    print("sequences", client.list_sequences())
```

The setup is done when the CLI and the script list the same sequences and the server's `X.Y` matches
the installed SDK's (`pip show` writes `X.Y.Z-dev` as `X.Y.Z.devN`; patch numbers are independent). For
a `-dev` daemon both must also be on the same side of #817, which the probe of section 4 tells. Say so with
the output, then hand over: `mosaico-sdk-query` to search the data,
`mosaico-ml-dataset` to build training sets, `mosaico-onboarding` for a guided tour.

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

| `Last error` contains                                   | Cause                                                      | Fix                                                                                        |
| ------------------------------------------------------- | ---------------------------------------------------------- | ------------------------------------------------------------------------------------------ |
| `Connection refused`                                    | nothing listens on that host:port: wrong port, daemon down | find the right port (`MOSAICOD_PORT`, `--port`, the compose port mapping), then the daemon |
| `tcp handshaker shutdown`, slow failure                 | host unreachable: wrong address, firewall, VPN             | check the address and the network path                                                     |
| `Domain name not found`                                 | the host name does not resolve                             | fix the name                                                                               |
| `Ssl handshake failed` … `wrong version number`         | client uses TLS, daemon does not                           | `--no-tls` on the profile, unset `MOSAICO_TLS`, no `enable_tls` / `tls_cert_path`          |
| `Missing API key in request header` (permission denied) | daemon enforces keys, none was sent                        | add the key to the profile or `api_key=`                                                   |
| `API key does not exist.` / `API key is expired.`       | wrong, revoked or expired key                              | ask the daemon admin for a valid key                                                       |
| nothing after `within 5s` (no `Last error`)             | SDK 0.6 or older, which hides the real error               | run the probe below, then install the matching SDK (section 1)                             |
| `no available action for string` … `info`               | daemon up, older than 0.7 (knows only `version`)           | install the SDK with the daemon's `X.Y` (section 1)                                        |
| `Bad request: body deserialization error` on connect    | SDK speaks protobuf (#817 on), daemon still JSON           | confirm with the probe below; SDK of the daemon's commit, or update the daemon             |
| `Bad request: body decode error`                        | daemon speaks protobuf (#817 on), SDK still JSON           | install the SDK from the daemon's commit or later (section 1)                              |

The key rows come from the daemon source and were not reproduced live. A key without the `read`
permission connects fine (the `info` call needs no permission) and fails later: `sequence ls` with
`provided API key has not enough permissions to execute Query action.`, `list_sequences()` with
`provided API key does not have READ permissions.`; the fix is a key with
`read` (https://docs.mosaico.dev/daemon/api_key).

When the client has TLS off and the daemon has it on, the handshake fails without a readable cause:
ask whether the daemon runs with `MOSAICOD_TLS_ENABLED=true` (plus `MOSAICOD_TLS_CERT_FILE` and
`MOSAICOD_TLS_PRIVATE_KEY_FILE`; all three in `docs/main/docs/daemon/env.md`, not on the site yet: https://docs.mosaico.dev/daemon/env
is the 0.6 page and lists only the two certificate variables). Since 0.7 there is no `--tls` flag on
`mosaicod server` (`docs/main/docs/daemon/migration.md`, not on the site yet); https://docs.mosaico.dev/daemon/tls still shows
it and is out of date on that point. A self-signed daemon needs its `ca.pem` as `--cert-path` /
`tls_cert_path`; a missing file is a `FileNotFoundError` before any network call.

**Prove the fix, do not infer it.** Once you name the cause, re-run the command that failed with only
the suspected value corrected (for example `MOSAICO_DAEMON_URL=<host>:<right port> mosaico sequence ls`)
and show that it lists the sequences. A port read from `docker ps` or a config file is a hypothesis
until that command succeeds.

Two results that look like success but are not proof:

- A wrong API key "works" when the daemon does not enforce keys (`MOSAICOD_API_KEY_ENABLED`, off by
  default, `docs/main/docs/daemon/env.md`, not on the site yet). Do not tell the user the key is valid; say enforcement is off.
  From the client side, a call that succeeds with a made-up key is the proof that it is off; only the
  daemon admin (`mosaicod api-key`) can confirm a key.
- A misspelled host that still connects was resolved by a DNS search domain. Retry with a trailing dot
  (`myhost.`) to see what the name really resolves to.

To tell "daemon down" from "SDK too old", ask the daemon its version with the Arrow Flight client that
the SDK already installs (use `grpc+tls://` for a TLS daemon; with key enforcement on and no key, it
fails with `Missing API key`). An answer means the daemon is up at that address and the problem is
on the client:

```python
import pyarrow.flight as fl
c = fl.FlightClient("grpc+tcp://localhost:6726")
tries = (("info", b"", "protobuf"), ("info", b"{}", "JSON"), ("version", b"{}", "JSON"))
for action, body, wire in tries:
    try:
        print(action, wire, next(iter(c.do_action(fl.Action(action, body)))).body.to_pybytes())
        break
    except Exception as e:
        print(action, wire, "->", e)
```

The answer carries the version in readable text, and the attempt that answered places the daemon:
`info` in protobuf from #817 on, `info` in JSON for 0.7 up to #817, `version` before 0.7 (an `info`
error `no available action for string` means the daemon is up and older than 0.7). Both sides of #817
call themselves `0.8.0-dev`, so the version alone does not settle it. A `body deserialization error`
after a successful connect is a malformed metadata or query filter, not a wire mismatch.

When the user runs the daemon and asks what to change, point to the setting, do not restart it
yourself: TLS through `MOSAICOD_TLS_*` in `docs/main/docs/daemon/env.md` (not on the site yet; the certificate steps of
https://docs.mosaico.dev/daemon/tls still apply), keys in https://docs.mosaico.dev/daemon/api_key, every
variable in `docs/main/docs/daemon/env.md` and `docs/main/docs/daemon/config.md` (neither on the site yet:
https://docs.mosaico.dev/daemon/env is still the 0.6 page, with the credentials inside `MOSAICOD_DB_URL`). The database URL carries no credentials:
`MOSAICOD_DB_URL=postgresql://host:5432/db` with `MOSAICOD_DB_USER` and `MOSAICOD_DB_PASSWORD` (or
`MOSAICOD_DB_PASSWORD_FILE`) as separate variables. Since 0.7 mosaicod sets user and password from those
variables, so a `user:password@` inside the URL is not used and the database login fails.
