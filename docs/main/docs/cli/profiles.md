---
title: Connection Profiles
sidebar_position: 2
description: "How to configure, switch between, and manage the connection profiles the mosaico CLI uses to reach a mosaicod instance, from first-time setup to multi-environment workflows."
---

A **profile** is a named, saved connection: a host, a port, and optionally an API key and TLS settings. Every `mosaico` command that talks to the platform (everything under [`sequence` and `topic`](commands.md)) needs one. Profiles let you switch between environments, `dev`, `staging`, `prod`, without repeating connection flags on every invocation.

## First-time setup

Create your first profile with `profile add`, giving it a name of your choice:

```bash
mosaico profile add dev
```

By default this runs interactively, prompting for whatever you don't pass as a flag:

```
Mosaico Server Host: localhost
Mosaico Server Port (leave empty for 6726):
API Key:
Enable TLS? [y/N]:
Success: Profile 'dev' saved to ~/.mosaico/config.toml
Profile 'dev' has been configured as your active default.
```

The **first profile you ever add is automatically set as the default**, so from this point on every command uses it without needing a `--profile` flag:

```bash
mosaico sequence ls
```

If you'd rather script the setup, skip the prompts with `--no-interactive` and pass everything as flags (only `--host` is required):

```bash
mosaico profile add dev --no-interactive --host localhost
```

Confirm it was saved correctly at any time with:

```bash
mosaico profile ls
```

```
  PROFILE   HOST        PORT   DEFAULT
  dev       localhost   6726   ✓
```

## Working with multiple profiles

Add a second profile the same way. It won't touch your existing default unless you ask it to:

```bash
mosaico profile add prod \
  --no-interactive \
  --host api.mosaico.example.com \
  --tls \
  --api-key "msk_live_xyz789" \
  --cert-path /etc/mosaico/certs/ca.pem
```

```bash
mosaico profile ls
```

```
  PROFILE   HOST                          PORT   DEFAULT
  dev       localhost                     6726   ✓
  prod      api.mosaico.example.com       6726
```

`dev` is still the default, so plain commands keep talking to it. There are two ways to reach `prod` instead:

**One-off, for a single command**, with `--profile`:

```bash
mosaico --profile prod sequence ls
```

**Permanently, for every future command**, by switching the default:

```bash
mosaico profile default prod
```

```bash
mosaico sequence ls  # now talks to prod
```

`--profile` always wins over whichever profile is marked default, so you can freely target a non-default profile without disturbing your day-to-day default.

## Managing profiles

### `mosaico profile add`

Adds or updates a profile in the configuration file.

```bash
mosaico profile add <NAME> [OPTIONS]
```

| Option | Default | Description |
| :--- | --- | :--- |
| `--default` | `false` | Mark this profile as the default. The very first profile you ever add becomes the default automatically, regardless of this flag. |
| `--interactive` / `--no-interactive` | `--interactive` | Toggle interactive prompts for any value not passed as a flag. |
| `--host <HOST>` | | Server host name or IP. May embed a port, e.g. `localhost:6726`. Required in `--no-interactive` mode. |
| `--port <PORT>` | `6726` | Server port, if not embedded in `--host`. |
| `--api-key <API_KEY>` | | Authentication API key. |
| `--tls` / `--no-tls` | `--no-tls` | Enable TLS for this connection. |
| `--cert-path <CERT_PATH>` | | Path to a custom TLS CA certificate. |
| `-f`, `--force` | `false` | Overwrite an existing profile without a confirmation prompt (only meaningful with `--no-interactive`). |

Running `profile add` on a name that already exists updates it in place: interactively you're asked to confirm the overwrite, non-interactively you need `--force`.

### `mosaico profile default`

```bash
mosaico profile default <NAME>
```

Switches the active default to the profile named `<NAME>`. This is the command you want when you're moving your day-to-day work to a different environment, as opposed to `--profile`, which only affects a single invocation.

### `mosaico profile remove`

```bash
mosaico profile remove <NAME> [-f | --force]
```

Prompts for confirmation unless `--force` is passed.

:::note
If the profile you remove was the default, another remaining profile is automatically promoted to default so a valid fallback always exists after the removal.
:::

### `mosaico profile ls`

```bash
mosaico profile ls [--output table|csv]
```

Lists every configured profile: name, host, port, and whether it's the default. The API key is never printed. CSV rows follow `name,host,port,is_default`.

## Where profiles live

Profiles are stored in a single TOML file, by default at `~/.mosaico/config.toml`:

```toml title="~/.mosaico/config.toml"
[dev]
host = "localhost"
port = 6726
api_key = ""
tls = false
cert_path = ""
default = true

[prod]
host = "api.mosaico.example.com"
port = 6726
api_key = "msk_live_xyz789"
tls = true
cert_path = "/etc/mosaico/certs/ca.pem"
default = false
```

Exactly one profile can have `default = true`; it's the one used whenever a command doesn't say otherwise, and the file can be edited by hand if you prefer.

## Overriding with environment variables

Sometimes you want to override a single connection detail for one shell session, swap in a different API key in CI, point at a different host for a one-off test, without editing the config file or defining a whole new profile. Environment variables let you do that on top of whichever profile gets resolved (named or default): each one overrides only its own field.

| Variable | Description |
| :--- | :--- |
| `MOSAICO_PROFILE` | Name of the profile to use, as an alternative to `--profile`. |
| `MOSAICO_DAEMON_URL` | Overrides the host, e.g. `api.mosaico.dev` or `api.mosaico.dev:6276`. If no port is embedded, the profile's port (or the default, `6726`) is used. |
| `MOSAICO_API_KEY` | Overrides the API key. |
| `MOSAICO_TLS` | Overrides whether TLS is enabled. Accepted truthy values are `1`, `true`, or `yes` (case-insensitive); anything else, including unset, is treated as disabled. |
| `MOSAICO_CERT_PATH` | Overrides the path to a custom TLS CA certificate. |
| `MOSAICO_CONFIG_PATH` | Overrides the configuration file path (default `~/.mosaico/config.toml`). |

Resolution order is:

1. **Pick a profile.** `--profile <NAME>` or `MOSAICO_PROFILE` selects a named profile from the config file; if neither is set, the profile marked `default` is used instead.
2. **Apply field overrides.** Any of the environment variables above that are set replace the corresponding field on top of the profile picked in step 1.

:::note
`sequence` and `topic` subcommands refuse to run, including when you only ask for `--help` on them, unless a profile can be resolved this way. `mosaico sequence --help` and `mosaico profile ...` work with no profile configured at all; `mosaico sequence ls --help` does not.
:::
