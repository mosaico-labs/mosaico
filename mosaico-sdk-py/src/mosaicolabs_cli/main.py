from typing import Optional

import typer

from mosaicolabs_cli.commands import extension, profile, sequence, topic
from mosaicolabs_cli.commands.extension import MosaicoRouter
from mosaicolabs_cli.utils.env import MosaicoEnv
from mosaicolabs_cli.utils.mosaico_profile import MosaicoProfile

app = typer.Typer(
    cls=MosaicoRouter,
    help="""
    Mosaico CLI — Command line tool to interact with the Mosaico platform

    \b
    ENVIRONMENT VARIABLES:
    MOSAICO_PROFILE      Target profile name selection mapping (e.g., 'dev', 'prod').
    MOSAICO_DAEMON_URL   Mosaico remote daemon host or host:port (e.g., 'api.mosaico.dev' or 'api.mosaico.dev:6276'). When provided the embedded port (if any) will be respected; otherwise the default port is used.
    MOSAICO_API_KEY      Authentication credentials bearer token key.
    MOSAICO_TLS          Enable TLS/SSL connection (set to any value to enable). When enabled, the client will attempt to use TLS for secure communication with the Mosaico server. If MOSAICO_CERT_PATH is provided, it will be used as the CA certificate for verifying the server's TLS certificate. If MOSAICO_TLS is set but MOSAICO_CERT_PATH is not provided, the client will attempt to establish a TLS connection without a custom CA certificate, which may succeed if the server's certificate is signed by a well-known CA trusted by the system.
    MOSAICO_CERT_PATH    Path location pointing to a custom TLS CA certificate file.
    MOSAICO_CONFIG_PATH  Override the default configuration file path (~/.mosaico/config.toml).

    \b
    CONFIGURATION PRECEDENCE:
      1. Explicit Environment Variables (MOSAICO_DAEMON_URL, MOSAICO_API_KEY, MOSAICO_CERT_PATH)
      2. Explicit Environment Variable Profile Selection (MOSAICO_PROFILE)
      3. Inline CLI option overrides (e.g., --profile flag)
      4. Global fallback configuration file profile configuration keys (~/.mosaico/config.toml)
    """,
    no_args_is_help=True,
)


@app.callback()
def main_callback(
    ctx: typer.Context,
    profile_name: Optional[str] = typer.Option(
        None,
        "--profile",
        envvar=MosaicoEnv.PROFILE,
        help="Specify which connection profile to use.",
    ),
):
    """
    Resolve the Mosaico connection profile before any command runs.
    """
    ctx.obj = MosaicoProfile.resolve(profile_name=profile_name)


app.add_typer(profile.app, name="profile", help="Manage connection profiles.")
app.add_typer(sequence.app, name="sequence", help="Manage and list sequences.")
app.add_typer(topic.app, name="topic", help="Manage and list topics.")
app.add_typer(
    extension.app,
    name="extension",
    help="Manage and list external installed extensions.",
)

if __name__ == "__main__":
    app()
