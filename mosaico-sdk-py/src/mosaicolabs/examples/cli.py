import importlib
import logging
import sys

import click

from mosaicolabs.examples import config

# Map command names to their respective module paths
EXAMPLES_MAP = {
    "ros_injection": "mosaicolabs.examples.ros_injection.main",
    "data_inspection": "mosaicolabs.examples.data_inspection",
    "query_catalogs": "mosaicolabs.examples.query_catalogs",
}

CONTEXT_SETTINGS = dict(help_option_names=["-h", "--help"])


@click.command(context_settings=CONTEXT_SETTINGS)
@click.argument(
    "example",
    type=click.Choice(list(EXAMPLES_MAP.keys())),
)
@click.option(
    "--host",
    default="localhost",
    help="The Mosaico Server hostname.",
    show_default=True,
)
@click.option(
    "--port", default=6726, type=int, help="The Mosaico Server port.", show_default=True
)
@click.option(
    "--log-level",
    "-l",
    type=click.Choice(["DEBUG", "INFO", "WARNING", "ERROR"], case_sensitive=False),
    default="INFO",
    help="Set the logging level.",
    show_default=True,
)
def run_example_cli(example, host, port, log_level):
    """
    Mosaico SDK Examples Runner.

    This utility allows you to run official SDK examples with custom connection settings.
    """
    # Set global logging configuration
    logging.basicConfig(level=getattr(logging, log_level.upper()))

    # Inject CLI parameters into the config module
    # This ensures that when examples import MOSAICO_HOST, they get the CLI value
    config.MOSAICO_HOST = host
    config.MOSAICO_PORT = port

    click.secho(f"Launching example: {example}", fg="cyan", bold=True)
    click.echo(f"Target: {host}:{port}\n")

    try:
        # Dynamically import the chosen example module
        module_path = EXAMPLES_MAP[example]
        example_module = importlib.import_module(module_path)

        # Detect the entry point (checks for 'main')
        entry_point = getattr(example_module, "main", None)

        if entry_point:
            entry_point()
        else:
            click.secho(
                f"Error: No 'main' function found in {module_path}",
                fg="red",
            )
            sys.exit(1)

    except Exception as e:
        click.secho(f"\nExecution failed: {e}", fg="red", bold=True)
        logging.exception("Detailed stack trace:")
        sys.exit(1)


if __name__ == "__main__":
    run_example_cli()
