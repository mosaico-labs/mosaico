import typer

from mosaicolabs_cli.commands import mcap, ros, ulog

app = typer.Typer(no_args_is_help=True)

app.command(name="ros")(ros.inject)
app.command(name="mcap")(mcap.inject)
app.command(name="ulog")(ulog.inject)
