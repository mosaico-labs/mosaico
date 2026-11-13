import typer

from mosaicolabs_cli.commands import lerobot, mcap, ros

app = typer.Typer(no_args_is_help=True)

app.command(name="ros")(ros.inject)
app.command(name="mcap")(mcap.inject)
app.command(name="lerobot")(lerobot.inject)
