import typer

from mosaicolabs_cli.commands import mcap, ros

app = typer.Typer(no_args_is_help=True)

app.command(name="ros")(ros.extract)
app.command(name="mcap")(mcap.extract)
