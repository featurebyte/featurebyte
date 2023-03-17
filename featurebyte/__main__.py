"""
Featurebyte CLI tools
"""
import typer
from rich.text import Text

from featurebyte import version
from featurebyte.datasets.app import app as datasets_app
from featurebyte.docker.manager import (
    ApplicationName,
    console,
    get_status,
    print_logs,
    start_app,
    start_playground,
    stop_app,
)

app = typer.Typer(
    name="featurebyte",
    help="Manage featurebyte services",
    add_completion=False,
)
app.add_typer(datasets_app, name="datasets")


@app.command(name="start")
def start(
    app_name: ApplicationName = typer.Argument(
        default="featurebyte", help="Name of application to start"
    ),
    local: bool = typer.Option(default=False, help="Do not pull new images from registry"),
) -> None:
    """Start application"""
    start_app(app_name, local)


@app.command(name="playground")
def playground(
    local: bool = typer.Option(default=False, help="Do not pull new images from registry"),
) -> None:
    """Start playground environment"""
    start_playground(local)


@app.callback(invoke_without_command=True)
def default(ctx: typer.Context) -> None:
    """Invoke default command"""
    if ctx.invoked_subcommand is not None:
        return
    start_app(ApplicationName.FEATUREBYTE, False)


@app.command(name="stop")
def stop(
    app_name: ApplicationName = typer.Argument(
        default="featurebyte", help="Name of application to stop"
    )
) -> None:
    """Stop application"""
    stop_app(app_name)


@app.command(name="logs")
def logs(
    app_name: ApplicationName = typer.Argument(
        default="featurebyte", help="Name of application to print logs for"
    ),
    service_name: str = typer.Argument(default="all", help="Name of service to print logs for"),
    tail: int = typer.Argument(
        default=500, help="Number of lines to print from the end of the logs"
    ),
) -> None:
    """Print application logs"""
    print_logs(app_name, service_name, tail)


@app.command(name="status")
def status() -> None:
    """Get service status"""
    get_status()


@app.command(name="version")
def print_version() -> None:
    """Print featurebyte version"""
    console.print(Text("featurebyte ", style="cyan") + Text(version, style="bold green"))


if __name__ == "__main__":
    app()
