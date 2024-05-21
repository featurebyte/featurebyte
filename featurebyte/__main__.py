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
    krb5_realm: str = typer.Option(default=None, help="Kerberos realm, eg: ATHENA.MIT.EDU"),
    krb5_kdc: str = typer.Option(
        default=None, help="Kerberos KDC hostname, eg: kerberos.mit.edu:88"
    ),
) -> None:
    """Start application"""
    start_app(app_name, krb5_realm=krb5_realm, krb5_kdc=krb5_kdc)


@app.command(name="playground")
def playground(
    force_import: bool = typer.Option(default=False, help="Import datasets even if they exist"),
    krb5_realm: str = typer.Option(default=None, help="Kerberos realm, eg: ATHENA.MIT.EDU"),
    krb5_kdc: str = typer.Option(
        default=None, help="Kerberos KDC hostname, eg: kerberos.mit.edu:88"
    ),
) -> None:
    """Start playground environment"""
    start_playground(
        force_import=force_import,
        krb5_realm=krb5_realm,
        krb5_kdc=krb5_kdc,
    )


@app.callback(invoke_without_command=True)
def default(ctx: typer.Context) -> None:
    """Invoke default command"""
    if ctx.invoked_subcommand is not None:
        return
    start_app(ApplicationName.FEATUREBYTE)


@app.command(name="stop")
def stop(
    clean: bool = typer.Option(default=False, help="Remove all volumes and containers"),
) -> None:
    """Stop all applications"""
    stop_app(clean=clean)


@app.command(name="logs")
def logs(
    service_name: str = typer.Argument(default="all", help="Name of service to print logs for"),
    tail: int = typer.Argument(
        default=500, help="Number of lines to print from the end of the logs"
    ),
) -> None:
    """Print application logs"""
    print_logs(service_name, tail)


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
