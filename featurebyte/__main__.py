"""
Featurebyte CLI tools
"""
from typing import Generator

import os
import pwd
import tempfile
from contextlib import contextmanager
from enum import Enum

import typer
from python_on_whales.docker_client import DockerClient
from rich.console import Console
from rich.table import Table

from featurebyte import version
from featurebyte.common.path_util import get_package_root
from featurebyte.datasets.app import app as datasets_app


class ApplicationName(str, Enum):
    """
    Enum for application names
    """

    FEATUREBYTE = "featurebyte"
    SPARK = "spark"


app = typer.Typer(
    name="featurebyte",
    help="Manage featurebyte services",
    add_completion=False,
)
app.add_typer(datasets_app, name="datasets")

services_map = {
    ApplicationName.FEATUREBYTE: ["featurebyte-server", "mongo-rs"],
    ApplicationName.SPARK: ["spark-thrift"],
}
console = Console()


@contextmanager
def get_docker_client(app_name: ApplicationName) -> Generator[DockerClient, None, None]:
    """
    Get docker client

    Parameters
    ----------
    app_name: ApplicationName
        Name of application to get docker client for

    Yields
    -------
    Generator[DockerClient, None, None]
    """
    with tempfile.TemporaryDirectory() as temp_dir:
        compose_env_file = os.path.join(temp_dir, ".env")
        uid = os.getuid()
        user = pwd.getpwuid(uid)
        with open(compose_env_file, "w", encoding="utf8") as file_obj:
            file_obj.write(f'LOCAL_UID="{uid}" LOCAL_GID="{user.pw_gid}"')
        docker = DockerClient(
            compose_project_name=app_name.value,
            compose_files=[os.path.join(get_package_root(), f"docker/{app_name}.yml")],
            compose_env_file=compose_env_file,
        )
        yield docker


def print_logs(app_name: ApplicationName, tail: int) -> None:
    """
    Print service logs

    Parameters
    ----------
    app_name: ApplicationName
        Application name
    tail: int
        Number of lines to print
    """
    with get_docker_client(app_name) as docker:
        docker_logs = docker.compose.logs(follow=True, stream=True, tail=str(tail))
        docker_service = services_map[app_name][0]
        for source, content in docker_logs:
            line = content.decode("utf-8").strip()
            if not line.startswith(docker_service):
                continue
            if source == "stdout":
                console.print(f"[green]{line}[/]")
            else:
                console.print(f"[red]{line}[/]")


@app.command(name="start")
def start(
    app_name: ApplicationName = typer.Argument(
        default="featurebyte", help="Name of application to start"
    )
) -> None:
    """Start application"""
    with get_docker_client(app_name) as docker:
        docker.compose.up(detach=True)


@app.command(name="stop")
def stop(
    app_name: ApplicationName = typer.Argument(
        default="featurebyte", help="Name of application to stop"
    )
) -> None:
    """Stop application"""
    with get_docker_client(app_name) as docker:
        docker.compose.down()


@app.command(name="logs")
def logs(
    app_name: ApplicationName = typer.Argument(
        default="featurebyte", help="Name of application to print logs for"
    ),
    tail: int = typer.Argument(
        default=500, help="Number of lines to print from the end of the logs"
    ),
) -> None:
    """Print application logs"""
    print_logs(app_name, tail)


@app.command(name="status")
def status() -> None:
    """Get service status"""
    table = Table(title="Service Status")
    table.add_column("App", justify="left", style="cyan")
    table.add_column("Name", justify="left", style="cyan")
    table.add_column("Status", justify="center")
    table.add_column("Health", justify="center")
    for app_name in ApplicationName:
        with get_docker_client(ApplicationName(app_name)) as docker:
            containers = docker.compose.ps()
            for container in containers:
                health = container.state.health.status if container.state.health else "N/A"
                app_health = (
                    f"[green]{health}[/]"
                    if health == "healthy"
                    else (f"[red]{health}[/]" if health != "N/A" else health)
                )
                app_status = (
                    f"[green]{container.state.status}[/]"
                    if container.state.status == "running"
                    else f"[red]{container.state.status}[/]"
                )
                table.add_row(app_name, container.name, app_status, app_health)
    console.print(table)


@app.command(name="version")
def print_version() -> None:
    """Print featurebyte version"""
    console.print(f"[green]featurebyte[/] version: [bold yellow]{version}[/]")


if __name__ == "__main__":
    app()
