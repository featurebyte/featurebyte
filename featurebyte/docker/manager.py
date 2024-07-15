"""
Featurebyte tools for managing docker containers
"""
from typing import Generator, List, Optional

import os
import sys
import tempfile
import time
from contextlib import contextmanager
from enum import Enum

from python_on_whales import DockerException
from python_on_whales.docker_client import DockerClient
from rich.align import Align
from rich.console import Console
from rich.panel import Panel
from rich.status import Status
from rich.table import Table
from rich.text import Text

from featurebyte.api.feature_store import FeatureStore, SourceType
from featurebyte.common.path_util import get_package_root
from featurebyte.config import Configurations
from featurebyte.datasets.app import import_dataset
from featurebyte.exception import DockerError
from featurebyte.logging import get_logger
from featurebyte.query_graph.node.schema import SparkDetails

logger = get_logger(__name__)


class ApplicationName(str, Enum):
    """
    Enum for application names
    """
    FEATUREBYTE = "featurebyte"
    SPARK = "spark"

console = Console()


@contextmanager
def get_docker_client(krb5_realm: Optional[str] = None,
                      krb5_kdc: Optional[str] = None) -> Generator[DockerClient, None, None]:
    """
    Get docker client

    Parameters
    ----------
    krb5_realm : Optional[str]
        Kerberos realm, by default None
    krb5_kdc : Optional[str]
        Kerberos kdc of the krb5_realm, by default None

    Yields
    -------
    Generator[DockerClient, None, None]
        Docker client
    """
    # Set as empty strings if None
    krb5_realm = krb5_realm if krb5_realm is not None else ""
    krb5_kdc = krb5_kdc if krb5_kdc else ""

    with tempfile.TemporaryDirectory() as temp_dir:
        compose_env_file = os.path.join(temp_dir, ".env")
        env_file_lines = [
            f"KRB5_REALM={krb5_realm}\n",
            f"KRB5_KDC={krb5_kdc}\n",
        ]
        if sys.platform != "win32":
            import pwd
            uid = os.getuid()
            user = pwd.getpwuid(uid)
            env_file_lines.extend(
                [
                    f'LOCAL_UID="{uid}"\n',
                    f'LOCAL_GID="{user.pw_gid}"\n',
                ]
            )
        log_level = os.environ.get("LOG_LEVEL", "INFO")
        env_file_lines.append(f'LOG_LEVEL="{log_level}"\n')
        with open(compose_env_file, "w", encoding="utf8") as file_obj:
            file_obj.writelines(env_file_lines)
        docker = DockerClient(
            compose_project_name="featurebyte",
            compose_files=[os.path.join(get_package_root(), "docker/featurebyte.yml")],
            compose_env_file=compose_env_file,
        )
        yield docker


def get_service_names(app_name: ApplicationName) -> List[str]:
    """
    Get list of service names for application

    Parameters
    ----------
    app_name: ApplicationName
        Name of application to get service names for

    Raises
    ------
    ValueError
        If application name is not valid

    Returns
    -------
    List[str]
    """
    if app_name == ApplicationName.FEATUREBYTE:
        return ["featurebyte-server", "featurebyte-worker"]
    if app_name == ApplicationName.SPARK:
        return ["spark-thrift"]
    raise ValueError("Not a valid application name")


def __setup_network() -> None:
    """
    Setup docker network
    """
    client = DockerClient()
    networks = client.network.list()
    if "featurebyte" in map(lambda x: x.name, networks):
        logger.debug("featurebyte network already exists")
    else:
        logger.debug("featurebyte network creating")
        DockerClient().network.create("featurebyte", driver="bridge")


def print_logs(service_name: str, tail: int) -> None:
    """
    Print service logs

    Parameters
    ----------
    service_name: str
        Service name
    tail: int
        Number of lines to print
    """
    with get_docker_client() as docker:
        docker_logs = docker.compose.logs(follow=True, stream=True, tail=str(tail))
        for source, content in docker_logs:
            line = content.decode("utf-8").strip()
            if service_name != "all" and not line.startswith(service_name):
                continue
            style = "green" if source == "stdout" else "red"
            console.print(Text(line, style=style))


def start_app(
    app_name: ApplicationName,
    krb5_realm: Optional[str] = None,
    krb5_kdc: Optional[str] = None,
    services: List[str] = None,
    verbose: bool = True,
) -> None:
    """
    Start application

    Parameters
    ----------
    app_name : ApplicationName
        Application name
    krb5_realm : Optional[str]
        Kerberos realm, by default None
    krb5_kdc : Optional[str]
        Kerberos kdc of the krb5_realm, by default None
    services : List[str]
        List of services to start
    verbose : bool
        Print verbose output

    Raises
    ------
    DockerError
        Docker compose fails to start
    """
    try:
        services = services or get_service_names(app_name)

        # Load config to ensure it exists before starting containers
        Configurations()
        __setup_network()

        with get_docker_client(krb5_realm=krb5_realm, krb5_kdc=krb5_kdc) as docker:
            docker.compose.up(services=services, detach=True)

            # Wait for all services to be healthy
            def _wait_for_healthy(spinner_status: Optional[Status] = None) -> None:
                while True:
                    unhealthy_containers = []
                    for container in docker.compose.ps():
                        health = container.state.health.status if container.state.health else "N/A"
                        if health not in {"healthy", "N/A"}:
                            unhealthy_containers.append(container.name)
                    if len(unhealthy_containers) == 0:
                        break
                    statuses = (
                        Text("Services: [")
                        + Text(", ").join(map(lambda s: Text(s, style="red"), unhealthy_containers))
                        + Text("] are unhealthy", style="None")
                    )
                    if spinner_status:
                        spinner_status.update(statuses)
                    time.sleep(1)

            if verbose:
                with console.status("Waiting for services to be healthy...") as spinner_status:
                    _wait_for_healthy(spinner_status)
                    message = (
                        """
                        [bold green]Featurebyte application started successfully![/]

                        API server now running at: [cyan underline]http://127.0.0.1:8088[/].
                        You can now use the featurebyte SDK with the API server.
                        """
                    )
                console.print(Panel(Align.left(message), title="FeatureByte Services", width=120))
            else:
                _wait_for_healthy()
    except DockerException as exc:
        if "Is the docker daemon running" in str(exc):
            raise DockerError("Docker is not running, please start docker and try again.") from exc
        raise DockerError(f"Failed to start application: {exc.stderr}") from exc

def start_playground(datasets: Optional[List[str]] = None,
                     krb5_realm: Optional[str] = None,
                     krb5_kdc: Optional[str] = None,
                     force_import: bool = False,
                     verbose: bool = True) -> None:
    """
    Start featurebyte playground environment

    Parameters
    ----------
    datasets : Optional[List[str]]
        List of datasets to import, by default None (import all datasets)
    krb5_realm : Optional[str]
        Kerberos realm, by default None
    krb5_kdc : Optional[str]
        Kerberos kdc of the krb5_realm, by default None
    force_import : bool
        Import datasets even if they are already imported, by default False
    verbose : bool
        Print verbose output, by default True
    """
    num_steps = 4

    # determine services to start
    services = get_service_names(ApplicationName.FEATUREBYTE) + get_service_names(ApplicationName.SPARK)

    step = 1
    logger.info(f"({step}/{num_steps}) Starting featurebyte services")
    start_app(ApplicationName.FEATUREBYTE, krb5_realm=krb5_realm, krb5_kdc=krb5_kdc, services=services, verbose=verbose)
    step += 1

    # create local spark feature store
    logger.info(f"({step}/{num_steps}) Creating local spark feature store")
    Configurations().use_profile("local")
    feature_store = FeatureStore.get_or_create(
        name="playground",
        source_type=SourceType.SPARK,
        details=SparkDetails(
            host="spark-thrift",
            http_path="cliservice",
            port=10000,
            storage_type="file",
            storage_url="/data/staging/playground",
            storage_path="file:///opt/spark/data/staging/playground",
            catalog_name="spark_catalog",
            schema_name="playground",
        ),
    )
    step += 1

    # import datasets
    logger.info(f"({step}/{num_steps}) Import datasets")
    existing_datasets = feature_store.get_data_source().list_schemas(database_name="spark_catalog")
    datasets = datasets or ["grocery", "healthcare", "creditcard"]
    for dataset in datasets:
        if not force_import and dataset in existing_datasets:
            logger.info(f"Dataset {dataset} already exists, skipping import")
            continue
        logger.info(f"Importing dataset: {dataset}")
        import_dataset(dataset)
    step += 1

    logger.info(f"({step}/{num_steps}) Playground environment started successfully. Ready to go! 🚀")

def stop_app(clean: Optional[bool] = False, verbose: Optional[bool] = True) -> None:
    """
    Stop application

    Parameters
    ----------
    clean : Optional[bool]
        Whether to clean up all data, by default False
    verbose : Optional[bool]
        Print verbose output
    """
    with get_docker_client() as docker:
        if clean:
            docker.compose.down(remove_orphans=True, volumes=True)
        else:
            docker.compose.down()
    if verbose:
        message = (
        """
        [bold green]Featurebyte application stopped.[/]
        """
        )
        console.print(Panel(Align.left(message), title="FeatureByte Service", width=120))


def get_status() -> None:
    """Get service status"""
    table = Table(title="Service Status", width=100)
    table.add_column("Name", justify="left", style="cyan")
    table.add_column("Status", justify="center")
    table.add_column("Health", justify="center")
    health_colors = {
        "healthy": "green",
        "starting": "yellow",
        "unhealthy": "red",
    }
    status_colors = {
        "running": "green",
        "exited": "red",
    }

    with get_docker_client() as docker:
        containers = docker.compose.ps()
        for container in containers:
            health = container.state.health.status if container.state.health else "N/A"
            app_health = Text(health, health_colors.get(health, "yellow"))
            app_status = Text(
                container.state.status, status_colors.get(container.state.status, "yellow")
            )
            table.add_row(container.name, app_status, app_health)
    console.print(table)
