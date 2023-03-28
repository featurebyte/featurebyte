"""
Featurebyte tools for managing docker containers
"""
from typing import Generator, List, Optional

import os
import pwd
import sys
import tempfile
import time
import uuid
from contextlib import contextmanager
from enum import Enum

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
from featurebyte.logger import logger
from featurebyte.query_graph.node.schema import SparkDetails


class ApplicationName(str, Enum):
    """
    Enum for application names
    """
    FEATUREBYTE = "featurebyte"
    SPARK = "spark"
    DOCS = "docs"


# Application messages
messages = {
    ApplicationName.FEATUREBYTE: {
        "start": (
            """
            [bold green]Featurebyte application started successfully![/]

            API server now running at: [cyan underline]http://127.0.0.1:8088[/].
            You can now use the featurebyte SDK with the API server.
            """
        ),
        "stop": (
            """
            [bold green]Featurebyte application stopped.[/]
            """
        ),
    },
    ApplicationName.SPARK: {
        "start": (
            """
            [bold green]Spark application started successfully![/]

            Spark thrift server now running at: [cyan underline]http://localhost:10000[/].
            You can now use the Spark thrift server for a FeatureStore in the featurebyte SDK.
            """
        ),
        "stop": (
            """
            [bold green]Spark application stopped.[/]
            """
        ),
    },
    ApplicationName.DOCS: {
        "start": (
            """
            [bold green]Docs application started successfully![/]

            Documentation server now running at: [cyan underline]http://localhost:8089[/].
            You can now view the documentation at the above URL.
            """
        ),
        "stop": (
            """
            [bold green]Docs application stopped.[/]
            """
        ),
    },
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
        Docker client
    """
    with tempfile.TemporaryDirectory() as temp_dir:
        compose_env_file = os.path.join(temp_dir, ".env")
        if sys.platform != "win32":
            uid = os.getuid()
            user = pwd.getpwuid(uid)
            with open(compose_env_file, "w", encoding="utf8") as file_obj:
                file_obj.write(f'LOCAL_UID="{uid}"\nLOCAL_GID="{user.pw_gid}"\n')
            docker = DockerClient(
                compose_project_name=app_name.value,
                compose_files=[os.path.join(get_package_root(), "docker/featurebyte.yml")],
                compose_env_file=compose_env_file,
            )
        else:
            docker = DockerClient(
                compose_project_name=app_name.value,
                compose_files=[os.path.join(get_package_root(), "docker/featurebyte.yml")],
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
    if app_name == ApplicationName.DOCS:
        return ["featurebyte-docs"]
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


def print_logs(app_name: ApplicationName, service_name: str, tail: int) -> None:
    """
    Print service logs

    Parameters
    ----------
    app_name: ApplicationName
        Application name
    service_name: str
        Service name
    tail: int
        Number of lines to print
    """
    with get_docker_client(app_name) as docker:
        docker_logs = docker.compose.logs(follow=True, stream=True, tail=str(tail))
        for source, content in docker_logs:
            line = content.decode("utf-8").strip()
            if service_name != "all" and not line.startswith(service_name):
                continue
            style = "green" if source == "stdout" else "red"
            console.print(Text(line, style=style))


### WARNING THIS NEEDS TO BE REMOVED AFTER BETA ###
def __backup_docker_conf() -> None:
    # If docker folder does not exist, create it
    docker_folder = os.path.expanduser("~/.docker")
    if not os.path.isdir(docker_folder):
        os.mkdir(docker_folder)

    docker_cfg = os.path.expanduser("~/.docker/config.json")
    docker_cfg_old = os.path.expanduser("~/.docker/config.json.old")

    # If docker config file does not exist, create it
    if not os.path.isfile(docker_cfg):
        with open(docker_cfg, "w", encoding="utf-8") as docker_cfg_file:
            docker_cfg_file.write("{}")

    # Backup docker config file
    with open(docker_cfg, encoding="utf-8") as docker_cfg_file:
        with open(docker_cfg_old, "w", encoding="utf-8") as backup_file:
            backup_file.write(docker_cfg_file.read())


def __use_docker_svc_account() -> None:
    with open(
        os.path.expanduser("~/.docker/config.json"), "w", encoding="utf-8"
    ) as docker_cfg_file:
        # pylint: disable=line-too-long
        docker_cfg_file.write(
            """
        {
          "auths": {
            "us-central1-docker.pkg.dev": {
              "auth": "X2pzb25fa2V5X2Jhc2U2NDpld29nSUNKMGVYQmxJam9nSW5ObGNuWnBZMlZmWVdOamIzVnVkQ0lzQ2lBZ0luQnliMnBsWTNSZmFXUWlPaUFpWW1WMFlTMTBaWE4wYVc1bkxUTTNNemt3TnlJc0NpQWdJbkJ5YVhaaGRHVmZhMlY1WDJsa0lqb2dJalkzWmpCbU5EVm1aVFJsT0RVNE9XUXlNemszWXpka01qVTFOalV3WlRVMlltRTBNVEU1T1RjaUxBb2dJQ0p3Y21sMllYUmxYMnRsZVNJNklDSXRMUzB0TFVKRlIwbE9JRkJTU1ZaQlZFVWdTMFZaTFMwdExTMWNiazFKU1VWMlVVbENRVVJCVGtKbmEzRm9hMmxIT1hjd1FrRlJSVVpCUVZORFFrdGpkMmRuVTJwQlowVkJRVzlKUWtGUlEyZFNSWEpJTUcxR09GQk1NRVJjYm5Sek9YVkJjbUl5UWxSV1ZFbzBRMGN6UmxSNFNEaE5hRkZQVEd0UlJGSjVhRFUxTTI5RWRsazNXSFpCUzJwUWRYZG9ibWxFY0U5YVZFRkRNVE4yYVRGY2JqRnRZVmRTUzFSRWVqRXdSRTV5YldGWldqWkthaXRaVmk4d2JEaHdjSEp6UTFKS2QwbDBTbFpRVUdsbmJuVjVhRUZJZVVFMU16VktSbFZSWlZNeWIyVmNibVo2ZVhkNlFteG5ZMWhuVlc5clNsb3hjVXR6YkRSYVpsVTFSbVJsVW5Ob1NXWkpaemt5Y2l0RlNFeGxNSGh1UVVKak9YcE1iM1ZvT1VoSFVUUm5TM0JjYmxCcE4wbDFUME5XVkZwWlR6QlhZbEZUY2pCMllYaGhRa0ZhWVZwQ2VXSktiVU5hVWpsSE1tTXJWemRWVFcxSVlqSjNWbGtyYjNwdlNsSnVObkJQUmpOY2JtbHVUbHBzYzFoYVRXbE5XbFZHUXpGd1VIbEZjWGg0YjBnd05WSjRNRzVzYjJrNFZXVnFNM05RZWpaRFp6QkJNbXR6Y21kb1ZVbDFja05DUjFWNFJXVmNialZzUmtGSE5FcFVRV2ROUWtGQlJVTm5aMFZCVXpoeWQwOUtOMmRXYjBwakt6RkJkSFZtSzFSeGFtRmtWMDVYUTBoWlZTdHpTV1pQV0hOQlUySldTMFpjYmpKNlNWTXJZM3AyT0VzcmFrTnBZVXhTT1hSSmNGWlZlV0pXYkRWeWIydHFkM000UWpGblptaHBaVGRpZEVsVWFHTnpZU3N4U25sWFpXZHRkM0JqUVdWY2JqTlJhVkJVVkZCVGJEbG5UbFowZWtJeVNWSnFNMUY0WkdRd0sxVXhkVlJuWVRCeVQwWXlhREpwWkhOMmNVVmFlVXhaTVhCbmEycHZiR1JXZFVacE9VeGNibnB3VTBGa2FFSjJlRFUxUlc0eFZsWnJZamhrUVhjMFZteGlaV0l6UTJFdkwwSllWSFEzVEZWemRUWktOVnBLWlZGd1oyZHFiVmwzZW5CSmFHeG5hMGhjYm1WU1ZsSkpRVE01UkZnM2RFaHBlWEZ0YlRsaGJUaE1LMFZXUVZSWlFqRXlTVXcxVW1oTlpVdzFlU3N2T1RkT1J6Rk1RWGcwVEdwUmVYbEZURzVFTXpGY2JuTTFXbXgzU0hkaEwyVnZPRlZFVVZVMVQzUnBWbUZ5ZUhSUldpdFJNVE5yTTB0QlJXWkRkR1UyVVV0Q1oxRkVUakJwZUcxMGRVUlpZVGhYTkc5SmRVMWNibXR2YVRJNFYxUlZiMmN2TWxKelJEVXhPRXRITUV0TmJHNDFUbGhRVXpaMVlrWXJWemRLTTNGUk1uWlhZM0ZYUW5KeE5EZFFaRWhMTm1sNmJ6UnlNbVZjYm1kcVlrdEdTa2h4WVd0TFdDdHlOM2huYm1oSVJXWjBRa1ZJYWsxRWFIbEVUbXBJTld0NWExaE5lRXgwYVdKeFUwTllMMFpDYURSaGIwdE1SVk5NUmxKY2JsRlVUR0V6UW1weWExWkRjM0ExV2tsSGFGUTBVM01yU1cxUlMwSm5VVVJJVm5aWVdtdEdNWEpEVWxZMmFucFhXRmwwYkRneVNHeEdaM1pzVURacmVFbGNia2RCVkdzM2VtRnNabHBFZDJoM2IwdEZWVGhxU0dsNmMxQk5VMHhyUVZCWWNtTTNSbFJ6VFhOREt6QnhVRTkwWXlzd1dWSjVVbk5OVDFFMFEzUjFNbkJjYm10Tk1ETXdSU3RNU2pWTU0zaFpVVGxyVTNGVWIwaFpVRmhuUzFGUVYyUXZaVzFrV2tKWEt6SjFibTQyWjBWSFprbExaR1Y2VTJGRk9HSktSVzFxUzBoY2JqVjNNelZPY0ZaYWVYZExRbWRSUXpKNGNHdEdhMmQyTUZKVGFYUjJNbmxPY0dsa1oyaDBabkVyUVhoRGQxTjRiamgwU0dReFVFODRNbTkzUnpSa09VNWNibkZDU0haUWFYTjVUMVJPVUcxcWIyVkJkR3BaTlZKdmEwZ3paRWxVVDNKc1JUSXZRMWg1WjBKVGRVaGxaM2c0UkRZNFFWTktVV0pEUkdaWGIwTkVVVXhjYmpGYWJteHlNbXBFU1VkclJIQjBZVmhFTTFveWNXVnpZamRIUkNzdlEzSjZTa2h2WWpNeU9FWTJkVTFCYjBWaFZuVjNkVFpRZGpCNmNWRkxRbWRJY0VaY2JsUmxTRmx0YlZOWlIzUlBiRmRwVERCdlEwUnFRekptYlRCYVVVTlVRelJCYUdGblJGUjNjWFJtWVZRM2Rqa3lla0pyTm5STFRFeDZRbm92ZGpReGREbGNia2xvUzBkdlJrczFUV2N3ZUdSMFJDc3dPV29ySzFKa1JFOHJVRkZZZGtWbFpqZEtXR3hUVEhVNFJXZEpiMGhsTDFCc1ZqRklSbGRMZGxwdVMwRkdNRU5jYmsxTVp6ZEVSelJTTjJSWlNYRTNaVEp4VGsxUGJGTXdRWElyT1VOblprazBaVXR4TDI0eFlVaEJiMGRCVmxNcmFuZ3plamR0YTB4SGFtMUJTM3BTWTJaY2JsSTRkelJCZEhGYVNXMWpRbXRsY0hKak0zTlRMM1JXTnpWS1EweE5VREZTY1VoWVVUQkpSRzFETmtoTVdYcFpkWFprYVVNeU1rNDJPRFV3UkVkUVdEQmNibTA0UkdaUlYwRm1UV3d6WjFWMGFXNDBkREkzYzB4dmFXdDVOVE42UVUxUVpYTTBhMFk1TDBrNFl6VmFWVU5QTUVvclUyVmtRV0k0UTFWNVYwSndkRGhjYmk5SFRXaDJWR2xRUVRSWFNUaGxiamhtVFd0a0t6UnpQVnh1TFMwdExTMUZUa1FnVUZKSlZrRlVSU0JMUlZrdExTMHRMVnh1SWl3S0lDQWlZMnhwWlc1MFgyVnRZV2xzSWpvZ0ltSmxkR0V0WVhKMGFXWmhZM1F0Y21WbmFYTjBjbmt0Y2tCaVpYUmhMWFJsYzNScGJtY3RNemN6T1RBM0xtbGhiUzVuYzJWeWRtbGpaV0ZqWTI5MWJuUXVZMjl0SWl3S0lDQWlZMnhwWlc1MFgybGtJam9nSWpFeE1qa3lPVEUzT0RRME5UZzFOREEzTURNME1pSXNDaUFnSW1GMWRHaGZkWEpwSWpvZ0ltaDBkSEJ6T2k4dllXTmpiM1Z1ZEhNdVoyOXZaMnhsTG1OdmJTOXZMMjloZFhSb01pOWhkWFJvSWl3S0lDQWlkRzlyWlc1ZmRYSnBJam9nSW1oMGRIQnpPaTh2YjJGMWRHZ3lMbWR2YjJkc1pXRndhWE11WTI5dEwzUnZhMlZ1SWl3S0lDQWlZWFYwYUY5d2NtOTJhV1JsY2w5NE5UQTVYMk5sY25SZmRYSnNJam9nSW1oMGRIQnpPaTh2ZDNkM0xtZHZiMmRzWldGd2FYTXVZMjl0TDI5aGRYUm9NaTkyTVM5alpYSjBjeUlzQ2lBZ0ltTnNhV1Z1ZEY5NE5UQTVYMk5sY25SZmRYSnNJam9nSW1oMGRIQnpPaTh2ZDNkM0xtZHZiMmRzWldGd2FYTXVZMjl0TDNKdlltOTBMM1l4TDIxbGRHRmtZWFJoTDNnMU1Ea3ZZbVYwWVMxaGNuUnBabUZqZEMxeVpXZHBjM1J5ZVMxeUpUUXdZbVYwWVMxMFpYTjBhVzVuTFRNM016a3dOeTVwWVcwdVozTmxjblpwWTJWaFkyTnZkVzUwTG1OdmJTSUtmUW89Cg=="
            }
          }
        }
        """
        )


def __restore_docker_conf() -> None:
    if os.path.isfile(os.path.expanduser("~/.docker/config.json.old")):
        with open(os.path.expanduser("~/.docker/config.json.old"), encoding="utf-8") as backup_file:
            with open(
                os.path.expanduser("~/.docker/config.json"), "w", encoding="utf-8"
            ) as docker_cfg_file:
                docker_cfg_file.write(backup_file.read())


def __delete_docker_backup() -> None:
    if os.path.isfile(os.path.expanduser("~/.docker/config.json.old")):
        os.remove(os.path.expanduser("~/.docker/config.json.old"))


### END WARNING ###


def start_app(
    app_name: ApplicationName,
    local: bool,
    verbose: bool = True,
) -> None:
    """
    Start application

    Parameters
    ----------
    app_name : ApplicationName
        Name of application to start
    local : bool
        Do not pull new images from registry
    verbose : bool
        Print verbose output
    """
    try:
        # load config to ensure it exists before starting containers
        config = Configurations()
        __setup_network()
        __backup_docker_conf()
        __use_docker_svc_account()
        with get_docker_client(app_name) as docker:
            if not local:
                docker.compose.pull()
            __restore_docker_conf()  # Restore as early as possible

            # Set telemetry id to be passed to container
            try:
                os.environ["FEATUREBYTE_TELEMETRY_ID"] = ":".join(
                    [f"{(uuid.getnode() >> ele) & 0xff:02x}" for ele in range(0, 8 * 6, 8)][::-1]
                )
            except Exception:  # pylint: disable=broad-except
                pass

            docker.compose.up(services=get_service_names(app_name), detach=True)

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
                if app_name in messages:
                    console.print(Panel(Align.left(messages[app_name]["start"]), title=app_name.value, width=120))
            else:
                _wait_for_healthy()

    finally:
        __restore_docker_conf()
        __delete_docker_backup()


def start_playground(local: bool = False, datasets: Optional[List[str]] = None, docs_enabled: bool = True,
                     force_import: bool = False) -> None:
    """
    Start featurebyte playground environment

    Parameters
    ----------
    local : bool
        Do not pull new images from registry, by default False
    datasets : Optional[List[str]]
        List of datasets to import, by default None (import all datasets)
    docs_enabled : bool
        Enable documentation service, by default True
    force_import : bool
        Import datasets even if they are already imported, by default False
    """
    logger.info("Starting featurebyte service")
    start_app(ApplicationName.FEATUREBYTE, local=local, verbose=False)
    logger.info("Starting local spark service")
    start_app(ApplicationName.SPARK, local=local, verbose=False)
    if docs_enabled and not local:
        logger.info("Starting documentation service")
        start_app(ApplicationName.DOCS, local=local, verbose=False)

    # create local spark feature store
    logger.info("Creating local spark feature store")
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
            storage_spark_url="file:///opt/spark/data/staging/playground",
            featurebyte_catalog="spark_catalog",
            featurebyte_schema="playground",
        ),
    )

    # import datasets
    existing_datasets = feature_store.get_data_source().list_schemas(database_name="spark_catalog")
    datasets = datasets or ["grocery", "healthcare", "creditcard"]
    for dataset in datasets:
        if not force_import and dataset in existing_datasets:
            logger.info(f"Dataset {dataset} already exists, skipping import")
            continue
        logger.info(f"Importing dataset: {dataset}")
        import_dataset(dataset)


def stop_app(
    app_name: ApplicationName,
    verbose: Optional[bool] = True,
) -> None:
    """
    Stop application

    Parameters
    ----------
    app_name : ApplicationName
        Name of application to stop
    verbose : Optional[bool]
        Print verbose output
    """
    with get_docker_client(app_name) as docker:
        docker.compose.down()
    if verbose and app_name in messages:
        console.print(Panel(Align.left(messages[app_name]["stop"]), title=app_name.value, width=120))


def get_status() -> None:
    """Get service status"""
    table = Table(title="Service Status", width=120)
    table.add_column("App", justify="left", style="cyan")
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

    for app_name in ApplicationName:
        with get_docker_client(ApplicationName(app_name)) as docker:
            containers = docker.compose.ps()
            for container in containers:
                health = container.state.health.status if container.state.health else "N/A"
                app_health = Text(health, health_colors.get(health, "yellow"))
                app_status = Text(
                    container.state.status, status_colors.get(container.state.status, "yellow")
                )
                table.add_row(app_name, container.name, app_status, app_health)
    console.print(table)
