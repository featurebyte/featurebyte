"""
CLI tools for managing sample datasets
"""

import base64
import os
import re
import tarfile
import tempfile
from urllib import request

import typer
from bson import ObjectId
from python_on_whales.docker_client import DockerClient
from rich.console import Console
from rich.table import Table

from featurebyte.common.path_util import get_package_root
from featurebyte.logging import get_logger

logger = get_logger(__name__)


console = Console()
datasets_dir = os.path.join(get_package_root(), "datasets")

app = typer.Typer(
    name="dataset",
    help="Manage sample datasets",
    add_completion=False,
)


@app.command(name="list")
def list_datasets() -> None:
    """List datasets available"""
    table = Table(title="Sample Datasets")
    table.add_column("Name", justify="left", style="cyan")
    table.add_column("Url", justify="left")
    table.add_column("Description", justify="left")
    for file_name in os.listdir(datasets_dir):
        if not file_name.endswith("sql"):
            continue
        name = file_name.replace(".sql", "")
        with open(os.path.join(datasets_dir, file_name), encoding="utf8") as file_obj:
            sql = file_obj.read()
        matches = re.findall(r"url:[\s]+(.+)", sql.split("\n")[0])
        url = matches[0] if matches else "N/A"
        matches = re.findall(r"description:[\s]+(.+)", sql.split("\n")[1])
        description = matches[0] if matches else "N/A"
        table.add_row(name, url, description)
    console.print(table)


@app.command(name="import")
def import_dataset(dataset_name: str) -> None:
    """
    Import dataset to local Spark database. Ensure local Spark app is running.
    """

    logger.info(f"Importing Dataset {dataset_name}")

    # check file exists
    path = os.path.join(datasets_dir, f"{dataset_name}.sql")
    if not os.path.exists(path):
        raise FileNotFoundError(path)

    # create temp id for import
    temp_id = str(ObjectId())

    # parse sql
    destination_folder = f"/opt/spark/data/staging/{temp_id}"
    hive_staging_path = f"file://{destination_folder}/{dataset_name}"
    with open(path, encoding="utf8") as file_obj:
        sql = file_obj.read()
        sql = sql.format(staging_path=hive_staging_path)

    # extract url from first line in sql file. e.g.
    # -- url: https://storage.googleapis.com/featurebyte-public-datasets/grocery.tar.gz
    # get contiguous string after "url: " that is unbroken by whitespace
    matches = re.findall(r"url:\s+(\S+)", sql.splitlines()[0])
    url = matches[0] if matches else None

    assert url is not None
    assert url.lower().startswith("https://storage.googleapis.com/featurebyte-public-datasets/")

    # Download and upload contents to spark container
    with tempfile.TemporaryDirectory() as download_folder:
        # download tar file
        archive_file = os.path.join(download_folder, "data.tar.gz")
        logger.info(f"Downloading data file from: {url} -> {archive_file}")
        request.urlretrieve(url, archive_file)  # nosec

        logger.debug(f"Extracting files to staging location: {download_folder}")
        with tarfile.open(archive_file) as file_obj:
            base_objects = filter(
                lambda x: "/" not in x.path, file_obj.getmembers()
            )  # Filter root level paths
            file_obj.extractall(
                download_folder,
                members=filter(
                    lambda x: not x.path.startswith("/") and ".." not in x.path,
                    file_obj.getmembers(),
                ),
            )
        # Delete archive file
        os.remove(archive_file)

        # create temp destination folder in spark container
        DockerClient().execute(
            container="spark-thrift",
            command=["mkdir", destination_folder],
        )
        try:
            # Copy files to spark container
            for base_object in base_objects:
                base_path = base_object.path
                if base_object.isdir():
                    logger.info(
                        f"Copying folder from host:{download_folder}/{base_path} -> spark-thrift:{destination_folder}"
                    )

                    DockerClient().copy(
                        f"{download_folder}/{base_path}",
                        ("spark-thrift", destination_folder),
                    )
                elif base_object.isfile():
                    logger.info(
                        f"Copying file from host:{download_folder}/{base_path} -> spark-thrift:{destination_folder}/{base_path}"
                    )
                    DockerClient().copy(
                        f"{download_folder}/{base_path}",
                        ("spark-thrift", destination_folder),
                    )
                else:
                    raise ValueError(f"Unknown file type: {base_object}")

            # Call featurebyte-server container to import dataset
            sql_b64 = base64.b64encode(sql.encode("utf-8")).decode("utf-8")
            logger.info("Running spark commands to ingest dataset")
            DockerClient().execute(
                container="featurebyte-server",
                command=["python", "-m", "featurebyte.datasets.__main__", sql_b64],
            )
            logger.info("Dataset successfully imported")
        finally:
            # clean up staging folder
            DockerClient().execute(
                container="spark-thrift",
                command=["rm", "-rf", destination_folder],
            )


if __name__ == "__main__":
    app()
