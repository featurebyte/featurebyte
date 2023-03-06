"""
CLI tools for managing sample datasets
"""
import base64
import os
import re
import tarfile
import tempfile
from pathlib import Path
from urllib import request

import typer
from python_on_whales.docker_client import DockerClient
from rich.console import Console
from rich.table import Table

from featurebyte.common.path_util import get_package_root

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
    # check file exists
    path = os.path.join(datasets_dir, f"{dataset_name}.sql")
    if not os.path.exists(path):
        raise FileNotFoundError(path)

    local_staging_basepath = Path("~/.featurebyte/data/spark/staging/datasets").expanduser()
    local_staging_basepath.mkdir(parents=True, exist_ok=True)
    with tempfile.TemporaryDirectory(dir=local_staging_basepath) as local_staging_path:

        # parse sql
        hive_staging_path = f"file:///opt/spark/data/staging/datasets/{os.path.basename(local_staging_path)}/{dataset_name}"
        with open(path, encoding="utf8") as file_obj:
            sql = file_obj.read()
            sql = sql.format(staging_path=hive_staging_path)

        # extract url from first line in sql file. e.g.
        # -- url: https://storage.googleapis.com/featurebyte-public-datasets/grocery.tar.gz
        first_line = sql.split("\n")[0]
        # get contiguous string after "url: " that is unbroken by whitespace
        matches = re.findall(r"url:[\s]+([^\s]+)", first_line)
        if matches:
            # validate url is from featurebyte
            url = matches[0]
            assert url.lower().startswith(
                "https://storage.googleapis.com/featurebyte-public-datasets/"
            )

            with tempfile.TemporaryDirectory() as tempdir:
                # download tar file
                local_path = os.path.join(tempdir, "data.tar.gz")
                console.print(f"Downloading data from: {url} -> {local_path}")
                request.urlretrieve(url, local_path)  # nosec

                # extracting files to staging location
                console.print(f"Extracting files to staging location: {local_staging_path}")
                with tarfile.open(local_path) as file_obj:
                    file_obj.extractall(local_staging_path)

        sql_b64 = base64.b64encode(sql.encode("utf-8")).decode("utf-8")
        for line in DockerClient().execute(  # type:ignore
            container="featurebyte-server",
            command=["python", "-m", "featurebyte.datasets.__main__", sql_b64],
            tty=True,
        ):
            console.print(line)


if __name__ == "__main__":
    app()
