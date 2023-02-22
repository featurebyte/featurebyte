import argparse
import os
import re
import tarfile
import tempfile
from pathlib import Path
from urllib import request

from featurebyte.session.hive import HiveConnection


def list_datasets() -> None:
    """
    List datasets available
    """
    for file_name in os.listdir(os.path.join(os.path.dirname(__file__), "datasets")):
        if file_name.endswith("sql"):
            print(file_name.replace(".sql", ""))


def upload_dataset(dataset_name: str) -> None:
    """
    Upload dataset to local Spark datastore.
    Ensure local Spark container is running.

    Parameters
    ----------
    dataset_name: str
        Name of dataset to upload

    Raises
    -------
    FileNotFoundError
    """
    # check file exists
    path = os.path.join(os.path.join(os.path.dirname(__file__), "datasets", f"{dataset_name}.sql"))
    if not os.path.exists(path):
        raise FileNotFoundError(path)

    local_staging_basepath = Path(f"~/.spark/data/staging/datasets").expanduser()
    with tempfile.TemporaryDirectory(dir=local_staging_basepath) as local_staging_path:

        # parse sql
        hive_staging_path = f"file:///opt/spark/data/derby/staging/datasets/{os.path.basename(local_staging_path)}/{dataset_name}"
        with open(path) as file_obj:
            sql = file_obj.read()
            sql = sql.format(staging_path=hive_staging_path)

        first_line = sql.split("\n")[0]
        matches = re.findall(r"url:[\s]+([^\s]+)", first_line)
        if matches:
            with tempfile.TemporaryDirectory() as tempdir:
                # download tar file
                local_path = os.path.join(tempdir, "data.tar.gz")
                print(f"Downloading data from: {matches[0]} -> {local_path}")
                request.urlretrieve(matches[0], local_path)

                # extracting files to staging location
                print(f"Extracting files to staging location: {local_staging_path}")
                with tarfile.open(local_path) as file_obj:
                    file_obj.extractall(local_staging_path)

        # execute sql
        conn = HiveConnection()
        for statement in sql.split(";"):
            cursor = conn.cursor()
            statement = statement.strip()
            if not statement:
                continue
            print(statement)
            cursor.execute(statement)
            print(cursor.fetchall())


def main() -> None:

    parser = argparse.ArgumentParser(
        description="Populate sample datasets to local Spark datastore"
    )

    # add argument
    parser.add_argument(
        "-l", "--list", action="store_true", dest="list", help="List datasets available"
    )
    parser.add_argument(
        "-a",
        "--add",
        dest="dataset_name",
        default=None,
        type=str,
        help="Populate dataset to local Spark datastore",
    )

    # parse the arguments from standard input
    args = parser.parse_args()
    if args.list:
        list_datasets()
        return
    elif args.dataset_name:
        upload_dataset(args.dataset_name)
        return
    parser.print_help()


if __name__ == "__main__":
    # calling the main function
    main()
