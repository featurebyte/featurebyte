import argparse
import os
import re
import shutil
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

    local_staging_path = f"~/.spark/data/staging/datasets/{dataset_name}"
    hive_staging_path = f"file:///data/staging/datasets/{dataset_name}"

    # parse sql
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
            file = tarfile.open(local_path)
            file.extractall(local_staging_path)
            file.close()

    # execute sql
    conn = HiveConnection()
    for statement in sql.split(";"):
        cursor = conn.cursor()
        statement = statement.strip()
        if not statement:
            continue
        print(statement)
        try:
            cursor.execute(statement)
            print(cursor.fetchall())
        except:
            continue

    # delete staging files
    print("Cleaning up staging files")
    shutil.rmtree(Path(local_staging_path).expanduser())


def main():

    parser = argparse.ArgumentParser(
        description="Populate sample datasets to local Spark datastore"
    )

    # add argument
    parser.add_argument(
        "-l", "--list", action="store_true", dest="list", help="Shows all the datasets available"
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
