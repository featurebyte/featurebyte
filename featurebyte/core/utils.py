"""
Utility functions for API Objects
"""
from typing import Any

import asyncio
import os
import tempfile
import threading
from zipfile import ZipFile

import pandas as pd


class RunThread(threading.Thread):
    """
    Thread to run async function in a different thread
    """

    def __init__(self, func: Any, args: Any, kwargs: Any) -> None:
        self.func = func
        self.args = args
        self.kwargs = kwargs
        self.result = None
        super().__init__()

    def run(self) -> None:
        """
        Run async function
        """
        self.result = asyncio.run(self.func(*self.args, **self.kwargs))


def run_async(func: Any, *args: Any, **kwargs: Any) -> Any:
    """
    Run async function in both async and non-async context

    Parameters
    ----------
    func: Any
        Function to run
    args: Any
        Positional arguments
    kwargs: Any
        Keyword arguments

    Returns
    -------
    Any
        result from function call
    """
    try:
        loop = asyncio.get_running_loop()
    except RuntimeError:
        loop = None
    if loop and loop.is_running():
        thread = RunThread(func, args, kwargs)
        thread.start()
        thread.join()
        return thread.result
    return asyncio.run(func(*args, **kwargs))


def pandas_df_from_parquet_archive_data(zip_data: Any) -> pd.DataFrame:
    """
    Read data from zipped parquet archive byte stream to pandas dataframe

    Parameters
    ----------
    zip_data: Any
        ZipFile input (path or buffer-like)

    Returns
    -------
    pd.DataFrame
        Pandas Dataframe object
    """
    with tempfile.TemporaryDirectory() as tmpdir:
        output_path = os.path.join(tmpdir, "data.parquet")
        with ZipFile(zip_data, "r") as zipfile:
            zipfile.extractall(path=output_path)
        return (
            pd.read_parquet(output_path)
            .sort_values("__index__")
            .drop("__index__", axis=1)
            .reset_index(drop=True)
        )
