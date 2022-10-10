"""
Utility functions for API Objects
"""
from __future__ import annotations

from typing import Any

import asyncio
import threading
from io import BytesIO

import pandas as pd
import pyarrow as pa

COMPRESSION_TYPE = "bz2"


class RunThread(threading.Thread):
    """
    Run async function in a different thread
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


def dataframe_to_arrow_bytes(dataframe: pd.DataFrame) -> bytes:
    """
    Convert pandas DataFrame to compressed bytes in arrow format

    Parameters
    ----------
    dataframe: pd.DataFrame
        Dataframe to use

    Returns
    -------
    bytes
    """
    table = pa.Table.from_pandas(dataframe)
    sink = pa.BufferOutputStream()
    with pa.CompressedOutputStream(sink, COMPRESSION_TYPE) as compressed_buffer:
        pa.ipc.new_stream(compressed_buffer, table.schema).write_table(table)
    data = sink.getvalue().to_pybytes()
    assert isinstance(data, bytes)
    return data


def dataframe_from_arrow_stream(buffer: Any) -> pd.DataFrame | None:
    """
    Read data from arrow byte stream to pandas dataframe

    Parameters
    ----------
    buffer: Any
        buffer-like

    Returns
    -------
    pd.DataFrame | None
        Pandas Dataframe object
    """
    if isinstance(buffer, bytes):
        input_buffer = BytesIO(buffer)
    else:
        input_buffer = buffer

    with pa.CompressedInputStream(input_buffer, COMPRESSION_TYPE) as decompressed_buffer:
        reader = pa.ipc.open_stream(decompressed_buffer)
        return reader.read_all().to_pandas()


def pa_table_to_record_batches(table: pa.Table) -> Any:
    """
    Convert pyarrow table to list of RecordBatch object, with special handling
    include schema in output for empty table

    Parameters
    ----------
    table: pa.Table
        PyArrow Table object

    Returns
    -------
    Any
        List of RecordBatch objects
    """
    if table.shape[0]:
        return table.to_batches()

    # convert to pandas in order to create empty record batch with schema
    # there is no way to get empty record batch from pyarrow table directly
    return [pa.RecordBatch.from_pandas(table.to_pandas())]
