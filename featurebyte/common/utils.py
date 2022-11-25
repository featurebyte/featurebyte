"""
Utility functions for API Objects
"""
from __future__ import annotations

from typing import Any

from decimal import Decimal
from io import BytesIO

import pandas as pd
import pyarrow as pa


def create_new_arrow_stream_writer(buffer: Any, schema: pa.Schema) -> pa.RecordBatchStreamWriter:
    """
    Create new arrow IPC stream writer

    Parameters
    ----------
    buffer: Any
        buffer-like
    schema: pa.Schema
        Schema to use

    Returns
    -------
    pd.RecordBatchStreamWriter
        PyArrow RecordBatchStreamWriter object
    """
    ipc_options = pa.ipc.IpcWriteOptions(compression=pa.Codec("ZSTD", compression_level=9))
    return pa.ipc.new_stream(buffer, schema, options=ipc_options)


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
    create_new_arrow_stream_writer(sink, table.schema).write_table(table)
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

    reader = pa.ipc.open_stream(input_buffer)
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


def prepare_dataframe_for_json(dataframe: pd.DataFrame) -> None:
    """
    Process pandas dataframe in-place before converting to json

    Parameters
    ----------
    dataframe: pd.DataFrame
        Dataframe object
    """
    dataframe.reset_index(drop=True, inplace=True)
    for name in dataframe.columns:
        # Decimal with integer values becomes float during conversion to json
        if (
            dataframe[name].dtype == object
            and isinstance(dataframe[name].iloc[0], Decimal)
            and (dataframe[name] % 1 == 0).all()
        ):
            dataframe[name] = dataframe[name].astype(int)


def convert_dataframe_as_json(dataframe: pd.DataFrame) -> str:
    """
    Convert pandas dataframe to json

    Parameters
    ----------
    dataframe: pd.DataFrame
        Dataframe object

    Returns
    -------
    str
        JSON string
    """
    prepare_dataframe_for_json(dataframe)
    return str(dataframe.to_json(orient="table", date_unit="ns", double_precision=15))
