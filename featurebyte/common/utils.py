"""
Common utility functions
"""

from __future__ import annotations

import ast
import functools
import json
import logging
import os
import time
from contextlib import contextmanager
from datetime import datetime
from decimal import Decimal
from importlib import metadata as importlib_metadata
from io import StringIO
from json import JSONDecodeError
from typing import Any, Generator, Iterator, List, Optional, Union

import numpy as np
import pandas as pd
import pyarrow as pa
from dateutil import parser

from featurebyte.enum import DBVarType, InternalName

ARROW_METADATA_DB_VAR_TYPE = b"db_var_type"
PYARROW_JSON_ENCODED_TYPES = DBVarType.dictionary_types().union(DBVarType.array_types())


class ResponseStream:
    """
    Simulate a buffer-like object from a streamed response content iterator
    """

    def __init__(self, request_iterator: Iterator[bytes]) -> None:
        self._bytes = b""
        self._iterator = request_iterator
        self.closed = False

    def read(self, size: int = 1024) -> bytes:
        """
        Read data from iterator.

        Parameters
        ----------
        size: int
            Number of bytes to read

        Returns
        -------
        bytes
        """
        try:
            while len(self._bytes) < size:
                self._bytes += next(self._iterator)
            data = self._bytes[:size]
            self._bytes = self._bytes[size:]
            return data
        except StopIteration:
            self.closed = True
            return self._bytes


def get_version() -> str:
    """
    Retrieve module version

    Returns
    --------
    str
        Module version
    """
    try:
        return str(importlib_metadata.version("featurebyte"))
    except importlib_metadata.PackageNotFoundError:  # pragma: no cover
        return "unknown"


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


def try_json_decode(value: Any) -> Any:
    """
    Try to decode a value as JSON, if it fails, return None

    Parameters
    ----------
    value: Any
        Value to decode

    Returns
    -------
    Any
        Decoded value or None if decoding fails
    """
    if isinstance(value, str):
        try:
            return json.loads(value, strict=False)
        except JSONDecodeError:
            return None
    return None


def dataframe_from_arrow_table(arrow_table: pa.Table) -> pd.DataFrame:
    """
    Convert arrow table to pandas dataframe, handling list and map types

    Parameters
    ----------
    arrow_table: pa.Table
        Arrow table object

    Returns
    -------
    pd.DataFrame
        Pandas Dataframe object
    """
    # handle conversion of list and map types
    dataframe = arrow_table.to_pandas()
    for field in arrow_table.schema:
        if field.metadata and ARROW_METADATA_DB_VAR_TYPE in field.metadata:
            db_var_type = field.metadata[ARROW_METADATA_DB_VAR_TYPE].decode()
            if db_var_type in PYARROW_JSON_ENCODED_TYPES:
                dataframe[field.name] = dataframe[field.name].apply(try_json_decode)
    return dataframe


def dataframe_from_arrow_stream(buffer: Any) -> pd.DataFrame:
    """
    Read data from arrow byte stream to pandas dataframe

    Parameters
    ----------
    buffer: Any
        buffer-like

    Returns
    -------
    pd.DataFrame
        Pandas Dataframe object
    """
    reader = pa.ipc.open_stream(buffer)
    arrow_table = reader.read_all()
    return dataframe_from_arrow_table(arrow_table)


def literal_eval(value: Any) -> Any:
    """
    Runs ast.literal_eval on value, if it fails, return value

    Parameters
    ----------
    value: Any
        Value to evaluate

    Returns
    -------
    Any
    """
    if value is None:
        return value
    return ast.literal_eval(value)


def prepare_dataframe_for_json(dataframe: pd.DataFrame) -> None:
    """
    Process pandas dataframe in-place before converting to json

    Parameters
    ----------
    dataframe: pd.DataFrame
        Dataframe object
    """
    dataframe.reset_index(drop=True, inplace=True)
    if dataframe.shape[0] == 0:
        return
    for name in dataframe.columns:
        # Decimal with integer values becomes float during conversion to json
        if (
            dataframe[name].dtype == object
            and isinstance(dataframe[name].iloc[0], Decimal)
            and (dataframe[name] % 1 == 0).all()
        ):
            dataframe[name] = dataframe[name].astype(int)


def dataframe_to_json(
    dataframe: pd.DataFrame,
    type_conversions: Optional[dict[Optional[str], DBVarType]] = None,
    skip_prepare: bool = False,
) -> dict[str, Any]:
    """
    Write pandas dataframe to json

    Parameters
    ----------
    dataframe: pd.DataFrame
        Dataframe object
    type_conversions: Optional[dict[Optional[str], DBVarType]]
        Conversions to apply on columns
    skip_prepare: bool
        Whether to skip dataframe preparation

    Returns
    -------
    dict[str, Any]
        Dict containing JSON string and type conversions
    """
    if not skip_prepare:
        prepare_dataframe_for_json(dataframe)

    # convert infinity values to string as these gets converted to null
    numeric_cols = dataframe.select_dtypes(include=[np.number]).columns
    dataframe[numeric_cols] = dataframe[numeric_cols].replace({np.inf: "inf", -np.inf: "-inf"})

    return {
        "data": dataframe.to_json(orient="table", date_unit="ns", double_precision=15),
        "type_conversions": type_conversions,
    }


def dataframe_from_json(values: dict[str, Any]) -> pd.DataFrame:
    """
    Read pandas dataframe from json

    Parameters
    ----------
    values: dict[str, Any]
        Dict containing JSON string and type conversions

    Returns
    -------
    pd.DataFrame
        Dataframe object

    Raises
    ------
    NotImplementedError
        Unsupported type conversion
    """

    def _to_datetime(value: Any) -> Any:
        """
        Convert value with datetime if feasible

        Parameters
        ----------
        value: Any
            Value to convert

        Returns
        -------
        Any
        """
        if isinstance(value, str):
            try:
                return pd.to_datetime(value)
            except (TypeError, parser.ParserError):
                pass
        return value

    def _to_date(value: Any) -> Any:
        """
        Convert value with date if feasible

        Parameters
        ----------
        value: Any
            Value to convert

        Returns
        -------
        Any
        """
        if isinstance(value, str):
            try:
                return pd.to_datetime(value).date()
            except (TypeError, parser.ParserError):
                pass
        return value

    dataframe = pd.read_json(StringIO(values["data"]), orient="table", convert_dates=False)
    type_conversions: Optional[dict[Optional[str], DBVarType]] = values.get("type_conversions")
    if type_conversions:
        for col_name, dtype in type_conversions.items():
            # is col_name is None in type_conversions it should apply to the only column in the dataframe
            if not col_name:
                col_name = dataframe.columns[0]

            if dtype in [DBVarType.TIMESTAMP, DBVarType.TIMESTAMP_TZ]:
                dataframe[col_name] = dataframe[col_name].apply(_to_datetime)
            elif dtype == DBVarType.DATE:
                dataframe[col_name] = dataframe[col_name].apply(_to_date)
            else:
                raise NotImplementedError()
    return dataframe


def validate_datetime_input(value: Union[datetime, str]) -> str:
    """
    Validate datetime input value

    Parameters
    ---------
    value: Union[datetime, str]
        Input datetime value

    Returns
    -------
    str
        Validated UTC datetime value in ISO format
    """

    datetime_value: datetime
    if isinstance(value, datetime):
        datetime_value = value
    else:
        datetime_value = parser.parse(value)

    return datetime_value.isoformat()


def enforce_observation_set_row_order(function: Any) -> Any:
    """
    Decorator to enforce that the row order of the output is consistent with that of the observation set

    Parameters
    ----------
    function: Any
        Function to decorate

    Returns
    -------
    Any
        Decorated function
    """

    @functools.wraps(function)
    def wrapper(self: Any, observation_set: Any, **kwargs: Any) -> pd.DataFrame:
        if isinstance(observation_set, pd.DataFrame):
            observation_set = observation_set.copy()
            observation_set[InternalName.DATAFRAME_ROW_INDEX] = range(observation_set.shape[0])
            result_dataframe = (
                function(self, observation_set, **kwargs)
                .sort_values(InternalName.DATAFRAME_ROW_INDEX)
                .drop(InternalName.DATAFRAME_ROW_INDEX, axis=1)
            )
            result_dataframe.index = observation_set.index
        else:
            result_dataframe = function(self, observation_set, **kwargs)
        return result_dataframe

    return wrapper


def construct_repr_string(obj: object, additional_info: Optional[str] = None) -> str:
    """
    Construct repr string for an object

    Parameters
    ----------
    obj: object
        Object to construct repr string for
    additional_info: Optional[str]
        Additional info to include in the repr string

    Returns
    -------
    str
    """
    # construct representation string
    repr_str = f"<{obj.__class__.__module__}.{obj.__class__.__name__} at {hex(id(obj))}>"
    if additional_info:
        repr_str += "\n" + additional_info
    return repr_str


def convert_to_list_of_strings(value: Optional[Union[str, List[str]]]) -> List[str]:
    """
    Convert value to list of strings

    Parameters
    ----------
    value: Optional[Union[str, List[str]]]
        Value to convert

    Returns
    -------
    List[str]
        List of strings
    """
    output = []
    if isinstance(value, str):
        output = [value]
    if isinstance(value, list):
        output = value
    return output


def is_server_mode() -> bool:
    """
    Check if the code is running in server mode. Server mode is used when running the SDK code inside
    featurebyte worker.

    Returns
    -------
    bool
        True if the code is running in server mode
    """
    sdk_execution_mode = os.environ.get("FEATUREBYTE_SDK_EXECUTION_MODE")
    return sdk_execution_mode == "SERVER"


@contextmanager
def timer(
    message: str, logger: logging.Logger, **logger_kwargs: Any
) -> Generator[None, None, None]:
    """
    Timer context manager to measure execution time.

    Parameters
    ----------
    message: str
        Message to log before and after the execution time measurement.
    logger: logging.Logger
        Logger object to log the execution time message.
    logger_kwargs: Any
        Additional keyword arguments to pass to the logger.

    Yields
    ------
    Generator[None, None, None]
        A context manager that logs the execution time with the given message and logger.
    """
    start_time = time.time()
    try:
        yield
    finally:
        end_time = time.time()
        duration = end_time - start_time
        logger.info(f"{message}: {duration} seconds", **logger_kwargs)
