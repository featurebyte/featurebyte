"""
Utility functions for API Objects
"""
from __future__ import annotations

from typing import Any, Iterator, List, Optional, Union

import functools
from datetime import datetime
from decimal import Decimal
from importlib import metadata as importlib_metadata
from io import BytesIO
from pathlib import Path

import numpy as np
import pandas as pd
import pyarrow as pa
import pyarrow.parquet as pq
import pygments
from alive_progress import alive_bar
from dateutil import parser
from pygments.formatters.html import HtmlFormatter
from requests import Response

from featurebyte.common.env_util import get_alive_bar_additional_params
from featurebyte.enum import DBVarType, InternalName


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
    dataframe = dataframe.replace({np.inf: "inf", -np.inf: "-inf"})

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

    dataframe = pd.read_json(values["data"], orient="table", convert_dates=False)
    type_conversions: Optional[dict[Optional[str], DBVarType]] = values.get("type_conversions")
    if type_conversions:
        for col_name, dtype in type_conversions.items():
            # is col_name is None in type_conversions it should apply to the only column in the dataframe
            if not col_name:
                col_name = dataframe.columns[0]  # pylint: disable=no-member

            if dtype == DBVarType.TIMESTAMP_TZ:
                dataframe[col_name] = dataframe[col_name].apply(_to_datetime)
            else:
                raise NotImplementedError()
    return dataframe


def parquet_from_arrow_stream(response: Response, output_path: Path, num_rows: int) -> None:
    """
    Write parquet file from arrow byte stream

    Parameters
    ----------
    response: Response
        Streamed http response
    output_path: Path
        Output path
    num_rows: int
        Number of rows to write
    """
    reader = pa.ipc.open_stream(ResponseStream(response.iter_content(1024)))
    batch = reader.read_next_batch()
    with pq.ParquetWriter(output_path, batch.schema) as writer:
        try:
            with alive_bar(
                total=num_rows,
                title="Downloading table",
                **get_alive_bar_additional_params(),
            ) as progress_bar:
                while True:
                    table = pa.Table.from_batches([batch])
                    if table.num_rows == 0:
                        break
                    writer.write_table(table)
                    progress_bar(table.num_rows)  # pylint: disable=not-callable
                    batch = reader.read_next_batch()
        except StopIteration:
            pass


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
    def wrapper(self: Any, observation_set: pd.DataFrame, **kwargs: Any) -> pd.DataFrame:
        observation_set = observation_set.copy()
        observation_set[InternalName.ROW_INDEX] = range(observation_set.shape[0])
        result_dataframe = (
            function(self, observation_set, **kwargs)
            .sort_values(InternalName.ROW_INDEX)
            .drop(InternalName.ROW_INDEX, axis=1)
        )
        result_dataframe.index = observation_set.index
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


class CodeStr(str):
    """
    Code string content that can be displayed in markdown format
    """

    def _repr_html_(self) -> str:
        lexer = pygments.lexers.get_lexer_by_name("python")
        highlighted_code = pygments.highlight(
            str(self).strip(),
            lexer=lexer,
            formatter=HtmlFormatter(noclasses=True, nobackground=True),
        )
        return (
            '<div style="margin:30px; padding: 20px; border:1px solid #aaa">'
            f"{highlighted_code}</div>"
        )


class InfoDict(dict):
    """
    Featurebyte asset information dictionary that can be displayed in HTML
    """

    def _repr_html_(self) -> str:
        def dict_to_html(data):
            """
            Convert dict to html using basic html tags
            """
            content = []
            if "class_name" in data:
                # info table title
                content.append(
                    '<div style="font-weight:bold; text-align:center; border-bottom:1px solid black; width: 100%; padding-top:20px">'
                    f'{data["class_name"]}</div>'
                )

            if isinstance(data, list):
                return pd.DataFrame(data)._repr_html_()
                # # list of dictionaries
                # data = pd.DataFrame(data).to_dict(orient="list")
                # header = "</th><th>".join([
                #     " ".join(key.split("_")).title() for key in data.keys()
                # ])
                # content.append(
                #     f'<thead><tr style="height:35px; border-bottom: 1px solid lightgray"><th>{header}</th></tr></thead><tbody>'
                # )
                # for row in zip(*data.values()):
                #     row_content = "</td><td>".join([str(value) for value in row])
                #     content.append(f"<tr><td>{row_content}</td></tr>")
                # content.append("</tbody>")
            else:
                for key, value in data.items():
                    if isinstance(value, dict):
                        # process dictionaries recursively
                        value = (
                            f'<div style="border:0;padding:0;margin:0">{dict_to_html(value)}</div>'
                        )
                    elif isinstance(value, list):
                        if value and isinstance(value[0], dict):
                            # list of dictionaries
                            value = f'<div style="border:0;padding:0;margin:0">{dict_to_html(value)}</div>'
                    else:
                        # nicer datetime formatting
                        try:
                            value = datetime.fromisoformat(value).strftime("%Y-%m-%d %H:%M:%S")
                        except:
                            pass

                    key = " ".join(key.split("_")).title()
                    content.append(
                        f'<tr><td style="font-weight:bold; vertical-align:top; width:200px; over-flow:wrap">{key}</td>'
                        f'<td style="width:80%; text-align: left">{value}</td></tr>'
                    )
            return f'<table style="width:100%;padding:0;margin:0">{"".join(content)}</table>'

        return dict_to_html(self)
