"""
Utility functions for API Objects
"""
from __future__ import annotations

from typing import Any, Dict, Iterator, List, Optional, Union

import copy
import functools
import re
from datetime import datetime
from decimal import Decimal
from importlib import metadata as importlib_metadata
from io import BytesIO
from pathlib import Path
from xml.dom.minidom import Document, Element, getDOMImplementation

import numpy as np
import pandas as pd
import pyarrow as pa
import pyarrow.parquet as pq
import pygments
from alive_progress import alive_bar
from bson import ObjectId
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


class InfoDict(Dict[str, Any]):
    """
    Featurebyte asset information dictionary that can be displayed in HTML
    """

    def _repr_html_(self) -> str:
        def _set_element_style(elem: Element, style: Dict[str, Any]) -> None:
            """
            Set style of dom element

            Parameters
            ----------
            elem: Element
                Element to set style for.
            style: Dict[str, Any]
                Style dictionary
            """
            elem.setAttribute("style", ";".join(f"{key}:{value}" for key, value in style.items()))

        def _populate_html_elem(
            data: Dict[str, Any], doc: Document, elem: Element, html_content: Dict[str, str]
        ) -> None:
            """
            Populate html document with data dict

            Parameters
            ----------
            data: Dict[str, str]
                Data dictionary
            doc: Document
                HTML document
            elem: Element
                HTML element to populate into
            html_content: Dict[str, str]
                Dictionary referencing HTML content in the document
            """
            # create html table
            table = doc.createElement("table")
            _set_element_style(table, {"width": "100%", "padding": 0, "margin": 0})

            for key, value in data.items():
                # add row to table
                row = doc.createElement("tr")
                table.appendChild(row)

                # populate key column
                key_column = doc.createElement("td")
                _set_element_style(
                    key_column,
                    {
                        "font-weight": "bold",
                        "vertical-align": "top",
                        "width": "200px",
                        "over-flow": "overflow-wrap",
                        "word-break": "break-all",
                    },
                )
                key_column.appendChild(doc.createTextNode(key))
                row.appendChild(key_column)

                # populate value column
                if isinstance(value, dict):
                    # process dictionaries recursively
                    value_elem = doc.createElement("div")
                    _set_element_style(value_elem, {"border": "0", "padding": "0", "margin": "0"})
                    _populate_html_elem(
                        data=value, doc=doc, elem=value_elem, html_content=html_content
                    )
                elif isinstance(value, list) and isinstance(value[0], dict):
                    # list of dictionaries
                    value_elem = doc.createElement("div")
                    _set_element_style(value_elem, {"border": "0", "padding": "0", "margin": "0"})
                    df_key = str(ObjectId())
                    html_content[df_key] = pd.DataFrame(value).to_html()
                    value_elem.appendChild(doc.createTextNode(f"{{{df_key}}}"))
                else:
                    # nicer datetime formatting
                    try:
                        value = datetime.fromisoformat(value).strftime("%Y-%m-%d %H:%M:%S")
                    except (ValueError, TypeError):
                        pass
                    value_elem = doc.createTextNode(str(value))

                val_column = doc.createElement("td")
                _set_element_style(val_column, {"width": "80%", "text-align": "left"})
                val_column.appendChild(value_elem)
                row.appendChild(val_column)

            elem.appendChild(table)

        # create html document
        impl = getDOMImplementation()
        assert impl
        doc_type = impl.createDocumentType(
            "html",
            "-//W3C//DTD XHTML 1.0 Strict//EN",
            "http://www.w3.org/TR/xhtml1/DTD/xhtml1-strict.dtd",
        )
        doc = impl.createDocument("http://www.w3.org/1999/xhtml", "html", doc_type)
        doc_elem = doc.documentElement

        # add title for the table
        data = copy.deepcopy(self)
        class_name = data.pop("class_name", "Unknown")
        class_name = re.sub(r"([A-Z]+[a-z]+)", r" \1", class_name).strip()
        title_div = doc.createElement("div")
        title_div.appendChild(doc.createTextNode(class_name))
        _set_element_style(
            elem=title_div,
            style={
                "font-weight": "bold",
                "text-align": "center",
                "border-bottom": "1px solid black",
                "width": "100%",
                "padding-top": "20px",
            },
        )
        doc_elem.appendChild(title_div)

        # add info table
        html_content: Dict[str, str] = {}
        _populate_html_elem(data=data, doc=doc, elem=doc_elem, html_content=html_content)
        html = str(doc.toprettyxml())
        return html.format(**html_content)
