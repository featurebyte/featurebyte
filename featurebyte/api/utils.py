"""
Utility functions for API Objects
"""

from __future__ import annotations

from pathlib import Path
from typing import Any

import pandas as pd
import pyarrow as pa
import pyarrow.parquet as pq
from alive_progress import alive_bar

from featurebyte.common.env_util import get_alive_bar_additional_params
from featurebyte.common.utils import dataframe_from_arrow_table


def dataframe_from_arrow_stream_with_progress(buffer: Any, num_rows: int) -> pd.DataFrame:
    """
    Read data from arrow byte stream to pandas dataframe

    Parameters
    ----------
    buffer: Any
        buffer-like
    num_rows: int
        Number of rows to read

    Returns
    -------
    pd.DataFrame
        Pandas Dataframe object
    """
    reader = pa.ipc.open_stream(buffer)
    if num_rows == 0:
        arrow_table = reader.read_all()
    else:
        batches = []
        batches.append(reader.read_next_batch())
        try:
            with alive_bar(
                total=num_rows,
                title="Downloading table",
                **get_alive_bar_additional_params(),
            ) as progress_bar:
                while True:
                    if batches[-1].num_rows > 0:
                        progress_bar(batches[-1].num_rows)
                    batches.append(reader.read_next_batch())
        except StopIteration:
            pass
        arrow_table = pa.Table.from_batches(batches)

    return dataframe_from_arrow_table(arrow_table)


def parquet_from_arrow_stream(buffer: Any, output_path: Path, num_rows: int) -> None:
    """
    Write parquet file from arrow byte stream

    Parameters
    ----------
    buffer: Any
        buffer-like
    output_path: Path
        Output path
    num_rows: int
        Number of rows to write
    """
    reader = pa.ipc.open_stream(buffer)
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
                    writer.write_table(table)
                    if table.num_rows > 0:
                        progress_bar(table.num_rows)
                    batch = reader.read_next_batch()
        except StopIteration:
            pass
