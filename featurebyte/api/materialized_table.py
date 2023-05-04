"""
Materialized Table Mixin
"""
from typing import Any, Callable, ClassVar, Optional, Tuple, Union

import os
import tempfile
from http import HTTPStatus
from pathlib import Path

import pandas as pd
from typeguard import typechecked

from featurebyte.api.feature_store import FeatureStore
from featurebyte.api.source_table import SourceTable
from featurebyte.common.utils import parquet_from_arrow_stream
from featurebyte.config import Configurations
from featurebyte.exception import RecordDeletionException, RecordRetrievalException
from featurebyte.models.materialized_table import MaterializedTableModel


class MaterializedTableMixin(MaterializedTableModel):
    """
    Mixin for Materialized Table
    """

    _route: ClassVar[str] = ""
    _poll_async_task: Callable[..., Any]

    def download(self, output_path: Optional[Union[str, Path]] = None) -> Path:
        """
        Downloads the table from the database.

        Parameters
        ----------
        output_path: Optional[Union[str, Path]]
            Location to save downloaded parquet file.

        Returns
        -------
        Path

        Raises
        ------
        FileExistsError
            File already exists at output path.
        RecordRetrievalException
            Error retrieving record from API.
        """
        file_name = f"{self.location.table_details.table_name}.parquet"
        output_path = output_path or Path(f"./{file_name}")
        output_path = Path(output_path)
        if output_path.exists():
            raise FileExistsError(f"{output_path} already exists.")

        client = Configurations().get_client()
        response = client.get(f"{self._route}/pyarrow_table/{self.id}", stream=True)
        if response.status_code != HTTPStatus.OK:
            raise RecordRetrievalException(response)
        parquet_from_arrow_stream(
            response=response, output_path=output_path, num_rows=self.num_rows
        )
        return output_path

    def to_pandas(self) -> pd.DataFrame:
        """
        Converts the table to pandas dataframe.

        Returns
        -------
        pd.DataFrame
        """
        with tempfile.TemporaryDirectory() as temp_dir:
            output_path = os.path.join(temp_dir, "temp.parquet")
            self.download(output_path=output_path)
            return pd.read_parquet(output_path)

    def delete(self) -> None:
        """
        Deletes the materialized table.

        Raises
        ------
        RecordDeletionException
            When the record cannot be deleted properly
        """
        client = Configurations().get_client()
        response = client.delete(f"{self._route}/{self.id}")
        if response.status_code != HTTPStatus.ACCEPTED:
            raise RecordDeletionException(response)
        self._poll_async_task(task_response=response, retrieve_result=False, has_output_url=False)

    @typechecked
    def preview(self, limit: int = 10) -> pd.DataFrame:
        """
        Returns a DataFrame that contains a selection of rows of the table.

        Parameters
        ----------
        limit: int
            Maximum number of return rows.

        Returns
        -------
        pd.DataFrame
            Preview rows of the table.
        """
        return self._source_table.preview(limit=limit)

    @typechecked
    def sample(self, size: int = 10, seed: int = 1234) -> pd.DataFrame:
        """
        Returns a DataFrame that contains a random selection of rows of the table based on a
        specified size and seed for sampling control.

        Parameters
        ----------
        size: int
            Maximum number of rows to sample.
        seed: int
            Seed to use for random sampling.

        Returns
        -------
        pd.DataFrame
            Sampled rows from the table.
        """
        return self._source_table.sample(size=size, seed=seed)

    @typechecked
    def describe(self, size: int = 0, seed: int = 1234) -> pd.DataFrame:
        """
        Returns descriptive statistics of the table columns.

        Parameters
        ----------
        size: int
            Maximum number of rows to sample. If 0, all rows will be used.
        seed: int
            Seed to use for random sampling.

        Returns
        -------
        pd.DataFrame
            Summary of the table.
        """
        return self._source_table.describe(size=size, seed=seed)

    def shape(self) -> Tuple[int, int]:
        """
        Returns the shape of the table.

        Returns
        -------
        Tuple[int, int]
        """
        return self.num_rows, len(self.columns_info)

    @property
    def _source_table(self) -> SourceTable:
        data_source = FeatureStore.get_by_id(self.location.feature_store_id).get_data_source()
        source_table = data_source.get_source_table(**self.location.table_details.json_dict())
        return source_table
