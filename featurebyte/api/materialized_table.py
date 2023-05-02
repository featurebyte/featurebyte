"""
Materialized Table Mixin
"""
from typing import Any, Callable, ClassVar, Optional, Union

import os
import tempfile
from http import HTTPStatus
from pathlib import Path

import pandas as pd

from featurebyte.api.feature_store import FeatureStore
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

        # get table shape
        data_source = FeatureStore.get_by_id(self.location.feature_store_id).get_data_source()
        source_table = data_source.get_source_table(**self.location.table_details.json_dict())
        num_rows = source_table.shape()[0]

        client = Configurations().get_client()
        response = client.get(f"{self._route}/pyarrow_table/{self.id}", stream=True)
        if response.status_code != HTTPStatus.OK:
            raise RecordRetrievalException(response)
        parquet_from_arrow_stream(response=response, output_path=output_path, num_rows=num_rows)
        return output_path

    def to_pandas(self) -> pd.DataFrame:
        """
        Converts the table to pandas dataframe

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
