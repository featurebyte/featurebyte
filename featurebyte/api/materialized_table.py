"""
Materialized Table Mixin
"""
from typing import Optional, Protocol, Union

from http import HTTPStatus
from pathlib import Path

from featurebyte.api.feature_store import FeatureStore
from featurebyte.common.utils import parquet_from_arrow_stream
from featurebyte.config import Configurations
from featurebyte.query_graph.model.common_table import TabularSource


class HasLocation(Protocol):
    """
    Class with location attribute / property
    """

    location: TabularSource


class MaterializedTableMixin:
    """
    Mixin for Materialized Table
    """

    def download(self: HasLocation, output_path: Optional[Union[str, Path]] = None) -> Path:
        """
        Downloads the table from the database

        Parameters
        ----------
        output_path: Optional[Union[str, Path]]
            Location to save downloaded report

        Returns
        -------
        Path

        Raises
        ------
        FileExistsError
            File already exists at output path
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
        response = client.post(
            "/feature_store/download", json=self.location.json_dict(), stream=True
        )
        assert response.status_code == HTTPStatus.OK, response.json()
        parquet_from_arrow_stream(response=response, output_path=output_path, num_rows=num_rows)
        return output_path
