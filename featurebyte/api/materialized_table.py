"""
Materialized Table Mixin
"""
from typing import Optional, Protocol, Union

from http import HTTPStatus
from pathlib import Path

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

    def download(self: HasLocation, output_path: Optional[Union[str, Path]] = None):
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

        client = Configurations().get_client()
        response = client.post(
            f"/feature_store/download", json=self.location.json_dict(), stream=True
        )
        assert response.status_code == HTTPStatus.OK, response.json()
        parquet_from_arrow_stream(response=response, output_path=output_path)
        return output_path
