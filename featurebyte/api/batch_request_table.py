"""
BatchRequestTable class
"""
# pylint: disable=duplicate-code
from __future__ import annotations

from typing import Optional, Union

from pathlib import Path

import pandas as pd

from featurebyte.api.api_object import ApiObject
from featurebyte.api.api_object_util import ForeignKeyMapping
from featurebyte.api.feature_store import FeatureStore
from featurebyte.api.materialized_table import MaterializedTableMixin
from featurebyte.common.doc_util import FBAutoDoc
from featurebyte.models.batch_request_table import BatchRequestTableModel
from featurebyte.schema.batch_request_table import BatchRequestTableListRecord


class BatchRequestTable(BatchRequestTableModel, ApiObject, MaterializedTableMixin):
    """
    BatchRequestTable class
    """

    __fbautodoc__ = FBAutoDoc(proxy_class="featurebyte.BatchRequestTable")

    _route = "/batch_request_table"
    _list_schema = BatchRequestTableListRecord
    _get_schema = BatchRequestTableModel
    _list_fields = [
        "name",
        "type",
        "shape",
        "feature_store_name",
        "created_at",
    ]
    _list_foreign_keys = [
        ForeignKeyMapping("feature_store_id", FeatureStore, "feature_store_name"),
    ]

    def to_pandas(self) -> pd.DataFrame:
        """
        Converts the batch request table to a pandas dataframe.

        Returns
        -------
        pd.DataFrame

        Examples
        --------
        >>> batch_request_table = catalog.get_batch_request_table("my_batch_request_table")  # doctest: +SKIP
        >>> batch_request_table.to_pandas()  # doctest: +SKIP
        """
        return super().to_pandas()

    def preview(self, limit: int = 10) -> pd.DataFrame:
        """
        Returns a DataFrame that contains a selection of rows of the batch request table.

        Parameters
        ----------
        limit: int
            Maximum number of return rows.

        Returns
        -------
        pd.DataFrame
            Preview rows of the table.

        Examples
        --------
        >>> batch_request_table = catalog.get_batch_request_table("my_batch_request_table")  # doctest: +SKIP
        >>> batch_request_table.preview()  # doctest: +SKIP
        """
        return super().preview(limit=limit)

    def sample(self, size: int = 10, seed: int = 1234) -> pd.DataFrame:
        """
        Returns a DataFrame that contains a random selection of rows of the batch request table based on a
        specified size and seed for sampling control.

        Parameters
        ----------
        size: int
            Maximum number of rows to sample, with an upper bound of 10,000 rows.
        seed: int
            Seed to use for random sampling.

        Returns
        -------
        pd.DataFrame
            Sampled rows from the table.

        Examples
        --------
        >>> batch_request_table = catalog.get_batch_request_table("my_batch_request_table")  # doctest: +SKIP
        >>> batch_request_table.sample()  # doctest: +SKIP
        """
        return super().sample(size=size, seed=seed)

    def describe(self, size: int = 0, seed: int = 1234) -> pd.DataFrame:
        """
        Returns descriptive statistics of the batch request table.

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

        Examples
        --------
        >>> batch_request_table = catalog.get_batch_request_table("my_batch_request_table")  # doctest: +SKIP
        >>> batch_request_table.describe()  # doctest: +SKIP
        """
        return super().describe(size=size, seed=seed)

    def download(self, output_path: Optional[Union[str, Path]] = None) -> Path:
        """
        Downloads the batch request table from the database.

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

        Examples
        --------
        >>> batch_request_table = catalog.get_batch_request_table("batch_request_table_name")  # doctest: +SKIP
        >>> downloaded_path = batch_request_table.download(output_path="path/to/download")  # doctest: +SKIP

        # noqa: DAR402
        """
        return super().download(output_path=output_path)

    def delete(self) -> None:
        """
        Deletes the batch request table.

        Raises
        ------
        RecordDeletionException
            When the record cannot be deleted properly

        Examples
        --------
        >>> batch_request_table = catalog.get_batch_request_table("batch_request_table_name")  # doctest: +SKIP
        >>> batch_request_table.delete()  # doctest: +SKIP

        # noqa: DAR402
        """
        super().delete()
