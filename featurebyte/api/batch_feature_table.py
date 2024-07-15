"""
BatchFeatureTable class
"""

from __future__ import annotations

from typing import Any, ClassVar, List, Optional, Union

from pathlib import Path

import pandas as pd

from featurebyte.api.api_object import ApiObject
from featurebyte.api.api_object_util import ForeignKeyMapping
from featurebyte.api.batch_request_table import BatchRequestTable
from featurebyte.api.feature_store import FeatureStore
from featurebyte.api.materialized_table import MaterializedTableMixin
from featurebyte.common.doc_util import FBAutoDoc
from featurebyte.models.batch_feature_table import BatchFeatureTableModel
from featurebyte.schema.batch_feature_table import BatchFeatureTableListRecord


class BatchFeatureTable(BatchFeatureTableModel, ApiObject, MaterializedTableMixin):
    """
    BatchFeatureTable class
    """

    # class variables
    __fbautodoc__: ClassVar[FBAutoDoc] = FBAutoDoc(proxy_class="featurebyte.BatchFeatureTable")
    _route: ClassVar[str] = "/batch_feature_table"
    _list_schema: ClassVar[Any] = BatchFeatureTableListRecord
    _get_schema: ClassVar[Any] = BatchFeatureTableModel
    _list_fields: ClassVar[List[str]] = [
        "name",
        "feature_store_name",
        "batch_request_table_name",
        "shape",
        "created_at",
    ]
    _list_foreign_keys: ClassVar[List[ForeignKeyMapping]] = [
        ForeignKeyMapping("feature_store_id", FeatureStore, "feature_store_name"),
        ForeignKeyMapping("batch_request_table_id", BatchRequestTable, "batch_request_table_name"),
    ]

    def preview(self, limit: int = 10) -> pd.DataFrame:
        """
        Returns a DataFrame that contains a selection of rows of the batch feature table.

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
        >>> batch_feature_table = catalog.get_batch_feature_table("batch_feature_table_name")  # doctest: +SKIP
        >>> preview_df = batch_feature_table.preview()  # doctest: +SKIP
        """
        return super().preview(limit=limit)

    def sample(self, size: int = 10, seed: int = 1234) -> pd.DataFrame:
        """
        Returns a DataFrame that contains a random selection of rows of the batch feature table based on a
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
        >>> batch_feature_table = catalog.get_batch_feature_table("batch_feature_table_name")  # doctest: +SKIP
        >>> sample_df = batch_feature_table.sample()  # doctest: +SKIP
        """
        return super().sample(size=size, seed=seed)

    def describe(self, size: int = 0, seed: int = 1234) -> pd.DataFrame:
        """
        Returns descriptive statistics of the batch feature table.

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
        >>> batch_feature_table = catalog.get_batch_feature_table("batch_feature_table_name")  # doctest: +SKIP
        >>> summary_df = batch_feature_table.describe()  # doctest: +SKIP
        """
        return super().describe(size=size, seed=seed)

    def download(self, output_path: Optional[Union[str, Path]] = None) -> Path:
        """
        Downloads the batch feature table from the database.

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
        >>> batch_feature_table = catalog.get_batch_feature_table("batch_feature_table_name")  # doctest: +SKIP
        >>> downloaded_path = batch_feature_table.download(output_path="path/to/download")  # doctest: +SKIP

        # noqa: DAR402
        """
        return super().download(output_path=output_path)

    def delete(self) -> None:
        """
        Deletes the batch feature table.

        Raises
        ------
        RecordDeletionException
            When the record cannot be deleted properly

        Examples
        --------
        >>> batch_feature_table = catalog.get_batch_feature_table("batch_feature_table_name")  # doctest: +SKIP
        >>> batch_feature_table.delete()  # doctest: +SKIP

        # noqa: DAR402
        """
        super().delete()
