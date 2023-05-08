"""
HistoricalFeatureTable class
"""
from __future__ import annotations

import pandas as pd

from featurebyte.api.api_object import ApiObject, ForeignKeyMapping
from featurebyte.api.feature_store import FeatureStore
from featurebyte.api.materialized_table import MaterializedTableMixin
from featurebyte.api.observation_table import ObservationTable
from featurebyte.common.doc_util import FBAutoDoc
from featurebyte.models.historical_feature_table import HistoricalFeatureTableModel
from featurebyte.schema.historical_feature_table import HistoricalFeatureTableListRecord


class HistoricalFeatureTable(HistoricalFeatureTableModel, ApiObject, MaterializedTableMixin):
    """
    HistoricalFeatureTable class
    """

    __fbautodoc__ = FBAutoDoc(proxy_class="featurebyte.HistoricalFeatureTable")

    _route = "/historical_feature_table"
    _list_schema = HistoricalFeatureTableListRecord
    _get_schema = HistoricalFeatureTableModel
    _list_fields = [
        "name",
        "feature_store_name",
        "observation_table_name",
        "shape",
        "created_at",
    ]
    _list_foreign_keys = [
        ForeignKeyMapping("feature_store_id", FeatureStore, "feature_store_name"),
        ForeignKeyMapping("observation_table_id", ObservationTable, "observation_table_name"),
    ]

    def preview(self, limit: int = 10) -> pd.DataFrame:
        """
        Returns a DataFrame that contains a selection of rows of the historical feature table.

        Parameters
        ----------
        limit: int
            Maximum number of return rows.

        Returns
        -------
        pd.DataFrame
            Preview rows of the table.
        """
        return super().preview(limit=limit)

    def sample(self, size: int = 10, seed: int = 1234) -> pd.DataFrame:
        """
        Returns a DataFrame that contains a random selection of rows of the historical feature table based on a
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
        return super().sample(size=size, seed=seed)

    def describe(self, size: int = 0, seed: int = 1234) -> pd.DataFrame:
        """
        Returns descriptive statistics of the historical feature table.

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
        return super().describe(size=size, seed=seed)
