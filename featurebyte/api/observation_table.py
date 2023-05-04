"""
ObservationTable class
"""
from __future__ import annotations

import pandas as pd

from featurebyte.api.api_object import ApiObject, ForeignKeyMapping
from featurebyte.api.feature_store import FeatureStore
from featurebyte.api.materialized_table import MaterializedTableMixin
from featurebyte.common.doc_util import FBAutoDoc
from featurebyte.models.observation_table import ObservationTableModel
from featurebyte.schema.observation_table import ObservationTableListRecord


class ObservationTable(ObservationTableModel, ApiObject, MaterializedTableMixin):
    """
    ObservationTable class
    """

    __fbautodoc__ = FBAutoDoc(proxy_class="featurebyte.ObservationTable")

    _route = "/observation_table"
    _list_schema = ObservationTableListRecord
    _get_schema = ObservationTableModel
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
        Converts the observation table to a pandas dataframe.

        Returns
        -------
        pd.DataFrame

        Examples
        --------
        >>> observation_table = catalog.get_observation_table("observation_table_name")  # doctest: +SKIP
        >>> observation_table.to_pandas()  # doctest: +SKIP
        """
        return super().to_pandas()
