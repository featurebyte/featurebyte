"""
HistoricalFeatureTableModel
"""
from __future__ import annotations

from featurebyte.models.base import PydanticObjectId
from featurebyte.models.materialized_table import MaterializedTableModel


class HistoricalFeatureTableModel(MaterializedTableModel):
    """
    HistoricalFeatureTable is the result of asynchronous historical features requests
    """

    observation_table_id: PydanticObjectId
    feature_list_id: PydanticObjectId

    class Settings(MaterializedTableModel.Settings):
        """
        MongoDB settings
        """

        collection_name: str = "historical_feature_table"
