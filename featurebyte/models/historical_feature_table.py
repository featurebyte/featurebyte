"""
HistoricalFeatureTableModel
"""
from __future__ import annotations

import pymongo

from featurebyte.models.base import PydanticObjectId
from featurebyte.models.base_feature_or_target_table import BaseFeatureOrTargetTableModel
from featurebyte.models.materialized_table import MaterializedTableModel


class HistoricalFeatureTableModel(BaseFeatureOrTargetTableModel):
    """
    HistoricalFeatureTable is the result of asynchronous historical features requests
    """

    feature_list_id: PydanticObjectId

    class Settings(MaterializedTableModel.Settings):
        """
        MongoDB settings
        """

        collection_name: str = "historical_feature_table"

        indexes = MaterializedTableModel.Settings.indexes + [
            [
                ("name", pymongo.TEXT),
                ("description", pymongo.TEXT),
            ],
        ]
