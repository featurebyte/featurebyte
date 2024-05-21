"""
HistoricalFeatureTableModel
"""

from __future__ import annotations

from typing import Optional

import pymongo

from featurebyte.models.base import PydanticObjectId
from featurebyte.models.base_feature_or_target_table import BaseFeatureOrTargetTableModel
from featurebyte.models.materialized_table import MaterializedTableModel


class HistoricalFeatureTableModel(BaseFeatureOrTargetTableModel):
    """
    HistoricalFeatureTable is the result of asynchronous historical features requests
    """

    # Id of the feature list used to compute the historical feature table. None if the feature list
    # was not saved.
    feature_list_id: Optional[PydanticObjectId]

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
