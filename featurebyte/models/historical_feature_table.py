"""
HistoricalFeatureTableModel
"""
from __future__ import annotations

from typing import Dict, Optional

import pymongo
from pydantic import Field, StrictStr

from featurebyte.models.base import PydanticObjectId
from featurebyte.models.base_feature_or_target_table import BaseFeatureOrTargetTableModel
from featurebyte.models.materialized_table import MaterializedTableModel


class HistoricalFeatureTableModel(BaseFeatureOrTargetTableModel):
    """
    HistoricalFeatureTable is the result of asynchronous historical features requests
    """

    feature_list_id: PydanticObjectId
    most_recent_point_in_time: Optional[StrictStr] = Field(default=None)
    least_recent_point_in_time: Optional[StrictStr] = Field(default=None)
    entity_column_name_to_count: Optional[Dict[str, int]] = Field(default_factory=dict)
    min_interval_secs_between_entities: Optional[float] = Field(default_factory=None)

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
