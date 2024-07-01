"""
HistoricalFeatureTableModel
"""

from __future__ import annotations

from typing import List, Optional

import pymongo
from pydantic import Field

from featurebyte.models.base import FeatureByteBaseModel, PydanticObjectId
from featurebyte.models.base_feature_or_target_table import BaseFeatureOrTargetTableModel
from featurebyte.models.materialized_table import MaterializedTableModel


class FeatureInfo(FeatureByteBaseModel):
    """Features Info"""

    feature_id: PydanticObjectId
    feature_name: str


class HistoricalFeatureTableModel(BaseFeatureOrTargetTableModel):
    """
    HistoricalFeatureTable is the result of asynchronous historical features requests
    """

    # Id of the feature list used to compute the historical feature table. None if the feature list
    # was not saved.
    feature_list_id: Optional[PydanticObjectId]
    features_info: Optional[List[FeatureInfo]] = Field(default=None)

    @property
    def feature_names(self) -> Optional[List[str]]:
        """
        List of feature names associated with the historical feature table.

        Returns
        -------
        Optional[List[str]]
        """
        if self.features_info is None:
            return None
        return [feature_info.feature_name for feature_info in self.features_info]

    @property
    def feature_ids(self) -> Optional[List[PydanticObjectId]]:
        """
        List of feature IDs associated with the historical feature table.

        Returns
        -------
        Optional[List[PydanticObjectId]]
        """
        if self.features_info is None:
            return None
        return [feature_info.feature_id for feature_info in self.features_info]

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
