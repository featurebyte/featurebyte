"""
ColumnStatistics class
"""

from typing import List

from pymongo import IndexModel

from featurebyte.models.base import (
    FeatureByteBaseModel,
    FeatureByteCatalogBaseDocumentModel,
    PydanticObjectId,
    UniqueValuesConstraint,
)


class StatisticsModel(FeatureByteBaseModel):
    """
    StatisticsModel class
    """

    distinct_count: int


class ColumnStatisticsModel(FeatureByteCatalogBaseDocumentModel):
    """
    ColumnStatistics class
    """

    table_id: PydanticObjectId
    column_name: str
    stats: StatisticsModel

    class Settings(FeatureByteCatalogBaseDocumentModel.Settings):
        """
        MongoDB Settings
        """

        collection_name: str = "column_statistics"
        unique_constraints: List[UniqueValuesConstraint] = []

        indexes = FeatureByteCatalogBaseDocumentModel.Settings.indexes + [
            IndexModel("table_id"),
            IndexModel("column_name"),
        ]
