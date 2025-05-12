"""
ColumnStatistics class
"""

from typing import List, Optional

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


class ColumnStatisticsInfo(FeatureByteBaseModel):
    """
    Column statistics information
    """

    all_column_statistics: dict[PydanticObjectId, dict[str, ColumnStatisticsModel]]

    def get_column_statistics(
        self, table_id: PydanticObjectId, column_name: str
    ) -> Optional[ColumnStatisticsModel]:
        """
        Get column statistics for a specific table and column

        Parameters
        ----------
        table_id : PydanticObjectId
            Table ID
        column_name : str
            Column name

        Returns
        -------
        Optional[ColumnStatisticsModel]
            Column statistics model
        """
        return self.all_column_statistics.get(table_id, {}).get(column_name)
