"""
ColumnStatisticsService class
"""

from __future__ import annotations

from featurebyte.models.base import PydanticObjectId
from featurebyte.models.column_statistics import ColumnStatisticsInfo, ColumnStatisticsModel
from featurebyte.schema.common.base import BaseDocumentServiceUpdateSchema
from featurebyte.service.base_document import BaseDocumentService


class ColumnStatisticsService(
    BaseDocumentService[
        ColumnStatisticsModel, ColumnStatisticsModel, BaseDocumentServiceUpdateSchema
    ]
):
    """
    ColumnStatisticsService class
    """

    document_class = ColumnStatisticsModel

    async def get_column_statistics_info(self) -> ColumnStatisticsInfo:
        """
        Get column statistics information for all tables in the catalog

        Returns
        -------
        ColumnStatisticsInfo
            Column statistics information
        """
        all_column_statistics: dict[PydanticObjectId, dict[str, ColumnStatisticsModel]] = {}
        async for model in self.list_documents_iterator(query_filter={}):
            table_id = model.table_id
            column_name = model.column_name
            if table_id not in all_column_statistics:
                all_column_statistics[table_id] = {}
            all_column_statistics[table_id][column_name] = model
        return ColumnStatisticsInfo(all_column_statistics=all_column_statistics)
