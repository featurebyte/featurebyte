"""
ColumnStatisticsService class
"""

from __future__ import annotations

from featurebyte.models.column_statistics import ColumnStatisticsModel
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

    async def get_catalog_column_statistics(self) -> list[ColumnStatisticsModel]:
        """
        Get column statistics for tables in the catalog

        Returns
        -------
        list[ColumnStatisticsModel]
            List of column statistics models
        """
        output = []
        async for model in self.list_documents_iterator(query_filter={}):
            output.append(model)
        return output
