"""
SystemMetricsServiceClass
"""

from __future__ import annotations

from typing import Any, Dict, Optional

from featurebyte.models.system_metrics import SystemMetricsData, SystemMetricsModel
from featurebyte.schema.common.base import BaseDocumentServiceUpdateSchema
from featurebyte.service.base_document import BaseDocumentService


class SystemMetricsService(
    BaseDocumentService[SystemMetricsModel, SystemMetricsModel, BaseDocumentServiceUpdateSchema]
):
    """
    SystemMetricsService class
    """

    document_class = SystemMetricsModel

    async def get_metrics(self, metadata: Dict[str, Any]) -> Optional[SystemMetricsModel]:
        """
        Get metrics document
        """
        query_filter = {"metadata": metadata}
        async for doc in self.list_documents_as_dict_iterator(query_filter=query_filter):
            return SystemMetricsModel(**doc)
        return None

    async def create_metrics(
        self, metrics: SystemMetricsData, metadata: Optional[Dict[str, Any]] = None
    ) -> None:
        """
        Create a new metrics document
        """
        await self.create_document(
            SystemMetricsModel(
                metrics=metrics,
                metadata=metadata,
            ),
        )
