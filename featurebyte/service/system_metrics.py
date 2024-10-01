"""
SystemMetricsServiceClass
"""

from __future__ import annotations

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

    async def create_metrics(self, metrics_data: SystemMetricsData) -> None:
        """
        Create a new metrics document

        Parameters
        ----------
        metrics_data : SystemMetricsData
            Metrics data to create
        """
        await self.create_document(SystemMetricsModel(metrics_data=metrics_data))
