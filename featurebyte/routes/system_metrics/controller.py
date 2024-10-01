"""
System metrics API route controller
"""

from __future__ import annotations

from featurebyte.models.system_metrics import SystemMetricsModel
from featurebyte.routes.common.base import BaseDocumentController
from featurebyte.schema.system_metrics import SystemMetricsList
from featurebyte.service.system_metrics import SystemMetricsService


class SystemMetricsController(
    BaseDocumentController[SystemMetricsModel, SystemMetricsService, SystemMetricsList]
):
    """
    SystemMetricsList controller
    """

    paginated_document_class = SystemMetricsList
