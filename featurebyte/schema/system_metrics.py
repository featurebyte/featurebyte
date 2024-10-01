"""
System metrics related schemas
"""

from typing import List

from featurebyte.models.system_metrics import SystemMetricsModel
from featurebyte.schema.common.base import PaginationMixin


class SystemMetricsList(PaginationMixin):
    """
    SystemMetricsList used to deserialize list document output
    """

    data: List[SystemMetricsModel]
