"""
BatchRequestTableModel API payload schema
"""

from __future__ import annotations

from typing import List

from featurebyte.models.batch_request_table import BatchRequestInput, BatchRequestTableModel
from featurebyte.schema.common.base import PaginationMixin
from featurebyte.schema.request_table import BaseRequestTableCreate, BaseRequestTableListRecord


class BatchRequestTableCreate(BaseRequestTableCreate):
    """
    BatchRequestTableModel creation schema
    """

    request_input: BatchRequestInput


class BatchRequestTableList(PaginationMixin):
    """
    Schema for listing batch request tables
    """

    data: List[BatchRequestTableModel]


class BatchRequestTableListRecord(BaseRequestTableListRecord):
    """
    This model determines the schema when listing batch request tables via BatchRequestTable.list()
    """
