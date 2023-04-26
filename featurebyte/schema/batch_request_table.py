"""
BatchRequestTableModel API payload schema
"""
from __future__ import annotations

from typing import List

from featurebyte.models.batch_request_table import BatchRequestInput, BatchRequestTableModel
from featurebyte.models.request_input import RequestInputType
from featurebyte.query_graph.node.schema import ColumnSpec, TableDetails
from featurebyte.schema.common.base import BaseInfo, PaginationMixin
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


class BatchRequestTableInfo(BaseInfo):
    """
    BatchRequestTable info schema
    """

    type: RequestInputType
    feature_store_name: str
    table_details: TableDetails
    columns_info: List[ColumnSpec]
