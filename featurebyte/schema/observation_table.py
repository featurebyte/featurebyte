"""
ObservationTableModel API payload schema
"""
from __future__ import annotations

from typing import List, Optional

from pydantic import Field

from featurebyte.models.observation_table import ObservationInput, ObservationTableModel
from featurebyte.models.request_input import RequestInputType
from featurebyte.query_graph.node.schema import ColumnSpec, TableDetails
from featurebyte.schema.common.base import BaseInfo, PaginationMixin
from featurebyte.schema.request_table import BaseRequestTableCreate, BaseRequestTableListRecord


class ObservationTableCreate(BaseRequestTableCreate):
    """
    ObservationTableModel creation schema
    """

    sample_rows: Optional[int] = Field(ge=0)
    request_input: ObservationInput


class ObservationTableList(PaginationMixin):
    """
    Schema for listing observation tables
    """

    data: List[ObservationTableModel]


class ObservationTableListRecord(BaseRequestTableListRecord):
    """
    This model determines the schema when listing observation tables via ObservationTable.list()
    """


class ObservationTableInfo(BaseInfo):
    """
    ObservationTable info schema
    """

    type: RequestInputType
    feature_store_name: str
    table_details: TableDetails
