"""
StaticSourceTableModel API payload schema
"""

from __future__ import annotations

from typing import List, Optional

from bson import ObjectId
from pydantic import Field

from featurebyte.models.base import FeatureByteBaseModel, NameStr, PydanticObjectId
from featurebyte.models.static_source_table import StaticSourceInput, StaticSourceTableModel
from featurebyte.schema.common.base import PaginationMixin
from featurebyte.schema.request_table import BaseRequestTableListRecord


class StaticSourceTableCreate(FeatureByteBaseModel):
    """
    StaticSourceTableModel creation schema
    """

    id: Optional[PydanticObjectId] = Field(default_factory=ObjectId, alias="_id")
    name: NameStr
    feature_store_id: PydanticObjectId
    sample_rows: Optional[int] = Field(ge=0, default=None)
    request_input: StaticSourceInput


class StaticSourceTableList(PaginationMixin):
    """
    Schema for listing static source tables
    """

    data: List[StaticSourceTableModel]


class StaticSourceTableListRecord(BaseRequestTableListRecord):
    """
    This model determines the schema when listing static source tables via StaticSourceTable.list()
    """
