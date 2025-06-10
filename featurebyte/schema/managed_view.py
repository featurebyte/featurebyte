"""
Schema for Managed View.
"""

from typing import List, Optional

from bson import ObjectId
from pydantic import Field, StrictStr

from featurebyte.models.base import FeatureByteBaseModel, NameStr, PydanticObjectId
from featurebyte.models.managed_view import ManagedViewModel
from featurebyte.query_graph.model.column_info import ColumnInfo
from featurebyte.query_graph.model.common_table import TabularSource
from featurebyte.query_graph.node.schema import TableDetails
from featurebyte.schema.common.base import (
    BaseDocumentServiceUpdateSchema,
    BaseInfo,
    PaginationMixin,
)


class ManagedViewCreateBase(FeatureByteBaseModel):
    """
    ManagedView creation base schema
    """

    id: Optional[PydanticObjectId] = Field(default_factory=ObjectId, alias="_id")
    name: NameStr
    description: Optional[StrictStr] = Field(default=None)
    sql: StrictStr


class ManagedViewCreate(ManagedViewCreateBase):
    """
    ManagedView creation schema
    """

    feature_store_id: PydanticObjectId
    is_global: bool = Field(default=False)


class ManagedViewServiceCreate(ManagedViewCreateBase):
    """
    ManagedView service creation schema
    """

    catalog_id: Optional[PydanticObjectId] = Field(default=None)
    tabular_source: TabularSource
    columns_info: Optional[List[ColumnInfo]] = Field(default=None)


class ManagedViewList(PaginationMixin):
    """
    Paginated list of ManagedView
    """

    data: List[ManagedViewModel]


class ManagedViewRead(ManagedViewModel):
    """
    ManagedView read payload schema
    """


class ManagedViewUpdate(BaseDocumentServiceUpdateSchema):
    """
    ManagedView update payload schema
    """

    name: NameStr


class ManagedViewTableInfo(FeatureByteBaseModel):
    """
    ManagedView table info schema
    """

    id: PydanticObjectId
    name: str


class ManagedViewInfo(BaseInfo):
    """
    ManagedView info schema
    """

    name: str
    feature_store_name: str
    table_details: TableDetails
    sql: StrictStr
    used_by_tables: List[ManagedViewTableInfo]
