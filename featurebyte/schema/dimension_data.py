"""
DimensionDataData API payload schema
"""
from __future__ import annotations

from typing import List, Optional

from pydantic import Field, StrictStr

from featurebyte.enum import DBVarType
from featurebyte.models.base import FeatureByteBaseModel
from featurebyte.models.dimension_data import DimensionDataModel
from featurebyte.models.feature_store import TableDetails
from featurebyte.schema.common.base import BaseInfo, PaginationMixin
from featurebyte.schema.data import DataCreate, DataUpdate
from featurebyte.schema.entity import EntityBriefInfoList


class DimensionDataCreate(DataCreate):
    """
    DimensionData Creation Schema
    """

    dimension_data_id_column: StrictStr


class DimensionDataList(PaginationMixin):
    """
    Paginated list of Event Data
    """

    data: List[DimensionDataModel]


class DimensionDataUpdate(DataUpdate):
    """
    DimensionData Update Schema
    """


class DimensionDataColumnInfo(FeatureByteBaseModel):
    """
    EventDataColumnInfo for storing column information

    name: str
        Column name
    dtype: DBVarType
        Variable type of the column
    entity: str
        Entity name associated with the column
    """

    name: StrictStr
    dtype: DBVarType
    entity: Optional[str] = Field(default=None)


class DimensionDataInfo(BaseInfo):
    """
    DimensionData info schema
    """

    dimension_data_id_column: str
    record_creation_date_column: Optional[str]
    table_details: TableDetails
    entities: EntityBriefInfoList
    columns_info: Optional[List[DimensionDataColumnInfo]]
