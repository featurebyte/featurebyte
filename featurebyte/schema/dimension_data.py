"""
DimensionDataData API payload schema
"""
from __future__ import annotations

from typing import List, Literal

from pydantic import Field, StrictStr

from featurebyte.enum import TableDataType
from featurebyte.models.dimension_data import DimensionDataModel
from featurebyte.schema.common.base import PaginationMixin
from featurebyte.schema.tabular_data import DataCreate, DataServiceUpdate, DataUpdate


class DimensionDataCreate(DataCreate):
    """
    DimensionData Creation Schema
    """

    type: Literal[TableDataType.DIMENSION_DATA] = Field(TableDataType.DIMENSION_DATA, const=True)
    dimension_id_column: StrictStr


class DimensionDataList(PaginationMixin):
    """
    Paginated list of Event Data
    """

    data: List[DimensionDataModel]


class DimensionDataUpdate(DataUpdate):
    """
    DimensionData update payload schema
    """


class DimensionDataServiceUpdate(DataServiceUpdate):
    """
    DimensionData service update schema
    """
