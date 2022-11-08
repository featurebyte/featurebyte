"""
DimensionDataData API payload schema
"""
from __future__ import annotations

from typing import List

from pydantic import StrictStr

from featurebyte.models.dimension_data import DimensionDataModel
from featurebyte.schema.common.base import PaginationMixin
from featurebyte.schema.data import DataCreate, DataUpdate


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
