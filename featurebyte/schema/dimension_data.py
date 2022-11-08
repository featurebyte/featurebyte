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
