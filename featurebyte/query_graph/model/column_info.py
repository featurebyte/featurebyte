"""
This module contains column info related models.
"""
from typing import Optional

from pydantic import Field, StrictStr

from featurebyte.enum import DBVarType
from featurebyte.models.base import FeatureByteBaseModel, PydanticObjectId
from featurebyte.query_graph.model.critical_data_info import CriticalDataInfo


class ColumnSpec(FeatureByteBaseModel):
    """
    Schema for columns retrieval
    """

    name: StrictStr
    dtype: DBVarType


class ColumnInfo(ColumnSpec):
    """
    ColumnInfo for storing column information

    name: str
        Column name
    dtype: DBVarType
        Variable type of the column
    entity_id: Optional[PydanticObjectId]
        Entity id associated with the column
    critical_data_info: Optional[CriticalDataInfo]
        Critical data info of the column
    """

    entity_id: Optional[PydanticObjectId] = Field(default=None)
    semantic_id: Optional[PydanticObjectId] = Field(default=None)
    critical_data_info: Optional[CriticalDataInfo] = Field(default=None)
