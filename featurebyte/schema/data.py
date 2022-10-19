"""
Data model's attribute payload schema
"""
from typing import List, Optional

from bson.objectid import ObjectId
from pydantic import Field, StrictStr

from featurebyte.models.base import FeatureByteBaseModel, PydanticObjectId
from featurebyte.models.feature_store import ColumnInfo, DataStatus, TabularSource


class DataCreate(FeatureByteBaseModel):
    """
    DataService create schema
    """

    id: Optional[PydanticObjectId] = Field(default_factory=ObjectId, alias="_id")
    name: StrictStr
    tabular_source: TabularSource
    columns_info: List[ColumnInfo]
    record_creation_date_column: Optional[StrictStr]


class DataUpdate(FeatureByteBaseModel):
    """
    DataService update schema
    """

    columns_info: Optional[List[ColumnInfo]]
    status: Optional[DataStatus]
    record_creation_date_column: Optional[StrictStr]
