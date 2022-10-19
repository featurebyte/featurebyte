"""
ItemData API payload schema
"""
from typing import List, Optional

from bson.objectid import ObjectId
from pydantic import Field, StrictStr

from featurebyte.models.base import FeatureByteBaseModel, PydanticObjectId
from featurebyte.models.feature_store import ColumnInfo, DataStatus, TabularSource


class ItemDataCreate(FeatureByteBaseModel):
    """
    ItemData creation schema
    """

    id: Optional[PydanticObjectId] = Field(default_factory=ObjectId, alias="_id")
    name: StrictStr
    tabular_source: TabularSource
    columns_info: List[ColumnInfo]
    event_id_column: StrictStr
    item_id_column: StrictStr


class ItemDataUpdate(FeatureByteBaseModel):
    """
    ItemData update schema
    """

    columns_info: Optional[List[ColumnInfo]]
    status: Optional[DataStatus]
