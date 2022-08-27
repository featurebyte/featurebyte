"""
EventData API payload schema
"""
from typing import List, Optional

from beanie import PydanticObjectId
from bson.objectid import ObjectId
from pydantic import Field, StrictStr

from featurebyte.models.base import FeatureByteBaseModel
from featurebyte.models.event_data import EventDataModel, EventDataStatus, FeatureJobSetting
from featurebyte.models.feature_store import ColumnInfo, TabularSource
from featurebyte.routes.common.schema import PaginationMixin


class EventDataCreate(FeatureByteBaseModel):
    """
    Event Data Creation Schema
    """

    id: Optional[PydanticObjectId] = Field(default_factory=ObjectId, alias="_id")
    name: StrictStr
    tabular_source: TabularSource
    columns_info: List[ColumnInfo]
    event_timestamp_column: StrictStr
    record_creation_date_column: Optional[StrictStr]
    default_feature_job_setting: Optional[FeatureJobSetting]


class EventDataList(PaginationMixin):
    """
    Paginated list of Event Data
    """

    data: List[EventDataModel]


class EventDataUpdate(FeatureByteBaseModel):
    """
    Event Data Update Schema
    """

    columns_info: List[ColumnInfo]
    default_feature_job_setting: Optional[FeatureJobSetting]
    record_creation_date_column: Optional[StrictStr]
    status: Optional[EventDataStatus]
