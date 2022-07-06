"""
EventData API payload schema
"""
from typing import Dict, List, Optional, Tuple

import datetime

from beanie import PydanticObjectId
from bson.objectid import ObjectId
from pydantic import BaseModel, Field

from featurebyte.models.event_data import EventDataModel, EventDataStatus, FeatureJobSetting
from featurebyte.models.feature_store import FeatureStoreModel, TableDetails
from featurebyte.routes.common.schema import PaginationMixin


class EventData(EventDataModel):
    """
    Event Data Document Model
    """

    id: Optional[PydanticObjectId] = Field(alias="_id", default_factory=ObjectId)
    user_id: Optional[PydanticObjectId]
    created_at: datetime.datetime
    status: EventDataStatus

    class Config:
        """
        Configuration for Event Data schema
        """

        # pylint: disable=too-few-public-methods

        json_encoders = {ObjectId: str}


class EventDataCreate(BaseModel):
    """
    Event Data Creation schema
    """

    name: str
    tabular_source: Tuple[FeatureStoreModel, TableDetails]
    event_timestamp_column: str
    column_entity_map: Dict[str, str] = Field(default_factory=dict)
    record_creation_date_column: Optional[str]
    default_feature_job_setting: Optional[FeatureJobSetting]


class EventDataList(PaginationMixin):
    """
    Paginated list of Event Data
    """

    data: List[EventData]


class EventDataUpdate(BaseModel):
    """
    Event Data update schema
    """

    default_feature_job_setting: Optional[FeatureJobSetting]
    status: Optional[EventDataStatus]
