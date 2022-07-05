"""
EventData API payload schema
"""
# pylint: disable=too-few-public-methods
from typing import Dict, List, Optional, Tuple

import datetime

from beanie import PydanticObjectId
from bson.objectid import ObjectId
from pydantic import BaseModel, Field

from featurebyte.models.event_data import EventDataModel, EventDataStatus, FeatureJobSetting
from featurebyte.models.feature_store import FeatureStoreModel, TableDetails
from featurebyte.routes.common.schema import PaginationMixin, ResponseModel


class EventDataCreate(BaseModel):
    """
    Event Data Creation Payload

    Parameters
    ----------
    """

    name: str
    tabular_source: Tuple[FeatureStoreModel, TableDetails]
    event_timestamp_column: str
    column_entity_map: Dict[str, str] = Field(default_factory=dict)
    record_creation_date_column: Optional[str]
    default_feature_job_setting: Optional[FeatureJobSetting]


class EventData(EventDataModel, ResponseModel):
    """
    Event Data

    id: ObjectId
        Document identifier
    user_id: ObjectId
        User identifier
    """

    user_id: Optional[PydanticObjectId]
    created_at: datetime.datetime
    status: EventDataStatus

    class Config:
        """
        Configuration for Event Data schema
        """

        allow_population_by_field_name = True
        arbitrary_types_allowed = True
        json_encoders = {ObjectId: str}


class EventDataList(PaginationMixin):
    """
    Paginated list of Event Datas
    """

    data: List[EventData]

    class Config:
        """
        Configuration for Event Datas schema
        """

        allow_population_by_field_name = True
        arbitrary_types_allowed = True
        json_encoders = {ObjectId: str}


class EventDataUpdate(BaseModel):
    """
    Event Data update schema
    """

    default_feature_job_setting: Optional[FeatureJobSetting]
    status: Optional[EventDataStatus]
