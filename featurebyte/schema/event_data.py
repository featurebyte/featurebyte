"""
EventData API payload schema
"""
from typing import Dict, List, Optional, Tuple

from beanie import PydanticObjectId
from pydantic import BaseModel, Field, StrictStr

from featurebyte.models.event_data import EventDataModel, EventDataStatus, FeatureJobSetting
from featurebyte.models.feature_store import FeatureStoreModel, TableDetails
from featurebyte.routes.common.schema import PaginationMixin


class EventData(EventDataModel):
    """
    Event Data Document Model
    """

    user_id: Optional[PydanticObjectId]


class EventDataCreate(BaseModel):
    """
    Event Data Creation schema
    """

    name: StrictStr
    tabular_source: Tuple[FeatureStoreModel, TableDetails]
    event_timestamp_column: StrictStr
    column_entity_map: Optional[Dict[StrictStr, str]] = Field(default=None)
    record_creation_date_column: Optional[StrictStr]
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

    column_entity_map: Optional[Dict[StrictStr, str]] = Field(default=None)
    default_feature_job_setting: Optional[FeatureJobSetting]
    status: Optional[EventDataStatus]
