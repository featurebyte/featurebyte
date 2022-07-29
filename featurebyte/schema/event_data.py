"""
EventData API payload schema
"""
from typing import Dict, List, Optional

from beanie import PydanticObjectId
from pydantic import Field, StrictStr

from featurebyte.models.base import FeatureByteBaseModel
from featurebyte.models.event_data import EventDataModel, EventDataStatus, FeatureJobSetting
from featurebyte.models.feature_store import TabularSource
from featurebyte.routes.common.schema import PaginationMixin


class EventDataCreate(FeatureByteBaseModel):
    """
    Event Data Creation Schema
    """

    id: PydanticObjectId = Field(alias="_id")
    name: StrictStr
    tabular_source: TabularSource
    event_timestamp_column: StrictStr
    column_entity_map: Optional[Dict[StrictStr, str]] = Field(default=None)
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

    column_entity_map: Optional[Dict[StrictStr, str]] = Field(default=None)
    default_feature_job_setting: Optional[FeatureJobSetting]
    status: Optional[EventDataStatus]
