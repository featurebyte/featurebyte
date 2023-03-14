"""
EventData API payload schema
"""
from __future__ import annotations

from typing import List, Literal, Optional

from pydantic import Field, StrictStr

from featurebyte.enum import TableDataType
from featurebyte.models.base import FeatureByteBaseModel
from featurebyte.models.event_data import EventDataModel
from featurebyte.query_graph.model.feature_job_setting import FeatureJobSetting
from featurebyte.schema.common.base import PaginationMixin
from featurebyte.schema.tabular_data import DataCreate, DataServiceUpdate, DataUpdate


class EventDataCreate(DataCreate):
    """
    EventData Creation Schema
    """

    type: Literal[TableDataType.EVENT_DATA] = Field(TableDataType.EVENT_DATA, const=True)
    event_id_column: StrictStr
    event_timestamp_column: StrictStr
    default_feature_job_setting: Optional[FeatureJobSetting]


class EventDataList(PaginationMixin):
    """
    Paginated list of Event Data
    """

    data: List[EventDataModel]


class EventDataUpdateMixin(FeatureByteBaseModel):
    """
    EventData specific update schema
    """

    default_feature_job_setting: Optional[FeatureJobSetting]


class EventDataUpdate(EventDataUpdateMixin, DataUpdate):
    """
    EventData update payload schema
    """


class EventDataServiceUpdate(EventDataUpdateMixin, DataServiceUpdate):
    """
    EventData service update schema
    """
