"""
EventData API payload schema
"""
from __future__ import annotations

from typing import List, Literal, Optional

from pydantic import Field, StrictStr

from featurebyte.enum import TableDataType
from featurebyte.models.event_data import EventDataModel, FeatureJobSetting
from featurebyte.schema.common.base import PaginationMixin
from featurebyte.schema.tabular_data import DataCreate, DataUpdate


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


class EventDataUpdate(DataUpdate):
    """
    EventData Update Schema
    """

    default_feature_job_setting: Optional[FeatureJobSetting]
