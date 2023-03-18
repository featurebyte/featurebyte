"""
EventTable API payload schema
"""
from __future__ import annotations

from typing import List, Literal, Optional

from pydantic import Field, StrictStr

from featurebyte.enum import TableDataType
from featurebyte.models.base import FeatureByteBaseModel
from featurebyte.models.event_table import EventTableModel
from featurebyte.query_graph.model.feature_job_setting import FeatureJobSetting
from featurebyte.schema.common.base import PaginationMixin
from featurebyte.schema.table import TableCreate, TableServiceUpdate, TableUpdate


class EventTableCreate(TableCreate):
    """
    EventTable Creation Schema
    """

    type: Literal[TableDataType.EVENT_TABLE] = Field(TableDataType.EVENT_TABLE, const=True)
    event_id_column: StrictStr
    event_timestamp_column: StrictStr
    default_feature_job_setting: Optional[FeatureJobSetting]


class EventTableList(PaginationMixin):
    """
    Paginated list of EventTable
    """

    data: List[EventTableModel]


class EventTableUpdateMixin(FeatureByteBaseModel):
    """
    EventTable specific update schema
    """

    default_feature_job_setting: Optional[FeatureJobSetting]


class EventTableUpdate(EventTableUpdateMixin, TableUpdate):
    """
    EventTable update payload schema
    """


class EventTableServiceUpdate(EventTableUpdateMixin, TableServiceUpdate):
    """
    EventTable service update schema
    """
