"""
EventData API payload schema
"""
from __future__ import annotations

from typing import Any, List, Optional

from beanie import PydanticObjectId
from bson.objectid import ObjectId
from pydantic import Field, StrictStr

from featurebyte.enum import DBVarType
from featurebyte.models.base import FeatureByteBaseModel
from featurebyte.models.event_data import EventDataModel, EventDataStatus, FeatureJobSetting
from featurebyte.models.feature_store import ColumnInfo, TableDetails, TabularSource
from featurebyte.routes.common.schema import PaginationMixin
from featurebyte.schema.common.base import BaseBriefInfo, BaseInfo
from featurebyte.schema.common.operation import DictProject
from featurebyte.schema.entity import EntityBriefInfoList


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


class EventDataBriefInfo(BaseBriefInfo):
    """
    EventData brief info schema
    """

    status: EventDataStatus


class EventDataBriefInfoList(FeatureByteBaseModel):
    """
    Paginated list of event data brief info
    """

    __root__: List[EventDataBriefInfo]

    @classmethod
    def from_paginated_data(cls, paginated_data: dict[str, Any]) -> EventDataBriefInfoList:
        """
        Construct event data brief info list from paginated data

        Parameters
        ----------
        paginated_data: dict[str, Any]
            Paginated data

        Returns
        -------
        EventDataBriefInfoList
        """
        event_data_project = DictProject(rule=("data", ["name", "status"]))
        return EventDataBriefInfoList(__root__=event_data_project.project(paginated_data))


class EventDataColumnInfo(FeatureByteBaseModel):
    """
    EventDataColumnInfo for storing column information

    name: str
        Column name
    dtype: DBVarType
        Variable type of the column
    entity: str
        Entity name associated with the column
    """

    name: StrictStr
    dtype: DBVarType
    entity: Optional[str] = Field(default=None)


class EventDataInfo(EventDataBriefInfo, BaseInfo):
    """
    EventData info schema
    """

    event_timestamp_column: str
    record_creation_date_column: str
    table_details: TableDetails
    default_feature_job_setting: Optional[FeatureJobSetting]
    entities: EntityBriefInfoList
    column_count: int
    # feature_count: int
    columns_info: Optional[List[EventDataColumnInfo]]
