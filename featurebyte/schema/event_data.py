"""
EventData API payload schema
"""
from __future__ import annotations

from typing import Any, List, Optional

from bson.objectid import ObjectId
from pydantic import Field, StrictStr

from featurebyte.enum import DBVarType
from featurebyte.models.base import FeatureByteBaseModel, PydanticObjectId
from featurebyte.models.event_data import EventDataModel, FeatureJobSetting
from featurebyte.models.feature_store import ColumnInfo, DataStatus, TableDetails, TabularSource
from featurebyte.routes.common.schema import PaginationMixin
from featurebyte.schema.common.base import BaseBriefInfo, BaseInfo
from featurebyte.schema.common.operation import DictProject
from featurebyte.schema.entity import EntityBriefInfoList


class EventDataCreate(FeatureByteBaseModel):
    """
    EventData Creation Schema
    """

    id: Optional[PydanticObjectId] = Field(default_factory=ObjectId, alias="_id")
    name: StrictStr
    tabular_source: TabularSource
    columns_info: List[ColumnInfo]
    event_id_column: StrictStr
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
    EventData Update Schema
    """

    columns_info: Optional[List[ColumnInfo]]
    default_feature_job_setting: Optional[FeatureJobSetting]
    record_creation_date_column: Optional[StrictStr]
    status: Optional[DataStatus]


class EventDataBriefInfo(BaseBriefInfo):
    """
    EventData brief info schema
    """

    status: DataStatus


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
    record_creation_date_column: Optional[str]
    table_details: TableDetails
    default_feature_job_setting: Optional[FeatureJobSetting]
    entities: EntityBriefInfoList
    column_count: int
    # feature_count: int
    columns_info: Optional[List[EventDataColumnInfo]]
