"""
EventData API payload schema
"""
from __future__ import annotations

from typing import Any, List, Optional

from beanie import PydanticObjectId
from bson.objectid import ObjectId
from pydantic import Field, StrictStr

from featurebyte.models.base import FeatureByteBaseModel
from featurebyte.models.event_data import EventDataModel, EventDataStatus, FeatureJobSetting
from featurebyte.models.feature_store import ColumnInfo, TableDetails, TabularSource
from featurebyte.routes.common.schema import PaginationMixin
from featurebyte.schema.common.base import BaseBriefInfo, BaseInfo
from featurebyte.schema.common.operation import DictProject, DictTransform
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


class EventDataBriefInfoList(PaginationMixin):
    """
    Paginated list of event data brief info
    """

    data: List[EventDataBriefInfo]

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
        event_data_transform = DictTransform(
            rule={
                "__root__": DictProject(rule=["page", "page_size", "total"]),
                "data": DictProject(rule=("data", ["name", "status"])),
            }
        )
        return EventDataBriefInfoList(**event_data_transform.transform(paginated_data))


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
