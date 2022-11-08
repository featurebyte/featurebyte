"""
Data model's attribute payload schema
"""
from __future__ import annotations

from typing import Any, List, Optional

from bson.objectid import ObjectId
from pydantic import Field, StrictStr

from featurebyte.models.base import FeatureByteBaseModel, PydanticObjectId
from featurebyte.models.feature_store import ColumnInfo, DataStatus, TabularSource
from featurebyte.schema.common.base import BaseBriefInfo, BaseDocumentServiceUpdateSchema
from featurebyte.schema.common.operation import DictProject


class DataCreate(FeatureByteBaseModel):
    """
    DataService create schema
    """

    id: Optional[PydanticObjectId] = Field(default_factory=ObjectId, alias="_id")
    name: StrictStr
    tabular_source: TabularSource
    columns_info: List[ColumnInfo]
    record_creation_date_column: Optional[StrictStr]


class DataUpdate(BaseDocumentServiceUpdateSchema):
    """
    DataService update schema
    """

    columns_info: Optional[List[ColumnInfo]]
    status: Optional[DataStatus]
    record_creation_date_column: Optional[StrictStr]


class DataBriefInfo(BaseBriefInfo):
    """
    Data brief info schema
    """

    status: DataStatus


class DataBriefInfoList(FeatureByteBaseModel):
    """
    Paginated list of data brief info
    """

    __root__: List[DataBriefInfo]

    @classmethod
    def from_paginated_data(cls, paginated_data: dict[str, Any]) -> DataBriefInfoList:
        """
        Construct data brief info list from paginated data

        Parameters
        ----------
        paginated_data: dict[str, Any]
            Paginated data

        Returns
        -------
        DataBriefInfoList
        """
        data_project = DictProject(rule=("data", ["name", "status"]))
        return DataBriefInfoList(__root__=data_project.project(paginated_data))
