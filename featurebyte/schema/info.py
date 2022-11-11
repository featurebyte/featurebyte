"""
Info related schema
"""
from __future__ import annotations

from typing import Any, List, Optional

from datetime import datetime

from pydantic import Field, StrictStr, root_validator

from featurebyte import SourceType
from featurebyte.enum import DBVarType
from featurebyte.models.base import FeatureByteBaseModel, PydanticObjectId, VersionIdentifier
from featurebyte.models.event_data import FeatureJobSetting
from featurebyte.models.feature import DefaultVersionMode
from featurebyte.models.feature_list import (
    FeatureListStatus,
    FeatureReadinessDistribution,
    FeatureTypeFeatureCount,
)
from featurebyte.models.feature_store import DatabaseDetails, DataStatus, TableDetails
from featurebyte.schema.common.base import BaseBriefInfo, BaseInfo
from featurebyte.schema.common.operation import DictProject
from featurebyte.schema.feature import FeatureBriefInfoList, ReadinessComparison, VersionComparison
from featurebyte.schema.feature_list import ProductionReadyFractionComparison


class FeatureStoreInfo(BaseInfo):
    """
    FeatureStore in schema
    """

    source: SourceType
    database_details: DatabaseDetails


class EntityBriefInfo(BaseBriefInfo):
    """
    Entity brief info schema
    """

    serving_names: List[str]


class EntityInfo(EntityBriefInfo, BaseInfo):
    """
    Entity info schema
    """


class EntityBriefInfoList(FeatureByteBaseModel):
    """
    Paginated list of entity brief info
    """

    __root__: List[EntityBriefInfo]

    @classmethod
    def from_paginated_data(cls, paginated_data: dict[str, Any]) -> EntityBriefInfoList:
        """
        Construct entity brief info list from paginated data

        Parameters
        ----------
        paginated_data: dict[str, Any]
            Paginated data

        Returns
        -------
        EntityBriefInfoList
        """
        entity_project = DictProject(rule=("data", ["name", "serving_names"]))
        return EntityBriefInfoList(__root__=entity_project.project(paginated_data))


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
    columns_info: Optional[List[EventDataColumnInfo]]


class NamespaceInfo(BaseInfo):
    """
    Namespace info schema
    """

    entities: EntityBriefInfoList
    tabular_data: DataBriefInfoList
    default_version_mode: DefaultVersionMode
    version_count: int


class FeatureNamespaceInfo(NamespaceInfo):
    """
    FeatureNamespace info schema
    """

    dtype: DBVarType
    default_feature_id: PydanticObjectId


class FeatureInfo(FeatureNamespaceInfo):
    """
    Feature info schema
    """

    dtype: DBVarType
    version: VersionComparison
    readiness: ReadinessComparison
    versions_info: Optional[FeatureBriefInfoList]
    metadata: Any


class FeatureListBriefInfo(FeatureByteBaseModel):
    """
    FeatureList brief info schema
    """

    version: VersionIdentifier
    readiness_distribution: FeatureReadinessDistribution
    created_at: datetime
    production_ready_fraction: Optional[float] = Field(default=None)

    @root_validator
    @classmethod
    def _derive_production_ready_fraction(cls, values: dict[str, Any]) -> Any:
        if "readiness_distribution" in values and values.get("production_ready_fraction") is None:
            values["production_ready_fraction"] = values[
                "readiness_distribution"
            ].derive_production_ready_fraction()
        return values


class FeatureListBriefInfoList(FeatureByteBaseModel):
    """
    Paginated list of feature brief info
    """

    __root__: List[FeatureListBriefInfo]

    @classmethod
    def from_paginated_data(cls, paginated_data: dict[str, Any]) -> FeatureListBriefInfoList:
        """
        Construct feature info list from paginated data

        Parameters
        ----------
        paginated_data: dict[str, Any]
            Paginated data

        Returns
        -------
        FeatureBriefInfoList
        """
        feature_list_project = DictProject(
            rule=("data", ["version", "readiness_distribution", "created_at"])
        )
        return FeatureListBriefInfoList(__root__=feature_list_project.project(paginated_data))


class FeatureListInfo(NamespaceInfo):
    """
    FeatureList info schema
    """

    dtype_distribution: List[FeatureTypeFeatureCount]
    status: FeatureListStatus
    feature_count: int
    version: VersionComparison
    production_ready_fraction: ProductionReadyFractionComparison
    versions_info: Optional[FeatureListBriefInfoList]


class FeatureListNamespaceInfo(NamespaceInfo):
    """
    FeatureListNamespace info schema
    """

    dtype_distribution: List[FeatureTypeFeatureCount]
    default_feature_list_id: PydanticObjectId
    status: FeatureListStatus
    feature_count: int
