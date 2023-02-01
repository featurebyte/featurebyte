"""
Feature API payload schema
"""
from __future__ import annotations

from typing import Any, Dict, List, Optional

from datetime import datetime

from bson.objectid import ObjectId
from pydantic import Field, StrictStr

from featurebyte.enum import DBVarType
from featurebyte.models.base import FeatureByteBaseModel, PydanticObjectId, VersionIdentifier
from featurebyte.models.event_data import FeatureJobSetting
from featurebyte.models.feature import FeatureModel, FeatureReadiness
from featurebyte.query_graph.graph import QueryGraph
from featurebyte.query_graph.model.common_table import TabularSource
from featurebyte.schema.common.base import BaseDocumentServiceUpdateSchema, PaginationMixin
from featurebyte.schema.common.operation import DictProject


class FeatureCreate(FeatureByteBaseModel):
    """
    Feature Creation schema
    """

    id: Optional[PydanticObjectId] = Field(default_factory=ObjectId, alias="_id")
    name: StrictStr
    dtype: DBVarType
    graph: QueryGraph
    node_name: str
    tabular_source: TabularSource
    tabular_data_ids: List[PydanticObjectId] = Field(min_items=1)
    entity_ids: List[PydanticObjectId] = Field(min_items=0)
    feature_namespace_id: Optional[PydanticObjectId] = Field(default_factory=ObjectId)


class FeatureNewVersionCreate(FeatureByteBaseModel):
    """
    New version creation schema based on existing feature
    """

    source_feature_id: PydanticObjectId
    feature_job_setting: Optional[FeatureJobSetting]


class FeaturePaginatedList(PaginationMixin):
    """
    Paginated list of features
    """

    data: List[FeatureModel]


class FeatureUpdate(FeatureByteBaseModel):
    """
    Feature update schema
    """

    readiness: Optional[FeatureReadiness]


class FeatureServiceUpdate(BaseDocumentServiceUpdateSchema, FeatureUpdate):
    """
    Feature service update schema
    """

    online_enabled: Optional[bool]
    feature_list_ids: Optional[List[PydanticObjectId]]
    deployed_feature_list_ids: Optional[List[PydanticObjectId]]


class ReadinessComparison(FeatureByteBaseModel):
    """
    Readiness comparison schema
    """

    this: FeatureReadiness
    default: FeatureReadiness


class VersionComparison(FeatureByteBaseModel):
    """
    Readiness comparison schema
    """

    this: str
    default: str

    @classmethod
    def from_version_identifier(
        cls, this: VersionIdentifier, default: VersionIdentifier
    ) -> VersionComparison:
        """
        Construct VersionComparison object using VersionIdentifier objects

        Parameters
        ----------
        this: VersionIdentifier
            Current object version ID
        default: VersionIdentifier
            Default object version ID

        Returns
        -------
        VersionIdentifier
        """
        return cls(this=this.to_str(), default=default.to_str())


class FeatureBriefInfo(FeatureByteBaseModel):
    """
    Feature brief info schema
    """

    version: VersionIdentifier
    readiness: FeatureReadiness
    created_at: datetime


class FeatureBriefInfoList(FeatureByteBaseModel):
    """
    Paginated list of feature brief info
    """

    __root__: List[FeatureBriefInfo]

    @classmethod
    def from_paginated_data(cls, paginated_data: dict[str, Any]) -> FeatureBriefInfoList:
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
        feature_project = DictProject(rule=("data", ["version", "readiness", "created_at"]))
        return FeatureBriefInfoList(__root__=feature_project.project(paginated_data))


class FeatureSQL(FeatureByteBaseModel):
    """
    Feature SQL schema
    """

    graph: QueryGraph
    node_name: str


class FeaturePreview(FeatureSQL):
    """
    Feature Preview schema
    """

    feature_store_name: StrictStr
    point_in_time_and_serving_name: Dict[str, Any]
