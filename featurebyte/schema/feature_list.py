"""
FeatureList API payload schema
"""
from __future__ import annotations

from typing import Any, Dict, List, Optional

from datetime import datetime

from bson.objectid import ObjectId
from pydantic import Field, StrictStr, validator

from featurebyte.models.base import FeatureByteBaseModel, PydanticObjectId, VersionIdentifier
from featurebyte.models.feature_list import (
    FeatureListModel,
    FeatureListStatus,
    FeatureReadinessDistribution,
    FeatureTypeFeatureCount,
)
from featurebyte.query_graph.graph import Node, QueryGraph
from featurebyte.routes.common.schema import PaginationMixin
from featurebyte.schema.common.operation import DictProject
from featurebyte.schema.feature import VersionComparison
from featurebyte.schema.feature_namespace import NamespaceInfo


class FeatureListCreate(FeatureByteBaseModel):
    """
    FeatureList Creation schema
    """

    id: Optional[PydanticObjectId] = Field(default_factory=ObjectId, alias="_id")
    name: StrictStr
    feature_ids: List[PydanticObjectId] = Field(min_items=1)
    feature_list_namespace_id: Optional[PydanticObjectId] = Field(default_factory=ObjectId)


class FeatureListPaginatedList(PaginationMixin):
    """
    Paginated list of Entity
    """

    data: List[FeatureListModel]


class FeatureListUpdate(FeatureByteBaseModel):
    """
    FeatureList update schema
    """

    deployed: Optional[bool]


class FeatureListServiceUpdate(FeatureListUpdate):
    """
    FeatureList service update schema
    """

    online_enabled_feature_ids: Optional[List[PydanticObjectId]]
    readiness_distribution: Optional[FeatureReadinessDistribution]


class ProductionReadyFractionComparison(FeatureByteBaseModel):
    """
    Production ready fraction comparison
    """

    this: float
    default: float


class FeatureListBriefInfo(FeatureByteBaseModel):
    """
    FeatureList brief info schema
    """

    version: VersionIdentifier
    readiness_distribution: FeatureReadinessDistribution
    created_at: datetime
    production_ready_fraction: Optional[float] = Field(default=None)

    @validator("production_ready_fraction")
    @classmethod
    def _derive_production_ready_fraction(cls, value: Any, values: dict[str, Any]) -> Any:
        if value is None:
            return values["readiness_distribution"].derive_production_ready_fraction()
        return value


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


class FeatureListPreviewGroup(FeatureByteBaseModel):
    """
    FeatureList preview schema for a group of features from the same feature store
    """

    feature_store_name: str
    graph: QueryGraph
    nodes: List[Node]


class FeatureListPreview(FeatureByteBaseModel):
    """
    FeatureList preview schema
    """

    preview_groups: List[FeatureListPreviewGroup]
    point_in_time_and_serving_name: Dict[str, Any]


class FeatureListHistorical(FeatureByteBaseModel):
    """
    FeatureList preview schema
    """

    graph: QueryGraph
    nodes: List[Node]
    point_in_time_and_serving_name: Dict[str, Any]
