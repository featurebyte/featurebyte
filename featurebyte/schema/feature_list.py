"""
FeatureList API payload schema
"""
from __future__ import annotations

from typing import Any, List, Optional

from datetime import datetime

from beanie import PydanticObjectId
from bson.objectid import ObjectId
from pydantic import Field, StrictStr, validator

from featurebyte.models.base import FeatureByteBaseModel
from featurebyte.models.feature_list import (
    FeatureListModel,
    FeatureListStatus,
    FeatureListVersionIdentifier,
    FeatureReadinessDistribution,
    FeatureTypeFeatureCount,
)
from featurebyte.routes.common.schema import PaginationMixin
from featurebyte.schema.common.operation import DictProject, DictTransform
from featurebyte.schema.feature import VersionComparison
from featurebyte.schema.feature_namespace import NamespaceInfo


class FeatureListCreate(FeatureByteBaseModel):
    """
    FeatureList Creation schema
    """

    id: Optional[PydanticObjectId] = Field(default_factory=ObjectId, alias="_id")
    name: StrictStr
    feature_ids: List[PydanticObjectId] = Field(min_items=1)
    version: Optional[FeatureListVersionIdentifier]
    feature_list_namespace_id: Optional[PydanticObjectId] = Field(default_factory=ObjectId)


class FeatureListPaginatedList(PaginationMixin):
    """
    Paginated list of Entity
    """

    data: List[FeatureListModel]


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

    version: FeatureListVersionIdentifier
    readiness_distribution: FeatureReadinessDistribution
    created_at: datetime
    production_ready_fraction: Optional[float] = Field(default=None)

    @validator("production_ready_fraction")
    @classmethod
    def _derive_production_ready_fraction(cls, value: Any, values: dict[str, Any]) -> Any:
        if value is None:
            return values["readiness_distribution"].derive_production_ready_fraction()
        return value


class FeatureListBriefInfoList(PaginationMixin):
    """
    Paginated list of feature brief info
    """

    data: List[FeatureListBriefInfo]

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
        feature_list_transform = DictTransform(
            rule={
                "__root__": DictProject(rule=["page", "page_size", "total"]),
                "data": DictProject(
                    rule=("data", ["version", "readiness_distribution", "created_at"])
                ),
            }
        )
        return FeatureListBriefInfoList(**feature_list_transform.transform(paginated_data))


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
