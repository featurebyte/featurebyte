"""
FeatureList API payload schema
"""
from __future__ import annotations

from typing import Any, Dict, List, Optional

from datetime import datetime

from bson.objectid import ObjectId
from pydantic import Field, StrictStr, root_validator, validator

from featurebyte.common.model_util import convert_version_string_to_dict
from featurebyte.models.base import FeatureByteBaseModel, PydanticObjectId, VersionIdentifier
from featurebyte.models.feature_list import (
    FeatureListModel,
    FeatureListNewVersionMode,
    FeatureListStatus,
    FeatureReadinessDistribution,
    FeatureTypeFeatureCount,
)
from featurebyte.query_graph.graph import QueryGraph
from featurebyte.query_graph.node import Node
from featurebyte.routes.common.schema import PaginationMixin
from featurebyte.schema.common.base import BaseDocumentServiceUpdateSchema
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


class FeatureVersionInfo(FeatureByteBaseModel):
    """
    Feature version info
    """

    name: str
    version: VersionIdentifier

    @validator("version", pre=True)
    @classmethod
    def _validate_version(cls, value: Any) -> Any:
        # convert version string into version dictionary
        if isinstance(value, str):
            return convert_version_string_to_dict(value)
        return value


class FeatureListNewVersionCreate(FeatureByteBaseModel):
    """
    New version creation schema based on existing feature list
    """

    source_feature_list_id: PydanticObjectId
    mode: FeatureListNewVersionMode
    features: Optional[List[FeatureVersionInfo]]


class FeatureListPaginatedList(PaginationMixin):
    """
    Paginated list of Entity
    """

    data: List[FeatureListModel]


class FeatureListUpdate(FeatureByteBaseModel):
    """
    FeatureList update schema
    """

    make_production_ready: Optional[bool]
    deployed: Optional[bool]


class FeatureListServiceUpdate(BaseDocumentServiceUpdateSchema, FeatureListUpdate):
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


class FeatureCluster(FeatureByteBaseModel):
    """
    Schema for a group of features from the same feature store
    """

    feature_store_name: StrictStr
    graph: QueryGraph
    node_names: List[StrictStr]

    @property
    def nodes(self) -> List[Node]:
        """
        Get feature nodes

        Returns
        -------
        List[Node]
        """
        return [self.graph.get_node_by_name(name) for name in self.node_names]


class FeatureListPreview(FeatureByteBaseModel):
    """
    FeatureList preview schema
    """

    feature_clusters: List[FeatureCluster]
    point_in_time_and_serving_name: Dict[str, Any]


class FeatureListGetHistoricalFeatures(FeatureByteBaseModel):
    """
    FeatureList get historical features schema
    """

    feature_clusters: List[FeatureCluster]
    serving_names_mapping: Optional[Dict[str, str]]
