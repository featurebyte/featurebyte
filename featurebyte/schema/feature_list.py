"""
FeatureList API payload schema
"""
from __future__ import annotations

from typing import Any, Dict, List, Optional

from bson.objectid import ObjectId
from pydantic import Field, StrictStr, validator

from featurebyte.common.model_util import convert_version_string_to_dict
from featurebyte.models.base import FeatureByteBaseModel, PydanticObjectId, VersionIdentifier
from featurebyte.models.feature_list import (
    FeatureCluster,
    FeatureListModel,
    FeatureListNewVersionMode,
    FeatureReadinessDistribution,
)
from featurebyte.schema.common.base import BaseDocumentServiceUpdateSchema, PaginationMixin


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


class FeatureListSQL(FeatureByteBaseModel):
    """
    FeatureList SQL schema
    """

    feature_clusters: List[FeatureCluster]


class FeatureListPreview(FeatureListSQL):
    """
    FeatureList preview schema
    """

    point_in_time_and_serving_name: Dict[str, Any]


class FeatureListGetHistoricalFeatures(FeatureByteBaseModel):
    """
    FeatureList get historical features schema
    """

    feature_clusters: List[FeatureCluster]
    serving_names_mapping: Optional[Dict[str, str]]


class FeatureListGetOnlineFeatures(FeatureByteBaseModel):
    """
    FeatureList get online features schema
    """

    entity_serving_names: List[Dict[str, Any]] = Field(min_items=1, max_items=50)


class OnlineFeaturesResponseModel(FeatureByteBaseModel):
    """
    Response model for online features
    """

    features: List[Dict[str, Any]]
