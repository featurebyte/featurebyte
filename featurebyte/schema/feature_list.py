"""
FeatureList API payload schema
"""
from __future__ import annotations

from typing import Any, Dict, List, Optional

from bson.objectid import ObjectId
from pydantic import Field, StrictStr, validator

from featurebyte.common.doc_util import FBAutoDoc
from featurebyte.common.validator import version_validator
from featurebyte.enum import ConflictResolution
from featurebyte.models.base import FeatureByteBaseModel, PydanticObjectId, VersionIdentifier
from featurebyte.models.feature_list import (
    FeatureCluster,
    FeatureListModel,
    FeatureReadinessDistribution,
)
from featurebyte.query_graph.node.validator import construct_unique_name_validator
from featurebyte.schema.common.base import BaseDocumentServiceUpdateSchema, PaginationMixin
from featurebyte.schema.common.feature_or_target import ComputeRequest
from featurebyte.schema.feature import BatchFeatureCreate, BatchFeatureCreatePayload


class FeatureListCreate(FeatureByteBaseModel):
    """
    Feature List Creation schema
    """

    id: Optional[PydanticObjectId] = Field(default_factory=ObjectId, alias="_id")
    name: StrictStr
    feature_ids: List[PydanticObjectId] = Field(min_items=1)


class FeatureListServiceCreate(FeatureListCreate):
    """
    Feature List Service Creation schema
    """

    feature_list_namespace_id: Optional[PydanticObjectId] = Field(default_factory=ObjectId)


class FeatureListCreateWithBatchFeatureCreationMixin(FeatureByteBaseModel):
    """Feature List Creation with Batch Feature Creation mixin"""

    id: PydanticObjectId = Field(default_factory=ObjectId, alias="_id")
    name: StrictStr
    conflict_resolution: ConflictResolution


class FeatureListCreateWithBatchFeatureCreationPayload(
    BatchFeatureCreatePayload, FeatureListCreateWithBatchFeatureCreationMixin
):
    """
    Feature List Creation with Batch Feature Creation schema (used by the client to prepare the payload)
    """


class FeatureListCreateWithBatchFeatureCreation(
    BatchFeatureCreate, FeatureListCreateWithBatchFeatureCreationMixin
):
    """
    Feature List Creation with Batch Feature Creation schema (used by the featurebyte server side)
    """


class FeatureVersionInfo(FeatureByteBaseModel):
    """
    Feature version info.

    Examples
    --------
    >>> new_feature_list = feature_list.create_new_version(  # doctest: +SKIP
    ...   features=[fb.FeatureVersionInfo(name="InvoiceCount_60days", version=new_feature.version)]
    ... )
    """

    __fbautodoc__ = FBAutoDoc(proxy_class="featurebyte.FeatureVersionInfo")

    name: str = Field(description="Name of feature namespace.")
    version: VersionIdentifier = Field(description="Feature version.")

    # pydantic validators
    _version_validator = validator("version", pre=True, allow_reuse=True)(version_validator)


class FeatureListNewVersionCreate(FeatureByteBaseModel):
    """
    New version creation schema based on existing feature list
    """

    source_feature_list_id: PydanticObjectId
    features: List[FeatureVersionInfo]

    # pydantic validators
    _validate_unique_feat_name = validator("features", allow_reuse=True)(
        construct_unique_name_validator(field="name")
    )


class FeatureListModelResponse(FeatureListModel):
    """
    Extended FeatureListModel with additional fields
    """

    is_default: bool


class FeatureListPaginatedList(PaginationMixin):
    """
    Paginated list of Entity
    """

    data: List[FeatureListModelResponse]


class FeatureListUpdate(FeatureByteBaseModel):
    """
    FeatureList update schema
    """

    make_production_ready: Optional[bool]
    deployed: Optional[bool]
    ignore_guardrails: Optional[bool]


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

    point_in_time_and_serving_name_list: List[Dict[str, Any]] = Field(min_items=1, max_items=50)


class FeatureListGetHistoricalFeatures(ComputeRequest):
    """
    FeatureList get historical features schema
    """

    feature_clusters: List[FeatureCluster]
    feature_list_id: Optional[PydanticObjectId]


class OnlineFeaturesRequestPayload(FeatureByteBaseModel):
    """
    FeatureList get online features schema
    """

    entity_serving_names: List[Dict[str, Any]] = Field(min_items=1, max_items=50)
