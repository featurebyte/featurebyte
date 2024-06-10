"""
FeatureList API payload schema
"""

from __future__ import annotations

from typing import Any, ClassVar, Dict, List, Optional, Union

from bson.objectid import ObjectId
from pydantic import Field, root_validator, validator

from featurebyte.common.doc_util import FBAutoDoc
from featurebyte.common.validator import version_validator
from featurebyte.config import FEATURE_PREVIEW_ROW_LIMIT, ONLINE_FEATURE_REQUEST_ROW_LIMIT
from featurebyte.enum import ConflictResolution
from featurebyte.models.base import (
    FeatureByteBaseModel,
    NameStr,
    PydanticObjectId,
    VersionIdentifier,
)
from featurebyte.models.feature_list import (
    FeatureCluster,
    FeatureListModel,
    FeatureReadinessDistribution,
)
from featurebyte.query_graph.node.validator import construct_unique_name_validator
from featurebyte.schema.common.base import BaseDocumentServiceUpdateSchema, PaginationMixin
from featurebyte.schema.common.feature_or_target import ComputeRequest
from featurebyte.schema.feature import (
    MAX_BATCH_FEATURE_ITEM_COUNT,
    BatchFeatureCreate,
    BatchFeatureCreatePayload,
    BatchFeatureItem,
)
from featurebyte.schema.worker.task.feature_list_create import FeatureParameters


class FeatureListCreate(FeatureByteBaseModel):
    """
    Feature List Creation schema
    """

    id: Optional[PydanticObjectId] = Field(default_factory=ObjectId, alias="_id")
    name: NameStr
    feature_ids: List[PydanticObjectId] = Field(min_items=1)


class FeatureListCreateJob(FeatureByteBaseModel):
    """
    Feature List Job Creation schema
    """

    id: Optional[PydanticObjectId] = Field(default_factory=ObjectId, alias="_id")
    name: NameStr
    features: Union[List[FeatureParameters], List[PydanticObjectId]] = Field(min_items=1)
    features_conflict_resolution: ConflictResolution


class FeatureListServiceCreate(FeatureListCreate):
    """
    Feature List Service Creation schema
    """

    feature_list_namespace_id: Optional[PydanticObjectId] = Field(default_factory=ObjectId)


class FeatureListCreateWithBatchFeatureCreationMixin(FeatureByteBaseModel):
    """Feature List Creation with Batch Feature Creation mixin"""

    id: PydanticObjectId = Field(default_factory=ObjectId, alias="_id")
    name: NameStr
    conflict_resolution: ConflictResolution
    features: List[BatchFeatureItem]
    skip_batch_feature_creation: bool = Field(default=False)

    @root_validator(pre=True)
    @classmethod
    def _validate_payload(cls, values: Dict[str, Any]) -> Dict[str, Any]:
        if (
            not values.get("skip_batch_feature_creation", False)
            and len(values.get("features", [])) > MAX_BATCH_FEATURE_ITEM_COUNT
        ):
            raise ValueError(
                f"features count must be less than or equal to {MAX_BATCH_FEATURE_ITEM_COUNT} "
                "if skip_batch_feature_creation is not set to True."
            )
        return values


class FeatureListCreateWithBatchFeatureCreationPayload(
    BatchFeatureCreatePayload, FeatureListCreateWithBatchFeatureCreationMixin
):
    """
    Feature List Creation with Batch Feature Creation schema (used by the client to prepare the payload)
    """

    features: List[BatchFeatureItem]


class FeatureListCreateWithBatchFeatureCreation(
    BatchFeatureCreate, FeatureListCreateWithBatchFeatureCreationMixin
):
    """
    Feature List Creation with Batch Feature Creation schema (used by the featurebyte server side)
    """

    features: List[BatchFeatureItem]


class FeatureVersionInfo(FeatureByteBaseModel):
    """
    Feature version info.

    Examples
    --------
    >>> new_feature_list = feature_list.create_new_version(  # doctest: +SKIP
    ...   features=[fb.FeatureVersionInfo(name="InvoiceCount_60days", version=new_feature.version)]
    ... )
    """

    # class variables
    __fbautodoc__: ClassVar[FBAutoDoc] = FBAutoDoc(proxy_class="featurebyte.FeatureVersionInfo")

    # instance variables
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
    allow_unchanged_feature_list_version: bool = Field(default=False)

    # pydantic validators
    _validate_unique_feat_name = validator("features", allow_reuse=True)(
        construct_unique_name_validator(field="name")
    )


class FeatureListModelResponse(FeatureListModel):
    """
    Extended FeatureListModel with additional fields
    """

    is_default: bool


class FeatureListPaginatedItem(FeatureListModelResponse):
    """
    Paginated item of FeatureList
    """

    # exclude this field from the response
    internal_feature_clusters: Optional[List[Any]] = Field(
        allow_mutation=False, alias="feature_clusters", exclude=True
    )


class FeatureListPaginatedList(PaginationMixin):
    """
    Paginated list of Entity
    """

    data: List[FeatureListPaginatedItem]


class FeatureListUpdate(FeatureByteBaseModel):
    """
    FeatureList update schema
    """

    make_production_ready: Optional[bool]
    ignore_guardrails: Optional[bool]


class FeatureListServiceUpdate(BaseDocumentServiceUpdateSchema, FeatureListUpdate):
    """
    FeatureList service update schema
    """

    deployed: Optional[bool]
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


class FeatureListGetHistoricalFeatures(ComputeRequest):
    """
    FeatureList get historical features schema
    """

    feature_clusters: Optional[List[FeatureCluster]]
    feature_list_id: Optional[PydanticObjectId]

    @root_validator
    @classmethod
    def _validate_feature_clusters(cls, values: Dict[str, Any]) -> Dict[str, Any]:
        feature_clusters = values.get("feature_clusters", None)
        feature_list_id = values.get("feature_list_id", None)
        if not feature_clusters and not feature_list_id:
            raise ValueError("Either feature_clusters or feature_list_id must be set")

        return values


class PreviewObservationSet(FeatureByteBaseModel):
    """
    Preview observation set schema
    """

    point_in_time_and_serving_name_list: Optional[List[Dict[str, Any]]] = Field(
        min_items=1, max_items=FEATURE_PREVIEW_ROW_LIMIT
    )
    observation_table_id: Optional[PydanticObjectId]

    @root_validator
    @classmethod
    def _validate_observation_set(cls, values: Dict[str, Any]) -> Dict[str, Any]:
        point_in_time_and_serving_name_list = values.get(
            "point_in_time_and_serving_name_list", None
        )
        observation_table_id = values.get("observation_table_id", None)
        if not point_in_time_and_serving_name_list and not observation_table_id:
            raise ValueError(
                "Either point_in_time_and_serving_name_list or observation_table_id must be set"
            )
        if observation_table_id is not None and point_in_time_and_serving_name_list is not None:
            raise ValueError(
                "Only one of point_in_time_and_serving_name_list and observation_table_id can be set"
            )

        return values


class FeatureListPreview(FeatureListGetHistoricalFeatures, PreviewObservationSet):
    """
    FeatureList preview schema
    """


class OnlineFeaturesRequestPayload(FeatureByteBaseModel):
    """
    FeatureList get online features schema
    """

    entity_serving_names: List[Dict[str, Any]] = Field(
        min_items=1, max_items=ONLINE_FEATURE_REQUEST_ROW_LIMIT
    )


class SampleEntityServingNames(FeatureByteBaseModel):
    """
    Schema for sample entity serving names
    """

    entity_serving_names: List[Dict[str, str]]
