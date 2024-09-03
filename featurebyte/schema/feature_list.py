"""
FeatureList API payload schema
"""

from __future__ import annotations

from typing import Annotated, Any, ClassVar

import numpy as np
from bson import ObjectId
from pydantic import (
    BaseModel,
    ConfigDict,
    Discriminator,
    Field,
    Tag,
    field_serializer,
    field_validator,
    model_validator,
)

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
from featurebyte.schema.constant import MAX_BATCH_FEATURE_ITEM_COUNT
from featurebyte.schema.feature import (
    BatchFeatureCreate,
    BatchFeatureCreatePayload,
    BatchFeatureItem,
)
from featurebyte.schema.worker.task.feature_list_create import (
    FeatureParameters,
    feature_params_discriminator,
)


class FeatureListCreate(FeatureByteBaseModel):
    """
    Feature List Creation schema
    """

    id: PydanticObjectId | None = Field(default_factory=ObjectId, alias="_id")
    name: NameStr
    feature_ids: list[PydanticObjectId] = Field(min_length=1)


class FeatureListCreateJob(FeatureByteBaseModel):
    """
    Feature List Job Creation schema
    """

    id: PydanticObjectId | None = Field(default_factory=ObjectId, alias="_id")
    name: NameStr
    features: (
        Annotated[list[FeatureParameters], Tag("feature_params")]
        | Annotated[list[PydanticObjectId], Tag("feature_ids")]
    ) = Field(discriminator=Discriminator(feature_params_discriminator), min_length=1)
    features_conflict_resolution: ConflictResolution


class FeatureListServiceCreate(FeatureListCreate):
    """
    Feature List Service Creation schema
    """

    feature_list_namespace_id: PydanticObjectId | None = Field(default_factory=ObjectId)


class FeatureListCreateWithBatchFeatureCreationMixin(FeatureByteBaseModel):
    """Feature List Creation with Batch Feature Creation mixin"""

    id: PydanticObjectId = Field(default_factory=ObjectId, alias="_id")
    name: NameStr
    conflict_resolution: ConflictResolution
    features: list[BatchFeatureItem]
    skip_batch_feature_creation: bool = Field(default=False)

    @model_validator(mode="before")
    @classmethod
    def _validate_payload(cls, values: Any) -> Any:
        if isinstance(values, BaseModel):
            values = values.model_dump(by_alias=True)

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

    features: list[BatchFeatureItem]


class FeatureListCreateWithBatchFeatureCreation(
    BatchFeatureCreate, FeatureListCreateWithBatchFeatureCreationMixin
):
    """
    Feature List Creation with Batch Feature Creation schema (used by the featurebyte server side)
    """

    features: list[BatchFeatureItem]


class FeatureVersionInfo(FeatureByteBaseModel):
    """
    Feature version info.

    Examples
    --------
    >>> new_feature_list = feature_list.create_new_version(  # doctest: +SKIP
    ...     features=[
    ...         fb.FeatureVersionInfo(name="InvoiceCount_60days", version=new_feature.version)
    ...     ]
    ... )
    """

    # class variables
    __fbautodoc__: ClassVar[FBAutoDoc] = FBAutoDoc(proxy_class="featurebyte.FeatureVersionInfo")

    # instance variables
    name: str = Field(description="Name of feature namespace.")
    version: VersionIdentifier = Field(description="Feature version.")

    # pydantic validators
    _version_validator = field_validator("version", mode="before")(version_validator)


class FeatureListNewVersionCreate(FeatureByteBaseModel):
    """
    New version creation schema based on existing feature list
    """

    source_feature_list_id: PydanticObjectId
    features: list[FeatureVersionInfo]
    allow_unchanged_feature_list_version: bool = Field(default=False)

    # pydantic validators
    _validate_unique_feat_name = field_validator("features")(
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
    internal_feature_clusters: list[Any] | None = Field(
        alias="feature_clusters", default=None, exclude=True
    )


class FeatureListPaginatedList(PaginationMixin):
    """
    Paginated list of Entity
    """

    data: list[FeatureListPaginatedItem]


class FeatureListUpdate(FeatureByteBaseModel):
    """
    FeatureList update schema
    """

    make_production_ready: bool | None = Field(default=None)
    ignore_guardrails: bool | None = Field(default=None)


class FeatureListServiceUpdate(BaseDocumentServiceUpdateSchema, FeatureListUpdate):
    """
    FeatureList service update schema
    """

    deployed: bool | None = Field(default=None)
    online_enabled_feature_ids: list[PydanticObjectId] | None = Field(default=None)
    readiness_distribution: FeatureReadinessDistribution | None = Field(default=None)


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

    feature_clusters: list[FeatureCluster]


class FeatureListGetHistoricalFeatures(ComputeRequest):
    """
    FeatureList get historical features schema
    """

    feature_clusters: list[FeatureCluster] | None = Field(default=None)
    feature_list_id: PydanticObjectId | None = Field(default=None)

    @model_validator(mode="after")
    def _validate_feature_clusters(self) -> FeatureListGetHistoricalFeatures:
        if not self.feature_clusters and not self.feature_list_id:
            raise ValueError("Either feature_clusters or feature_list_id must be set")
        return self


class PreviewObservationSet(FeatureByteBaseModel):
    """
    Preview observation set schema
    """

    point_in_time_and_serving_name_list: list[dict[str, Any]] | None = Field(
        min_length=1, max_length=FEATURE_PREVIEW_ROW_LIMIT, default=None
    )
    observation_table_id: PydanticObjectId | None = Field(default=None)

    @field_serializer("point_in_time_and_serving_name_list", when_used="json")
    def _serialize_point_in_time_and_serving_name_list(
        self, value: list[dict[str, Any]] | None
    ) -> Any:
        if isinstance(value, list):
            output = []
            for item in value:
                if isinstance(item, dict):
                    item = {
                        key: value.tolist() if isinstance(value, np.ndarray) else value
                        for key, value in item.items()
                    }
                output.append(item)
            return output
        return value

    @model_validator(mode="after")
    def _validate_observation_set(self) -> PreviewObservationSet:
        if not self.point_in_time_and_serving_name_list and not self.observation_table_id:
            raise ValueError(
                "Either point_in_time_and_serving_name_list or observation_table_id must be set"
            )

        if (
            self.observation_table_id is not None
            and self.point_in_time_and_serving_name_list is not None
        ):
            raise ValueError(
                "Only one of point_in_time_and_serving_name_list and observation_table_id can be set"
            )
        return self


class FeatureListPreview(FeatureListGetHistoricalFeatures, PreviewObservationSet):
    """
    FeatureList preview schema
    """


class OnlineFeaturesRequestPayload(FeatureByteBaseModel):
    """
    FeatureList get online features schema
    """

    entity_serving_names: list[dict[str, Any]] = Field(
        min_length=1, max_length=ONLINE_FEATURE_REQUEST_ROW_LIMIT
    )


class SampleEntityServingNames(FeatureByteBaseModel):
    """
    Schema for sample entity serving names
    """

    entity_serving_names: list[dict[str, str]]

    # model configuration
    model_config = ConfigDict(coerce_numbers_to_str=True)
