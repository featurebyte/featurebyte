"""
This module contains Feature list namespace related models
"""

from __future__ import annotations

from typing import Any, List

import pymongo
from pydantic import BaseModel, Field, field_validator, model_validator

from featurebyte.common.doc_util import FBAutoDoc
from featurebyte.common.validator import construct_sort_validator
from featurebyte.enum import OrderedStrEnum
from featurebyte.models.base import (
    FeatureByteCatalogBaseDocumentModel,
    PydanticObjectId,
    UniqueConstraintResolutionSignature,
    UniqueValuesConstraint,
)


class FeatureListStatus(OrderedStrEnum):
    """FeatureList status"""

    __fbautodoc__ = FBAutoDoc(proxy_class="featurebyte.FeatureListStatus")

    DEPRECATED = "DEPRECATED"
    DRAFT = "DRAFT"
    PUBLIC_DRAFT = "PUBLIC_DRAFT"
    TEMPLATE = "TEMPLATE"
    DEPLOYED = "DEPLOYED"


class FeatureListNamespaceModel(FeatureByteCatalogBaseDocumentModel):
    """
    Feature list set with the same feature list name

    id: PydanticObjectId
        Feature namespace id
    name: str
        Feature name
    feature_list_ids: List[PydanticObjectId]
        List of feature list ids
    deployed_feature_list_ids: List[PydanticObjectId]
        List of deployed feature list ids
    feature_namespace_ids: List[PydanticObjectId]
        List of feature namespace ids
    default_feature_list_id: PydanticObjectId
        Default feature list id
    status: FeatureListStatus
        Feature list status
    """

    feature_list_ids: List[PydanticObjectId] = Field(frozen=True)
    feature_namespace_ids: List[PydanticObjectId] = Field(frozen=True)
    deployed_feature_list_ids: List[PydanticObjectId] = Field(frozen=True, default_factory=list)
    default_feature_list_id: PydanticObjectId = Field(frozen=True)
    status: FeatureListStatus = Field(frozen=True, default=FeatureListStatus.DRAFT)

    # pydantic validators
    _sort_ids_validator = field_validator(
        "feature_list_ids", "feature_namespace_ids", "deployed_feature_list_ids"
    )(construct_sort_validator())

    @model_validator(mode="before")
    @classmethod
    def _derive_feature_related_attributes(cls, values: Any) -> Any:
        if isinstance(values, BaseModel):
            values = values.model_dump(by_alias=True)

        # "features" is not an attribute to the FeatureList model, when it appears in the input to
        # constructor, it is intended to be used to derive other feature-related attributes
        if "features" in values:
            features = values["features"]
            values["feature_namespace_ids"] = [feature.feature_namespace_id for feature in features]
        return values

    class Settings(FeatureByteCatalogBaseDocumentModel.Settings):
        """
        MongoDB settings
        """

        collection_name: str = "feature_list_namespace"
        unique_constraints: List[UniqueValuesConstraint] = [
            UniqueValuesConstraint(
                fields=("_id",),
                conflict_fields_signature={"id": ["_id"]},
                resolution_signature=None,
            ),
            UniqueValuesConstraint(
                fields=("name",),
                conflict_fields_signature={"name": ["name"]},
                resolution_signature=UniqueConstraintResolutionSignature.RENAME,
            ),
        ]
        indexes = FeatureByteCatalogBaseDocumentModel.Settings.indexes + [
            pymongo.operations.IndexModel("feature_list_ids"),
            pymongo.operations.IndexModel("feature_namespace_ids"),
            pymongo.operations.IndexModel("deployed_feature_list_ids"),
            pymongo.operations.IndexModel("default_feature_list_id"),
            pymongo.operations.IndexModel("status"),
            [
                ("name", pymongo.TEXT),
                ("description", pymongo.TEXT),
            ],
        ]
