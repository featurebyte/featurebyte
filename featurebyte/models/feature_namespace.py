"""
This module contains FeatureNamespace related models
"""

from __future__ import annotations

from typing import List, Optional

import pymongo
from pydantic import Field, field_validator

from featurebyte.common.doc_util import FBAutoDoc
from featurebyte.common.validator import construct_sort_validator
from featurebyte.enum import DBVarType, FeatureType, OrderedStrEnum, StrEnum
from featurebyte.models.base import (
    FeatureByteCatalogBaseDocumentModel,
    PydanticObjectId,
    UniqueConstraintResolutionSignature,
    UniqueValuesConstraint,
)


class FeatureReadiness(OrderedStrEnum):
    """Feature readiness"""

    DEPRECATED = "DEPRECATED"
    DRAFT = "DRAFT"
    PUBLIC_DRAFT = "PUBLIC_DRAFT"
    PRODUCTION_READY = "PRODUCTION_READY"


class DefaultVersionMode(StrEnum):
    """
    Default feature setting mode.
    """

    __fbautodoc__ = FBAutoDoc(proxy_class="featurebyte.DefaultVersionMode")

    AUTO = "AUTO", "Automatically select the version to use."
    MANUAL = "MANUAL", "Manually select the version to use."


class BaseFeatureNamespaceModel(FeatureByteCatalogBaseDocumentModel):
    """
    BaseFeatureNamespaceModel is the base class for FeatureNamespaceModel & TargetNamespaceModel.
    It contains all the attributes that are shared between FeatureNamespaceModel & TargetNamespaceModel.
    """

    default_version_mode: DefaultVersionMode = Field(default=DefaultVersionMode.AUTO, frozen=True)
    entity_ids: List[PydanticObjectId] = Field(frozen=True)

    class Settings(FeatureByteCatalogBaseDocumentModel.Settings):
        """
        MongoDB settings
        """

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
            pymongo.operations.IndexModel("dtype"),
            pymongo.operations.IndexModel(
                [
                    ("name", pymongo.TEXT),
                    ("description", pymongo.TEXT),
                ],
            ),
            pymongo.operations.IndexModel("entity_ids"),
        ]


class FeatureNamespaceModel(BaseFeatureNamespaceModel):
    """
    Feature set with the same feature name

    id: PydanticObjectId
        Feature namespace id
    name: str
        Feature name
    dtype: DBVarType
        Variable type of the feature
    feature_ids: List[PydanticObjectId]
        List of feature version id
    online_enabled_feature_ids: List[PydanticObjectId]
        List of online enabled feature version id
    readiness: FeatureReadiness
        Aggregated readiness across all feature versions of the same feature namespace
    created_at: datetime
        Datetime when the FeatureNamespace was first saved or published
    default_feature_id: PydanticObjectId
        Default feature version id
    default_version_mode: DefaultVersionMode
        Default feature version mode
    entity_ids: List[PydanticObjectId]
        Entity IDs used by the feature
    table_ids: List[PydanticObjectId]
        Table IDs used by the feature
    """

    dtype: DBVarType = Field(frozen=True, description="database variable type for the feature")
    readiness: FeatureReadiness = Field(frozen=True)
    feature_type: Optional[FeatureType] = Field(frozen=True, default=None)

    # list of IDs attached to this feature namespace or target namespace
    feature_ids: List[PydanticObjectId] = Field(frozen=True)
    default_feature_id: PydanticObjectId = Field(frozen=True)
    online_enabled_feature_ids: List[PydanticObjectId] = Field(frozen=True, default_factory=list)
    table_ids: List[PydanticObjectId] = Field(frozen=True)

    # pydantic validators
    _sort_ids_validator = field_validator("feature_ids", "entity_ids", "table_ids")(
        construct_sort_validator()
    )

    class Settings(BaseFeatureNamespaceModel.Settings):
        """
        MongoDB settings
        """

        collection_name: str = "feature_namespace"
        indexes = BaseFeatureNamespaceModel.Settings.indexes + [
            pymongo.operations.IndexModel("readiness"),
            pymongo.operations.IndexModel("feature_ids"),
            pymongo.operations.IndexModel("default_feature_id"),
            pymongo.operations.IndexModel("online_enabled_feature_ids"),
            pymongo.operations.IndexModel("table_ids"),
        ]
