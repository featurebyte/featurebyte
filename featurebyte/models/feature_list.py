"""
This module contains Feature list related models
"""
# pylint: disable=too-few-public-methods
from __future__ import annotations

from typing import List, Optional

from beanie import PydanticObjectId
from pydantic import Field, StrictStr

from featurebyte.enum import OrderedStrEnum
from featurebyte.models.base import (
    FeatureByteBaseDocumentModel,
    UniqueConstraintResolutionSignature,
    UniqueValuesConstraint,
)
from featurebyte.models.feature import DefaultVersionMode, FeatureReadiness

FeatureListVersionIdentifier = StrictStr


class FeatureListStatus(OrderedStrEnum):
    """FeatureList status"""

    DEPRECATED = "DEPRECATED"
    DRAFT = "DRAFT"
    PUBLIC_DRAFT = "PUBLIC_DRAFT"
    PUBLISHED = "PUBLISHED"


class FeatureListModel(FeatureByteBaseDocumentModel):
    """
    Model for feature list entity

    id: PydanticObjectId
        FeatureList id of the object
    name: str
        Name of the feature list
    feature_ids: List[PydanticObjectId]
        List of feature IDs
    readiness: FeatureReadiness
        Aggregated readiness of the features/feature classes
    status: FeatureListStatus
        FeatureList status
    version: FeatureListVersionIdentifier
        Feature list version
    entity_ids: List[PydanticObjectId]
        Entity IDs used in the feature list
    event_data_ids: List[PydanticObjectId]
        EventData IDs used in the feature list
    created_at: Optional[datetime]
        Datetime when the FeatureList was first saved or published
    feature_list_namespace_id: PydanticObjectId
        Feature list namespace id of the object
    """

    feature_ids: List[PydanticObjectId] = Field(default_factory=list)
    readiness: Optional[FeatureReadiness] = Field(allow_mutation=False)
    status: Optional[FeatureListStatus] = Field(allow_mutation=False)
    version: Optional[FeatureListVersionIdentifier] = Field(allow_mutation=False)
    entity_ids: List[PydanticObjectId] = Field(default_factory=list)
    event_data_ids: List[PydanticObjectId] = Field(default_factory=list)
    #    feature_list_namespace_id: PydanticObjectId = Field(
    #        allow_mutation=False, default_factory=ObjectId
    #    )

    class Settings:
        """
        MongoDB settings
        """

        collection_name: str = "feature_list"
        unique_constraints: List[UniqueValuesConstraint] = [
            UniqueValuesConstraint(
                fields=("_id",),
                conflict_fields_signature={"id": ["_id"]},
                resolution_signature=UniqueConstraintResolutionSignature.GET_NAME,
            ),
            UniqueValuesConstraint(
                fields=("name",),
                conflict_fields_signature={"name": ["name"]},
                resolution_signature=UniqueConstraintResolutionSignature.GET_NAME,
            ),
            UniqueValuesConstraint(
                fields=("feature_ids",),
                conflict_fields_signature={"feature_ids": ["feature_ids"]},
                resolution_signature=UniqueConstraintResolutionSignature.GET_NAME,
            ),
        ]


class FeatureListNamespaceModel(FeatureByteBaseDocumentModel):
    """
    Feature list set with the same feature list name

    id: PydanticObjectId
        Feature namespace id
    name: str
        Feature name
    feature_list_ids: List[PydanticObjectId]
        List of feature list ids
    default_feature_list_id: PydanticObjectId
        Default feature list id
    default_version_mode: DefaultVersionMode
        Default feature version mode
    entity_ids: List[PydanticObjectId]
        Entity IDs used in the feature list
    event_data_ids: List[PydanticObjectId]
        EventData IDs used in the feature list
    """

    feature_list_ids: List[PydanticObjectId] = Field(allow_mutation=False)
    default_feature_list_id: PydanticObjectId = Field(allow_mutation=False)
    default_version_mode: DefaultVersionMode = Field(
        default=DefaultVersionMode.AUTO, allow_mutation=False
    )
    entity_ids: List[PydanticObjectId] = Field(allow_mutation=False)
    event_data_ids: List[PydanticObjectId] = Field(allow_mutation=False)

    class Settings:
        """
        MongoDB settings
        """

        collection_name: str = "feature_list_namespace"
        unique_constraints: List[UniqueValuesConstraint] = [
            UniqueValuesConstraint(
                fields=("_id",),
                conflict_fields_signature={"id": ["_id"]},
                resolution_signature=UniqueConstraintResolutionSignature.GET_BY_ID,
            ),
            UniqueValuesConstraint(
                fields=("name", "version"),
                conflict_fields_signature={"name": ["name"], "version": ["version"]},
                resolution_signature=UniqueConstraintResolutionSignature.GET_BY_ID,
            ),
        ]
