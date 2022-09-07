"""
This module contains Feature list related models
"""
# pylint: disable=too-few-public-methods
from __future__ import annotations

from typing import Any, List, Optional

import functools

from beanie import PydanticObjectId
from bson.objectid import ObjectId
from pydantic import Field, StrictStr, validator
from typeguard import typechecked

from featurebyte.enum import DBVarType, OrderedStrEnum
from featurebyte.models.base import (
    FeatureByteBaseDocumentModel,
    FeatureByteBaseModel,
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


class FeatureTypeFeatureCount(FeatureByteBaseModel):
    """
    Feature count corresponding to the feature type within a feature list

    dtype: DBVarType
        Feature data type
    count: int
        Number of features with the specified data type
    """

    dtype: DBVarType
    count: int


class FeatureReadinessFeatureCount(FeatureByteBaseModel):
    """
    Feature count corresponding to the feature readiness within a feature list

    readiness: FeatureReadiness
        Feature readiness level
    count: int
        Number of features with the given readiness within a feature list
    """

    readiness: FeatureReadiness
    count: int


@functools.total_ordering
class FeatureReadinessDistribution(FeatureByteBaseModel):
    """
    Feature readiness distribution
    """

    __root__: List[FeatureReadinessFeatureCount]

    @staticmethod
    def _to_count_per_readiness_map(
        feature_readiness_dist: FeatureReadinessDistribution,
    ) -> dict[FeatureReadiness, int]:
        output = {}
        for feature_readiness in FeatureReadiness:
            output[feature_readiness] = 0

        for feature_readiness_count in feature_readiness_dist.__root__:
            output[feature_readiness_count.readiness] += feature_readiness_count.count
        return output

    @classmethod
    def _transform_and_check(
        cls, this_dist: FeatureReadinessDistribution, other_dist: FeatureReadinessDistribution
    ) -> tuple[dict[FeatureReadiness, int], dict[FeatureReadiness, int]]:
        this_dist_map = cls._to_count_per_readiness_map(this_dist)
        other_dist_map = cls._to_count_per_readiness_map(other_dist)
        if sum(this_dist_map.values()) != sum(other_dist_map.values()):
            raise ValueError(
                "Invalid comparison between two feature readiness distributions with different sums."
            )
        return this_dist_map, other_dist_map

    @typechecked
    def __eq__(self, other: FeatureReadinessDistribution) -> bool:  # type: ignore[override]
        this_dist_map, other_dist_map = self._transform_and_check(self, other)
        for feature_readiness in FeatureReadiness:
            if this_dist_map[feature_readiness] != other_dist_map[feature_readiness]:
                return False
        return True

    @typechecked
    def __lt__(self, other: FeatureReadinessDistribution) -> bool:
        this_dist_map, other_dist_map = self._transform_and_check(self, other)
        # feature readiness sorted from the worst readiness (deprecated) to the best readiness (production ready)
        # the one with the lower number of readiness should be preferred
        # this mean: dist_with_lower_bad_readiness > dist_with_higher_bad_readiness
        for feature_readiness in FeatureReadiness:
            compare_readiness = (
                this_dist_map[feature_readiness] == other_dist_map[feature_readiness]
            )
            if compare_readiness:
                continue
            return this_dist_map[feature_readiness] > other_dist_map[feature_readiness]
        return False

    def derive_readiness(self) -> FeatureReadiness:
        """
        Derive readiness based on feature readiness distribution

        Returns
        -------
        Aggregated featured readiness
        """
        return min(
            list(
                readiness_count.readiness
                for readiness_count in self.__root__
                if readiness_count.count
            )
            or [FeatureReadiness.DRAFT]
        )


class FeatureListNamespaceModel(FeatureByteBaseDocumentModel):
    """
    Feature list set with the same feature list name

    id: PydanticObjectId
        Feature namespace id
    name: str
        Feature name
    feature_list_ids: List[PydanticObjectId]
        List of feature list ids
    dtype_distribution: List[FeatureTypeFeatureCount]
        Feature type distribution
    readiness_distribution: FeatureReadinessDistribution
        Feature readiness distribution of the default feature list
    readiness: FeatureReadiness
        Aggregated readiness of the default feature list
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
    dtype_distribution: List[FeatureTypeFeatureCount] = Field(allow_mutation=False)
    readiness_distribution: FeatureReadinessDistribution = Field(allow_mutation=False)
    readiness: FeatureReadiness = Field(allow_mutation=False, default=FeatureReadiness.DRAFT)
    default_feature_list_id: PydanticObjectId = Field(allow_mutation=False)
    default_version_mode: DefaultVersionMode = Field(
        default=DefaultVersionMode.AUTO, allow_mutation=False
    )
    status: FeatureListStatus = Field(allow_mutation=False, default=FeatureListStatus.DRAFT)
    entity_ids: List[PydanticObjectId] = Field(allow_mutation=False)
    event_data_ids: List[PydanticObjectId] = Field(allow_mutation=False)

    @validator("readiness")
    @classmethod
    def _check_readiness(cls, value: FeatureReadiness, values: dict[str, Any]) -> FeatureReadiness:
        _ = value
        if isinstance(values["readiness_distribution"], list):
            readiness_dist = FeatureReadinessDistribution(__root__=values["readiness_distribution"])
        else:
            readiness_dist = values["readiness_distribution"]
        return readiness_dist.derive_readiness()

    class Settings:
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


class FeatureListModel(FeatureByteBaseDocumentModel):
    """
    Model for feature list entity

    id: PydanticObjectId
        FeatureList id of the object
    name: str
        Name of the feature list
    feature_ids: List[PydanticObjectId]
        List of feature IDs
    readiness_distribution: List[Dict[str, Any]]
        Feature readiness distribution of this feature list
    readiness: FeatureReadiness
        Aggregated readiness of this feature list
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
    readiness_distribution: FeatureReadinessDistribution = Field(
        allow_mutation=False, default_factory=list
    )
    readiness: Optional[FeatureReadiness] = Field(allow_mutation=False, default=None)
    version: Optional[FeatureListVersionIdentifier] = Field(allow_mutation=False)
    entity_ids: List[PydanticObjectId] = Field(allow_mutation=False, default_factory=list)
    event_data_ids: List[PydanticObjectId] = Field(allow_mutation=False, default_factory=list)
    feature_list_namespace_id: PydanticObjectId = Field(
        allow_mutation=False, default_factory=ObjectId
    )

    @validator("readiness")
    @classmethod
    def _check_readiness(cls, value: FeatureReadiness, values: dict[str, Any]) -> FeatureReadiness:
        _ = value
        if isinstance(values["readiness_distribution"], list):
            readiness_dist = FeatureReadinessDistribution(__root__=values["readiness_distribution"])
        else:
            readiness_dist = values["readiness_distribution"]
        return readiness_dist.derive_readiness()

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
                fields=("name", "version"),
                conflict_fields_signature={"name": ["name"], "version": ["version"]},
                resolution_signature=UniqueConstraintResolutionSignature.GET_BY_ID,
            ),
            UniqueValuesConstraint(
                fields=("feature_ids",),
                conflict_fields_signature={"feature_ids": ["feature_ids"]},
                resolution_signature=UniqueConstraintResolutionSignature.GET_NAME,
            ),
        ]
