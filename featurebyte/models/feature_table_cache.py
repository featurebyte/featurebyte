"""
This module contains Feature Table Cache related models
"""
from typing import List, Optional

import pymongo
from pydantic import Field

from featurebyte.models.base import (
    FeatureByteBaseModel,
    FeatureByteCatalogBaseDocumentModel,
    PydanticObjectId,
    UniqueConstraintResolutionSignature,
    UniqueValuesConstraint,
)


class CachedFeatureDefinition(FeatureByteBaseModel):
    """
    Definition of the feature cached in Feature Table Cache
    """

    feature_id: Optional[PydanticObjectId] = Field(default=None)
    definition_hash: str


class FeatureTableCacheModel(FeatureByteCatalogBaseDocumentModel):
    """
    Model for feature table cache

    id: PydanticObjectId
        FeatureTableCache id of the object
    observation_table_id: PydanticObjectId
        Observation Table Id
    table_name: str
        Name of the Feature Table Cache in DWH (SQL table name).
    feature_definition_hashes: List[str]
        List of feature definition hashes which are stored in this feature table cache
    """

    observation_table_id: PydanticObjectId
    table_name: str
    feature_definitions: List[CachedFeatureDefinition]

    class Settings(FeatureByteCatalogBaseDocumentModel.Settings):
        """
        MongoDB settings
        """

        collection_name: str = "feature_table_cache"
        unique_constraints: List[UniqueValuesConstraint] = [
            UniqueValuesConstraint(
                fields=("_id",),
                conflict_fields_signature={"id": ["_id"]},
                resolution_signature=UniqueConstraintResolutionSignature.GET_BY_ID,
            ),
            UniqueValuesConstraint(
                fields=("table_name",),
                conflict_fields_signature={"table_name": ["table_name"]},
                resolution_signature=UniqueConstraintResolutionSignature.GET_BY_ID,
            ),
        ]
        indexes = FeatureByteCatalogBaseDocumentModel.Settings.indexes + [
            pymongo.operations.IndexModel("observation_table_id"),
        ]
        auditable = False
