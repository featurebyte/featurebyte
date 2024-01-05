"""
This module contains Feature Table Cache related models
"""
from typing import List

import pymongo

from featurebyte.models.base import (
    FeatureByteCatalogBaseDocumentModel,
    PydanticObjectId,
    UniqueConstraintResolutionSignature,
    UniqueValuesConstraint,
)


class FeatureTableCacheModel(FeatureByteCatalogBaseDocumentModel):
    """
    Model for feature table cache

    id: PydanticObjectId
        FeatureTableCache id of the object
    observation_table_id: PydanticObjectId
        Observation Table Id
    name: str
        Name of the Feature Table Cache. Also used as name of the table in DWH
    features: List[str]
        List of feature names which are stored in this feature table cache
    """

    observation_table_id: PydanticObjectId
    name: str
    features: List[str]

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
                fields=("name",),
                conflict_fields_signature={"name": ["name"]},
                resolution_signature=UniqueConstraintResolutionSignature.GET_BY_ID,
            ),
        ]
        indexes = FeatureByteCatalogBaseDocumentModel.Settings.indexes + [
            pymongo.operations.IndexModel("observation_table_id"),
        ]
        auditable = False
