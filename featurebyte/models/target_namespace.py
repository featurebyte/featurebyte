"""
Target namespace module
"""
from typing import List

import pymongo
from pydantic import Field, validator

from featurebyte.common.validator import construct_sort_validator
from featurebyte.models.base import (
    FeatureByteCatalogBaseDocumentModel,
    PydanticObjectId,
    UniqueConstraintResolutionSignature,
    UniqueValuesConstraint,
)
from featurebyte.models.feature_namespace import DefaultVersionMode


class TargetNamespaceModel(FeatureByteCatalogBaseDocumentModel):
    """
    Target namespace model
    """

    target_ids: List[PydanticObjectId] = Field(allow_mutation=False)
    default_target_id: PydanticObjectId = Field(allow_mutation=False)
    default_version_mode: DefaultVersionMode = Field(
        default=DefaultVersionMode.AUTO, allow_mutation=False
    )

    # pydantic validators
    _sort_feature_ids_validator = validator("target_ids", allow_reuse=True)(
        construct_sort_validator()
    )

    class Settings(FeatureByteCatalogBaseDocumentModel.Settings):
        """
        MongoDB settings
        """

        collection_name: str = "target_namespace"
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
            pymongo.operations.IndexModel("default_target_id"),
        ]
