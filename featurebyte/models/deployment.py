"""
Deployment model
"""
from __future__ import annotations

from typing import List, Optional

import pymongo
from pydantic import BaseSettings, Field, StrictStr

from featurebyte.models.base import (
    FeatureByteCatalogBaseDocumentModel,
    PydanticObjectId,
    UniqueConstraintResolutionSignature,
    UniqueValuesConstraint,
)


class DeploymentModel(FeatureByteCatalogBaseDocumentModel):
    """Model for a deployment"""

    name: Optional[StrictStr]
    feature_list_id: PydanticObjectId
    enabled: bool
    context_id: Optional[PydanticObjectId] = Field(default=None)
    use_case_id: Optional[PydanticObjectId] = Field(default=None)

    class Settings(FeatureByteCatalogBaseDocumentModel.Settings):
        """
        MongoDB settings
        """

        collection_name: str = "deployment"
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
        ]

        indexes = FeatureByteCatalogBaseDocumentModel.Settings.indexes + [
            pymongo.operations.IndexModel("feature_list_id"),
            [
                ("name", pymongo.TEXT),
                ("description", pymongo.TEXT),
            ],
        ]


class FeastIntegrationSettings(BaseSettings):
    """
    Feast integration settings
    """

    FEATUREBYTE_FEAST_INTEGRATION_ENABLED: bool = False
