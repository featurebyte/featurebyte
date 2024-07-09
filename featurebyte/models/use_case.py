"""
Use Case model
"""

from typing import List, Optional

import pymongo
from pydantic import Field

from featurebyte.models.base import (
    FeatureByteCatalogBaseDocumentModel,
    PydanticObjectId,
    UniqueConstraintResolutionSignature,
    UniqueValuesConstraint,
)


class UseCaseModel(FeatureByteCatalogBaseDocumentModel):
    """
    UseCaseModel represents a use case within a feature store

    context_id: PydanticObjectId
        The context id of ContextModel
    target_id: Optional[PydanticObjectId]
        The target id of TargetModel
    target_namespace_id: Optional[PydanticObjectId]
        The target namespace id of TargetModel
    default_preview_table_id: PydanticObjectId
        The default preview observation table
    default_eda_table_id: PydanticObjectId
        The default eda observation table
    """

    context_id: PydanticObjectId
    target_id: Optional[PydanticObjectId] = Field(default=None)
    target_namespace_id: PydanticObjectId
    default_preview_table_id: Optional[PydanticObjectId] = Field(default=None)
    default_eda_table_id: Optional[PydanticObjectId] = Field(default=None)

    class Settings(FeatureByteCatalogBaseDocumentModel.Settings):
        """
        MongoDB settings
        """

        collection_name: str = "use_case"
        unique_constraints: List[UniqueValuesConstraint] = [
            UniqueValuesConstraint(
                fields=("_id",),
                conflict_fields_signature={"id": ["_id"]},
                resolution_signature=UniqueConstraintResolutionSignature.GET_BY_ID,
            ),
            UniqueValuesConstraint(
                fields=("name",),
                conflict_fields_signature={"name": ["name"]},
                resolution_signature=UniqueConstraintResolutionSignature.GET_NAME,
            ),
        ]

        indexes = FeatureByteCatalogBaseDocumentModel.Settings.indexes + [
            pymongo.operations.IndexModel("context_id"),
            [
                ("name", pymongo.TEXT),
                ("description", pymongo.TEXT),
            ],
        ]
