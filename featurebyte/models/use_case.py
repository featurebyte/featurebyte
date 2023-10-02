"""
Use Case model
"""
from typing import Any, Dict, List, Optional

import pymongo
from pydantic import Field, root_validator

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
    target_id: Optional[PydanticObjectId]
    target_namespace_id: Optional[PydanticObjectId]
    default_preview_table_id: Optional[PydanticObjectId] = Field(default=None)
    default_eda_table_id: Optional[PydanticObjectId] = Field(default=None)

    @root_validator(pre=True)
    @classmethod
    def _validate_target(cls, values: Dict[str, Any]) -> Dict[str, Any]:
        target_id = values.get("target_id", None)
        target_namespace_id = values.get("target_namespace_id", None)
        if not target_id and not target_namespace_id:
            raise ValueError("Either target_id or target_namespace_id must be specified.")
        return values

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
            [
                ("name", pymongo.TEXT),
                ("description", pymongo.TEXT),
            ]
        ]
