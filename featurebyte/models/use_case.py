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
    target_id: PydanticObjectId
        The target id of TargetModel
    observation_table_ids:  List[PydanticObjectId]
        The observation table ids of ObservationTableModel
    default_preview_table_id: PydanticObjectId
        The default preview table from observation_table_ids
    default_eda_table_id: PydanticObjectId
        The default eda table id of from observation_table_ids
    """

    context_id: PydanticObjectId
    target_id: PydanticObjectId
    observation_table_ids: List[PydanticObjectId] = Field(default=[])
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
            [
                ("name", pymongo.TEXT),
                ("description", pymongo.TEXT),
            ]
        ]
