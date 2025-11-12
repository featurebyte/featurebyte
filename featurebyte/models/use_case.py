"""
Use Case model
"""

from typing import List, Literal, Optional

import pymongo

from featurebyte.enum import StrEnum
from featurebyte.models.base import (
    FeatureByteCatalogBaseDocumentModel,
    PydanticObjectId,
    UniqueConstraintResolutionSignature,
    UniqueValuesConstraint,
)


class UseCaseType(StrEnum):
    """UseCaseType represents the type of use case"""

    PREDICTIVE = "predictive"
    DESCRIPTIVE = "descriptive"


class BaseUseCaseModel(FeatureByteCatalogBaseDocumentModel):
    """BaseUseCaseModel represents the base model for a use case within a feature store"""

    use_case_type: UseCaseType = UseCaseType.PREDICTIVE  # backward compatibility
    context_id: PydanticObjectId
    default_eda_table_id: Optional[PydanticObjectId] = None
    archived: bool = False

    # TODO: remove default_preview_table_id in future release
    default_preview_table_id: Optional[PydanticObjectId] = None

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
            pymongo.operations.IndexModel("use_case_type"),
            pymongo.operations.IndexModel("context_id"),
            [
                ("name", pymongo.TEXT),
                ("description", pymongo.TEXT),
            ],
        ]


class DescriptiveUseCaseModel(BaseUseCaseModel):
    """DescriptiveUseCaseModel represents a descriptive use case within a feature store"""

    use_case_type: Literal[UseCaseType.DESCRIPTIVE] = UseCaseType.DESCRIPTIVE


class UseCaseModel(BaseUseCaseModel):
    """PredictiveUseCaseModel represents a predictive use case within a feature store"""

    use_case_type: Literal[UseCaseType.PREDICTIVE] = UseCaseType.PREDICTIVE
    target_id: Optional[PydanticObjectId] = None
    target_namespace_id: PydanticObjectId
