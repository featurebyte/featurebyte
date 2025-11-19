"""
Target namespace module
"""

from typing import List, Optional, Union

import pymongo
from pydantic import Field, field_validator, model_validator

from featurebyte.common.validator import construct_sort_validator, duration_string_validator
from featurebyte.enum import DBVarType, TargetType
from featurebyte.exception import TargetValidationError
from featurebyte.models.base import FeatureByteBaseModel, PydanticObjectId
from featurebyte.models.feature_namespace import BaseFeatureNamespaceModel

PositiveLabelType = Union[str, int, bool]


class PositiveLabelCandidatesItem(FeatureByteBaseModel):
    """
    Positive label candidates for a target namespace
    """

    observation_table_id: PydanticObjectId
    positive_label_candidates: Union[List[str], List[int], List[bool]]


class TargetNamespaceModel(BaseFeatureNamespaceModel):
    """
    Target set with the same target name

    id: PydanticObjectId
        Target namespace id
    name: str
        Target name
    dtype: DBVarType
        Variable type of the target
    target_ids: List[PydanticObjectId]
        List of target version id
    created_at: datetime
        Datetime when the TargetNamespace was first saved or published
    default_target_id: PydanticObjectId
        Default target version id
    default_version_mode: DefaultVersionMode
        Default target version mode
    entity_ids: List[PydanticObjectId]
        Entity IDs used by the target
    table_ids: List[PydanticObjectId]
        Table IDs used by the target
    """

    dtype: Optional[DBVarType] = Field(
        default=None, frozen=True, description="database variable type for the target"
    )
    window: Optional[str] = Field(default=None)
    target_type: Optional[TargetType] = Field(default=None)

    # list of IDs attached to this feature namespace or target namespace
    target_ids: List[PydanticObjectId] = Field(frozen=True)
    default_target_id: Optional[PydanticObjectId] = Field(default=None, frozen=True)

    # positive label candidates for the target namespace
    positive_label_candidates: List[PositiveLabelCandidatesItem] = Field(default_factory=list)
    positive_label: Optional[PositiveLabelType] = Field(default=None)

    # pydantic validators
    _sort_ids_validator = field_validator("target_ids", "entity_ids")(construct_sort_validator())
    _duration_validator = field_validator("window", mode="before")(duration_string_validator)

    class Settings(BaseFeatureNamespaceModel.Settings):
        """
        MongoDB settings
        """

        collection_name: str = "target_namespace"
        indexes = BaseFeatureNamespaceModel.Settings.indexes + [
            pymongo.operations.IndexModel("target_ids"),
            pymongo.operations.IndexModel("default_target_id"),
        ]

    @model_validator(mode="after")
    def _validate_positive_label_target_type(self) -> "TargetNamespaceModel":
        """
        Validate that positive label can only be set for classification type target namespace.
        """
        if self.positive_label is not None and self.target_type != TargetType.CLASSIFICATION:
            raise TargetValidationError(
                f"Positive label can only be set for target namespace of type "
                f"{TargetType.CLASSIFICATION}, but got {self.target_type}."
            )
        return self
