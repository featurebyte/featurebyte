"""
Target namespace module
"""

from typing import List, Optional

import pymongo
from pydantic import Field, field_validator

from featurebyte.common.validator import construct_sort_validator, duration_string_validator
from featurebyte.enum import DBVarType, TargetType
from featurebyte.models.base import PydanticObjectId
from featurebyte.models.feature_namespace import BaseFeatureNamespaceModel


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
