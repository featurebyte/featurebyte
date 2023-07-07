"""
Target namespace module
"""
from typing import List

import pymongo
from pydantic import Field, validator

from featurebyte.common.validator import construct_sort_validator, duration_string_validator
from featurebyte.models.base import PydanticObjectId
from featurebyte.models.feature_namespace import BaseFeatureTargetNamespaceModel


class TargetNamespaceModel(BaseFeatureTargetNamespaceModel):
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

    horizon: str

    # list of IDs attached to this feature namespace or target namespace
    target_ids: List[PydanticObjectId] = Field(allow_mutation=False)
    default_target_id: PydanticObjectId = Field(allow_mutation=False)

    # pydantic validators
    _sort_feature_ids_validator = validator("target_ids", "entity_ids", allow_reuse=True)(
        construct_sort_validator()
    )
    _duration_validator = validator("horizon", pre=True, allow_reuse=True)(
        duration_string_validator
    )

    class Settings(BaseFeatureTargetNamespaceModel.Settings):
        """
        MongoDB settings
        """

        collection_name: str = "target_namespace"
        indexes = BaseFeatureTargetNamespaceModel.Settings.indexes + [
            pymongo.operations.IndexModel("target_ids"),
            pymongo.operations.IndexModel("default_target_id"),
        ]
