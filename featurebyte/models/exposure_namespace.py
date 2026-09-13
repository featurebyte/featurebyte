"""
Exposure namespace module
"""

from typing import List, Optional

import pymongo
from pydantic import Field, field_validator

from featurebyte.common.validator import construct_sort_validator, duration_string_validator
from featurebyte.enum import DBVarType
from featurebyte.models.base import PydanticObjectId
from featurebyte.models.feature_namespace import BaseFeatureNamespaceModel


class ExposureNamespaceModel(BaseFeatureNamespaceModel):
    """
    Exposure set with the same exposure name

    id: PydanticObjectId
        Exposure namespace id
    name: str
        Exposure name
    dtype: DBVarType
        Variable type of the exposure
    exposure_ids: List[PydanticObjectId]
        List of exposure version id
    created_at: datetime
        Datetime when the ExposureNamespace was first saved or published
    default_exposure_id: PydanticObjectId
        Default exposure version id
    default_version_mode: DefaultVersionMode
        Default exposure version mode
    entity_ids: List[PydanticObjectId]
        Entity IDs used by the exposure
    """

    dtype: Optional[DBVarType] = Field(
        default=None, frozen=True, description="database variable type for the exposure"
    )
    window: Optional[str] = Field(default=None)

    # list of IDs attached to this exposure namespace
    exposure_ids: List[PydanticObjectId] = Field(frozen=True)
    default_exposure_id: Optional[PydanticObjectId] = Field(default=None, frozen=True)

    # pydantic validators
    _sort_ids_validator = field_validator("exposure_ids", "entity_ids")(construct_sort_validator())
    _duration_validator = field_validator("window", mode="before")(duration_string_validator)

    class Settings(BaseFeatureNamespaceModel.Settings):
        """
        MongoDB settings
        """

        collection_name: str = "exposure_namespace"
        indexes = BaseFeatureNamespaceModel.Settings.indexes + [
            pymongo.operations.IndexModel("exposure_ids"),
            pymongo.operations.IndexModel("default_exposure_id"),
        ]
