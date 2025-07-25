"""
Target namespace schema
"""

from typing import List, Optional, Union

from bson import ObjectId
from pydantic import Field, model_validator

from featurebyte.common.validator import validate_target_type
from featurebyte.enum import DBVarType, TargetType
from featurebyte.models.base import FeatureByteBaseModel, NameStr, PydanticObjectId
from featurebyte.models.feature_namespace import DefaultVersionMode
from featurebyte.models.target_namespace import PositiveLabelCandidatesItem, TargetNamespaceModel
from featurebyte.schema.common.base import (
    BaseDocumentServiceUpdateSchema,
    BaseInfo,
    PaginationMixin,
)


class TargetNamespaceCreate(FeatureByteBaseModel):
    """
    Target Namespace Creation Schema
    """

    id: Optional[PydanticObjectId] = Field(default_factory=ObjectId, alias="_id")
    name: NameStr
    dtype: DBVarType
    target_ids: List[PydanticObjectId] = Field(default_factory=list)
    default_target_id: Optional[PydanticObjectId] = Field(default=None)
    default_version_mode: DefaultVersionMode = Field(default=DefaultVersionMode.AUTO)
    entity_ids: List[PydanticObjectId] = Field(default_factory=list)
    window: Optional[str] = Field(default=None)
    target_type: Optional[TargetType] = Field(default=None)

    @model_validator(mode="after")
    def _validate_settings(self) -> "TargetNamespaceCreate":
        if self.target_type:
            validate_target_type(target_type=self.target_type, dtype=self.dtype)
        return self


class PositiveLabelUpdate(FeatureByteBaseModel):
    """
    Positive label update schema - used by server side only, not exposed to client
    """

    observation_table_id: PydanticObjectId
    value: Union[str, float, bool]


class TargetNamespaceUpdate(BaseDocumentServiceUpdateSchema):
    """
    TargetNamespace update schema - exposed to client
    """

    window: Optional[str] = Field(default=None)
    target_type: Optional[TargetType] = Field(default=None)
    positive_label: Optional[PositiveLabelUpdate] = Field(default=None)


class TargetNamespaceClassificationMetadataUpdate(FeatureByteBaseModel):
    """
    TargetNamespace classification metadata update schema - used by server side only, not exposed to client
    """

    observation_table_id: PydanticObjectId


class TargetNamespaceServiceUpdate(TargetNamespaceUpdate):
    """
    TargetNamespaceService update schema - used by server side only, not exposed to client
    """

    default_version_mode: Optional[DefaultVersionMode] = Field(default=None)
    target_ids: Optional[List[PydanticObjectId]] = Field(default=None)
    default_target_id: Optional[PydanticObjectId] = Field(default=None)
    positive_label_candidates: Optional[List[PositiveLabelCandidatesItem]] = Field(default=None)
    positive_label: Optional[Union[str, int, bool]] = Field(default=None)  # type: ignore


class TargetNamespaceList(PaginationMixin):
    """
    Paginated list of TargetNamespace
    """

    data: List[TargetNamespaceModel]


class TargetNamespaceInfo(BaseInfo):
    """
    TargetNamespace info schema
    """

    name: str
    default_version_mode: DefaultVersionMode
    default_target_id: Optional[PydanticObjectId]
    target_type: Optional[TargetType]
