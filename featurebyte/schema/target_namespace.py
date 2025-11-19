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
from featurebyte.models.target_namespace import (
    PositiveLabelCandidatesItem,
    PositiveLabelType,
    TargetNamespaceModel,
)
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
    positive_label: Optional[PositiveLabelType] = Field(default=None)

    @model_validator(mode="after")
    def _validate_settings(self) -> "TargetNamespaceCreate":
        if self.target_type:
            validate_target_type(target_type=self.target_type, dtype=self.dtype)

        if self.positive_label:
            if self.target_type != TargetType.CLASSIFICATION:
                raise ValueError("Positive label can only be set for classification target type")

            if self.dtype not in {DBVarType.VARCHAR, DBVarType.CHAR, DBVarType.INT, DBVarType.BOOL}:
                raise ValueError(
                    "Positive label can only be set for target with dtype VARCHAR, CHAR, INT, or BOOL"
                )

            if self.dtype == DBVarType.BOOL and not isinstance(self.positive_label, bool):
                raise ValueError("Positive label must be a boolean value for BOOL dtype")

            if self.dtype in {DBVarType.VARCHAR, DBVarType.CHAR} and not isinstance(
                self.positive_label, str
            ):
                raise ValueError("Positive label must be a string value for VARCHAR or CHAR dtype")

            if self.dtype == DBVarType.INT and not isinstance(self.positive_label, int):
                raise ValueError("Positive label must be an integer value for INT dtype")
        return self


class PositiveLabelUpdate(FeatureByteBaseModel):
    """
    Positive label update schema - used by server side only, not exposed to client
    """

    observation_table_id: Optional[PydanticObjectId]
    value: PositiveLabelType


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
