"""
Target namespace schema
"""

from typing import List, Optional

from bson import ObjectId
from pydantic import Field

from featurebyte.enum import DBVarType
from featurebyte.models.base import FeatureByteBaseModel, NameStr, PydanticObjectId
from featurebyte.models.feature_namespace import DefaultVersionMode
from featurebyte.models.target_namespace import TargetNamespaceModel
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
    default_target_id: Optional[PydanticObjectId]
    default_version_mode: DefaultVersionMode = Field(default=DefaultVersionMode.AUTO)
    entity_ids: List[PydanticObjectId] = Field(default_factory=list)
    window: Optional[str]


class TargetNamespaceUpdate(BaseDocumentServiceUpdateSchema):
    """
    TargetNamespace update schema - exposed to client
    """

    default_version_mode: Optional[DefaultVersionMode]
    default_target_id: Optional[PydanticObjectId]
    window: Optional[str]


class TargetNamespaceServiceUpdate(TargetNamespaceUpdate):
    """
    TargetNamespaceService update schema - used by server side only, not exposed to client
    """

    target_ids: Optional[List[PydanticObjectId]]


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
    default_feature_id: PydanticObjectId
