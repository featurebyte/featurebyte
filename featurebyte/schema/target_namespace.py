"""
Target namespace schema
"""
from typing import List, Optional

from bson import ObjectId
from pydantic import Field, StrictStr

from featurebyte import DefaultVersionMode
from featurebyte.models.base import FeatureByteBaseModel, PydanticObjectId
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
    name: StrictStr
    target_ids: List[PydanticObjectId] = Field(default_factory=list)
    default_target_id: PydanticObjectId
    default_version_mode: DefaultVersionMode = Field(default=DefaultVersionMode.AUTO)


class TargetNamespaceUpdate(BaseDocumentServiceUpdateSchema):
    """
    TargetNamespace update schema
    """

    default_version_mode: Optional[DefaultVersionMode]
    default_target_id: Optional[PydanticObjectId]


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
