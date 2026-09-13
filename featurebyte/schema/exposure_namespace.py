"""
Exposure namespace schema
"""

from typing import List, Optional

from bson import ObjectId
from pydantic import Field

from featurebyte.enum import DBVarType
from featurebyte.models.base import FeatureByteBaseModel, NameStr, PydanticObjectId
from featurebyte.models.exposure_namespace import ExposureNamespaceModel
from featurebyte.models.feature_namespace import DefaultVersionMode
from featurebyte.schema.common.base import (
    BaseDocumentServiceUpdateSchema,
    BaseInfo,
    PaginationMixin,
)


class ExposureNamespaceCreate(FeatureByteBaseModel):
    """
    Exposure Namespace Creation Schema
    """

    id: Optional[PydanticObjectId] = Field(default_factory=ObjectId, alias="_id")
    name: NameStr
    dtype: DBVarType
    exposure_ids: List[PydanticObjectId] = Field(default_factory=list)
    default_exposure_id: Optional[PydanticObjectId] = Field(default=None)
    default_version_mode: DefaultVersionMode = Field(default=DefaultVersionMode.AUTO)
    entity_ids: List[PydanticObjectId] = Field(default_factory=list)
    window: Optional[str] = Field(default=None)


class ExposureNamespaceUpdate(BaseDocumentServiceUpdateSchema):
    """
    ExposureNamespace update schema - exposed to client
    """

    window: Optional[str] = Field(default=None)


class ExposureNamespaceServiceUpdate(ExposureNamespaceUpdate):
    """
    ExposureNamespaceService update schema - used by server side only, not exposed to client
    """

    default_version_mode: Optional[DefaultVersionMode] = Field(default=None)
    exposure_ids: Optional[List[PydanticObjectId]] = Field(default=None)
    default_exposure_id: Optional[PydanticObjectId] = Field(default=None)


class ExposureNamespaceList(PaginationMixin):
    """
    Paginated list of ExposureNamespace
    """

    data: List[ExposureNamespaceModel]


class ExposureNamespaceInfo(BaseInfo):
    """
    ExposureNamespace info schema
    """

    name: str
    default_version_mode: DefaultVersionMode
    default_exposure_id: Optional[PydanticObjectId]
