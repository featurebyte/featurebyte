"""
FeatureNamespace API pyaload schema
"""

from __future__ import annotations

from typing import List, Optional

from bson import ObjectId
from pydantic import Field

from featurebyte.enum import DBVarType, FeatureType
from featurebyte.models.base import FeatureByteBaseModel, NameStr, PydanticObjectId
from featurebyte.models.feature_namespace import (
    DefaultVersionMode,
    FeatureNamespaceModel,
    FeatureReadiness,
)
from featurebyte.schema.common.base import BaseDocumentServiceUpdateSchema, PaginationMixin


class FeatureNamespaceCreate(FeatureByteBaseModel):
    """
    Feature Namespace Creation Schema
    """

    id: Optional[PydanticObjectId] = Field(default_factory=ObjectId, alias="_id")
    name: NameStr
    dtype: DBVarType
    feature_type: FeatureType = Field(default=None)
    feature_ids: List[PydanticObjectId] = Field(default_factory=list)
    readiness: FeatureReadiness
    default_feature_id: PydanticObjectId
    default_version_mode: DefaultVersionMode = Field(default=DefaultVersionMode.AUTO)
    entity_ids: List[PydanticObjectId]
    table_ids: List[PydanticObjectId]


class FeatureNamespaceModelResponse(FeatureNamespaceModel):
    """
    Extended FeatureNamespace model
    """

    primary_entity_ids: List[PydanticObjectId]
    primary_table_ids: List[PydanticObjectId]


class FeatureNamespaceList(PaginationMixin):
    """
    Paginated list of FeatureNamespace
    """

    data: List[FeatureNamespaceModelResponse]


class FeatureNamespaceUpdate(BaseDocumentServiceUpdateSchema, FeatureByteBaseModel):
    """
    FeatureNamespace update schema - exposed to client
    """

    default_version_mode: Optional[DefaultVersionMode] = Field(default=None)
    default_feature_id: Optional[PydanticObjectId] = Field(default=None)
    feature_type: Optional[FeatureType] = Field(default=None)


class FeatureNamespaceServiceUpdate(FeatureNamespaceUpdate):
    """
    FeatureNamespace service update schema - used by server side only, not exposed to client
    """

    feature_ids: Optional[List[PydanticObjectId]] = Field(default=None)
    online_enabled_feature_ids: Optional[List[PydanticObjectId]] = Field(default=None)
    readiness: Optional[FeatureReadiness] = Field(default=None)
    default_feature_id: Optional[PydanticObjectId] = Field(default=None)
