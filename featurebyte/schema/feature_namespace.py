"""
FeatureNamespace API pyaload schema
"""

from __future__ import annotations

from typing import List, Optional

from bson.objectid import ObjectId
from pydantic import Field

from featurebyte.enum import DBVarType
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

    default_version_mode: Optional[DefaultVersionMode]
    default_feature_id: Optional[PydanticObjectId]


class FeatureNamespaceServiceUpdate(FeatureNamespaceUpdate):
    """
    FeatureNamespace service update schema - used by server side only, not exposed to client
    """

    feature_ids: Optional[List[PydanticObjectId]]
    online_enabled_feature_ids: Optional[List[PydanticObjectId]]
    readiness: Optional[FeatureReadiness]
    default_feature_id: Optional[PydanticObjectId]
