"""
FeatureNamespace API pyaload schema
"""

from __future__ import annotations

from bson import ObjectId
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

    id: PydanticObjectId | None = Field(default_factory=ObjectId, alias="_id")
    name: NameStr
    dtype: DBVarType
    feature_ids: list[PydanticObjectId] = Field(default_factory=list)
    readiness: FeatureReadiness
    default_feature_id: PydanticObjectId
    default_version_mode: DefaultVersionMode = Field(default=DefaultVersionMode.AUTO)
    entity_ids: list[PydanticObjectId]
    table_ids: list[PydanticObjectId]


class FeatureNamespaceModelResponse(FeatureNamespaceModel):
    """
    Extended FeatureNamespace model
    """

    primary_entity_ids: list[PydanticObjectId]
    primary_table_ids: list[PydanticObjectId]


class FeatureNamespaceList(PaginationMixin):
    """
    Paginated list of FeatureNamespace
    """

    data: list[FeatureNamespaceModelResponse]


class FeatureNamespaceUpdate(BaseDocumentServiceUpdateSchema, FeatureByteBaseModel):
    """
    FeatureNamespace update schema - exposed to client
    """

    default_version_mode: DefaultVersionMode | None = Field(default=None)
    default_feature_id: PydanticObjectId | None = Field(default=None)


class FeatureNamespaceServiceUpdate(FeatureNamespaceUpdate):
    """
    FeatureNamespace service update schema - used by server side only, not exposed to client
    """

    feature_ids: list[PydanticObjectId] | None = Field(default=None)
    online_enabled_feature_ids: list[PydanticObjectId] | None = Field(default=None)
    readiness: FeatureReadiness | None = Field(default=None)
    default_feature_id: PydanticObjectId | None = Field(default=None)
