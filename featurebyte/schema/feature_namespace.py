"""
FeatureNamespace API pyaload schema
"""
from __future__ import annotations

from typing import List, Optional

from bson.objectid import ObjectId
from pydantic import Field, StrictStr

from featurebyte.enum import DBVarType
from featurebyte.models.base import FeatureByteBaseModel, PydanticObjectId
from featurebyte.models.feature import DefaultVersionMode, FeatureNamespaceModel, FeatureReadiness
from featurebyte.schema.common.base import (
    BaseDocumentServiceUpdateSchema,
    BaseInfo,
    PaginationMixin,
)
from featurebyte.schema.data import DataBriefInfoList
from featurebyte.schema.entity import EntityBriefInfoList


class FeatureNamespaceCreate(FeatureByteBaseModel):
    """
    Feature Namespace Creation Schema
    """

    id: Optional[PydanticObjectId] = Field(default_factory=ObjectId, alias="_id")
    name: StrictStr
    dtype: DBVarType
    feature_ids: List[PydanticObjectId] = Field(default_factory=list)
    readiness: FeatureReadiness
    default_feature_id: PydanticObjectId
    default_version_mode: DefaultVersionMode = Field(default=DefaultVersionMode.AUTO)
    entity_ids: List[PydanticObjectId]
    tabular_data_ids: List[PydanticObjectId]


class FeatureNamespaceList(PaginationMixin):
    """
    Paginated list of FeatureNamespace
    """

    data: List[FeatureNamespaceModel]


class FeatureNamespaceUpdate(BaseDocumentServiceUpdateSchema, FeatureByteBaseModel):
    """
    FeatureNamespace update schema
    """

    default_version_mode: Optional[DefaultVersionMode]


class FeatureNamespaceServiceUpdate(FeatureNamespaceUpdate):
    """
    FeatureNamespace service update schema
    """

    feature_ids: Optional[List[PydanticObjectId]]
    online_enabled_feature_ids: Optional[List[PydanticObjectId]]
    readiness: Optional[FeatureReadiness]
    default_feature_id: Optional[PydanticObjectId]


class NamespaceInfo(BaseInfo):
    """
    Namespace info schema
    """

    entities: EntityBriefInfoList
    tabular_data: DataBriefInfoList
    default_version_mode: DefaultVersionMode
    version_count: int


class FeatureNamespaceInfo(NamespaceInfo):
    """
    FeatureNamespace info schema
    """

    dtype: DBVarType
    default_feature_id: PydanticObjectId
