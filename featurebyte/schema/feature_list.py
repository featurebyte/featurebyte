"""
FeatureList API payload schema
"""
from __future__ import annotations

from typing import List, Optional

from beanie import PydanticObjectId
from bson.objectid import ObjectId
from pydantic import Field, StrictStr

from featurebyte.models.base import FeatureByteBaseModel
from featurebyte.models.feature import FeatureReadiness
from featurebyte.models.feature_list import (
    FeatureListModel,
    FeatureListStatus,
    FeatureListVersionIdentifier,
)
from featurebyte.routes.common.schema import PaginationMixin


class FeatureListCreate(FeatureByteBaseModel):
    """
    FeatureList Creation schema
    """

    id: Optional[PydanticObjectId] = Field(default_factory=ObjectId, alias="_id")
    name: StrictStr
    feature_ids: List[PydanticObjectId]
    readiness: Optional[FeatureReadiness]
    status: Optional[FeatureListStatus]
    version: Optional[FeatureListVersionIdentifier]
    entity_ids: List[PydanticObjectId]
    event_data_ids: List[PydanticObjectId]


class FeatureListPaginatedList(PaginationMixin):
    """
    Paginated list of Entity
    """

    data: List[FeatureListModel]
