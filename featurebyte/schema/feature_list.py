"""
FeatureList API payload schema
"""
from __future__ import annotations

from typing import List, Optional

from beanie import PydanticObjectId
from bson.objectid import ObjectId
from pydantic import Field, StrictStr

from featurebyte.models.base import FeatureByteBaseModel
from featurebyte.models.feature_list import FeatureListModel, FeatureListVersionIdentifier
from featurebyte.routes.common.schema import PaginationMixin


class FeatureListCreate(FeatureByteBaseModel):
    """
    FeatureList Creation schema
    """

    id: Optional[PydanticObjectId] = Field(default_factory=ObjectId, alias="_id")
    name: StrictStr
    feature_ids: List[PydanticObjectId] = Field(min_items=1)
    version: Optional[FeatureListVersionIdentifier]
    feature_namespace_id: Optional[PydanticObjectId] = Field(default_factory=ObjectId)


class FeatureListPaginatedList(PaginationMixin):
    """
    Paginated list of Entity
    """

    data: List[FeatureListModel]
