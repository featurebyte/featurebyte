"""
FeatureList API payload schema
"""
from __future__ import annotations

from typing import List, Optional, Tuple

from beanie import PydanticObjectId
from pydantic import Field, StrictStr

from featurebyte.models.base import FeatureByteBaseModel
from featurebyte.models.feature import (
    FeatureListModel,
    FeatureListStatus,
    FeatureListVersionIdentifier,
    FeatureReadiness,
    FeatureVersionIdentifier,
)
from featurebyte.routes.common.schema import PaginationMixin


class FeatureListCreate(FeatureByteBaseModel):
    """
    FeatureList Creation schema
    """

    id: PydanticObjectId = Field(alias="_id")
    name: StrictStr
    description: Optional[str]
    feature_ids: List[PydanticObjectId] = Field(min_items=1)
    readiness: Optional[FeatureReadiness]
    status: Optional[FeatureListStatus]
    version: Optional[FeatureListVersionIdentifier]


class FeatureListPaginatedList(PaginationMixin):
    """
    Paginated list of Entity
    """

    data: List[FeatureListModel]
