"""
FeatureStore API payload schema
"""
from typing import List, Optional

from beanie import PydanticObjectId
from pydantic import Field, StrictStr

from featurebyte.enum import SourceType
from featurebyte.models.base import FeatureByteBaseModel
from featurebyte.models.feature_store import DatabaseDetails, FeatureStoreModel
from featurebyte.routes.common.schema import PaginationMixin


class FeatureStore(FeatureStoreModel):
    """
    Feature Store Document Model
    """

    user_id: Optional[PydanticObjectId]


class FeatureStoreCreate(FeatureByteBaseModel):
    """
    Feature Store Creation Schema
    """

    id: PydanticObjectId = Field(alias="_id")
    name: StrictStr
    type: SourceType
    details: DatabaseDetails


class FeatureStoreList(PaginationMixin):
    """
    Paginated list of FeatureStore
    """

    data: List[FeatureStore]
