"""
FeatureStore API payload schema
"""
from typing import List, Optional

from beanie import PydanticObjectId
from bson.objectid import ObjectId
from pydantic import Field, StrictStr

from featurebyte.enum import SourceType
from featurebyte.models.base import FeatureByteBaseModel
from featurebyte.models.feature_store import DatabaseDetails, FeatureStoreModel
from featurebyte.routes.common.schema import BaseInfo, PaginationMixin


class FeatureStoreCreate(FeatureByteBaseModel):
    """
    Feature Store Creation Schema
    """

    id: Optional[PydanticObjectId] = Field(default_factory=ObjectId, alias="_id")
    name: StrictStr
    type: SourceType
    details: DatabaseDetails


class FeatureStoreList(PaginationMixin):
    """
    Paginated list of FeatureStore
    """

    data: List[FeatureStoreModel]


class FeatureStoreInfo(BaseInfo):
    """
    FeatureStore in schema
    """

    source: SourceType
    database_details: DatabaseDetails
