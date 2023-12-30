"""
OnlineStore API payload schema
"""
from typing import List, Optional

from bson.objectid import ObjectId
from pydantic import Field, StrictStr

from featurebyte.models.base import FeatureByteBaseModel, PydanticObjectId
from featurebyte.models.online_store import OnlineStoreDetails, OnlineStoreModel
from featurebyte.schema.common.base import BaseDocumentServiceUpdateSchema, PaginationMixin


class OnlineStoreCreate(FeatureByteBaseModel):
    """
    Online Store Creation Schema
    """

    id: Optional[PydanticObjectId] = Field(default_factory=ObjectId, alias="_id")
    name: StrictStr
    details: OnlineStoreDetails


class OnlineStoreUpdate(BaseDocumentServiceUpdateSchema):
    """
    Online Store Creation Schema
    """

    name: Optional[StrictStr]
    details: Optional[OnlineStoreDetails]


class OnlineStoreList(PaginationMixin):
    """
    Paginated list of OnlineStore
    """

    data: List[OnlineStoreModel]
