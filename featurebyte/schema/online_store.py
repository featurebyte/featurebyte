"""
OnlineStore API payload schema
"""

from typing import List, Optional

from datetime import datetime

from bson.objectid import ObjectId
from pydantic import Field, StrictStr, validator

from featurebyte.models.base import FeatureByteBaseModel, NameStr, PydanticObjectId
from featurebyte.models.online_store import OnlineStoreDetails
from featurebyte.schema.common.base import BaseDocumentServiceUpdateSchema, PaginationMixin


class OnlineStoreCreate(FeatureByteBaseModel):
    """
    Online Store Creation Schema
    """

    id: Optional[PydanticObjectId] = Field(default_factory=ObjectId, alias="_id")
    name: NameStr
    details: OnlineStoreDetails


class OnlineStoreUpdate(BaseDocumentServiceUpdateSchema):
    """
    Online Store Creation Schema
    """

    name: Optional[StrictStr]
    details: Optional[OnlineStoreDetails]


class OnlineStoreRead(FeatureByteBaseModel):
    """
    Online Store details
    """

    id: PydanticObjectId = Field(alias="_id")
    user_id: Optional[PydanticObjectId]
    name: StrictStr
    created_at: Optional[datetime]
    updated_at: Optional[datetime]
    description: Optional[StrictStr]
    details: OnlineStoreDetails

    @validator("details")
    @classmethod
    def hide_details_credentials(cls, value: OnlineStoreDetails) -> OnlineStoreDetails:
        """
        Hide credentials in the details field

        Parameters
        ----------
        value: OnlineStoreDetails
            Online store details

        Returns
        -------
        OnlineStoreDetails
        """
        value.hide_details_credentials()
        return value


class OnlineStoreList(PaginationMixin):
    """
    Paginated list of OnlineStore
    """

    data: List[OnlineStoreRead]
