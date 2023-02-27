"""
Relationship Info payload schema
"""
from typing import List, Optional

from datetime import datetime

from bson import ObjectId
from pydantic import Field

from featurebyte.models.base import FeatureByteBaseModel, PydanticObjectId
from featurebyte.models.relationship import RelationshipInfo
from featurebyte.schema.common.base import PaginationMixin


class RelationshipInfoCreate(FeatureByteBaseModel):
    """
    Relationship Info Creation Schema
    """

    id: Optional[PydanticObjectId] = Field(default_factory=ObjectId, alias="_id")
    relationship_type: str
    child_id: PydanticObjectId
    parent_id: PydanticObjectId
    child_data_source_id: PydanticObjectId
    is_enabled: bool
    updated_by: PydanticObjectId


class RelationshipInfoList(PaginationMixin):
    """
    Paginated list of RelationshipInfo
    """

    data: List[RelationshipInfo]


class RelationshipInfoUpdate(FeatureByteBaseModel):
    """
    RelationshipInfo update payload schema
    """

    is_enabled: bool


class RelationshipInfoInfo(FeatureByteBaseModel):
    """
    RelationshipInfo info
    """

    relationship_type: str
    child_name: str
    parent_name: str
    data_source_name: str
    created_at: datetime
    updated_at: Optional[datetime]
    updated_by: str
