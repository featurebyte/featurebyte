"""
Relationship Info payload schema
"""

from datetime import datetime
from typing import List, Optional

from bson import ObjectId
from pydantic import Field, field_validator

from featurebyte.models.base import FeatureByteBaseModel, NameStr, PydanticObjectId
from featurebyte.models.relationship import (
    RelationshipInfoModel,
    RelationshipStatus,
    RelationshipType,
)
from featurebyte.schema.common.base import PaginationMixin


class RelationshipInfoCreate(FeatureByteBaseModel):
    """
    Relationship Info Creation Schema
    """

    id: Optional[PydanticObjectId] = Field(default_factory=ObjectId, alias="_id")
    name: NameStr
    relationship_type: RelationshipType
    entity_id: PydanticObjectId
    related_entity_id: PydanticObjectId
    relation_table_id: PydanticObjectId
    entity_column_name: Optional[str] = Field(default=None)
    related_entity_column_name: Optional[str] = Field(default=None)
    enabled: bool
    updated_by: Optional[PydanticObjectId] = Field(default=None)


class RelationshipInfoList(PaginationMixin):
    """
    Paginated list of RelationshipInfo
    """

    data: List[RelationshipInfoModel]


class RelationshipInfoUpdate(FeatureByteBaseModel):
    """
    RelationshipInfo update payload schema
    """

    enabled: Optional[bool] = Field(default=None)
    relationship_type: Optional[RelationshipType] = Field(default=None)
    relationship_status: Optional[RelationshipStatus] = Field(default=None)

    @field_validator("relationship_type")
    @classmethod
    def validate_user_settable(cls, v: Optional[RelationshipType]) -> Optional[RelationshipType]:
        if v is not None and v not in RelationshipType.user_settable():
            raise ValueError(f"relationship_type cannot be updated to {v}")
        return v

    @field_validator("relationship_status")
    @classmethod
    def validate_relationship_status(
        cls, v: Optional[RelationshipStatus]
    ) -> Optional[RelationshipStatus]:
        if v is not None and v not in RelationshipStatus.user_settable():
            raise ValueError(f"relationship_status cannot be updated to {v}")
        return v


class RelationshipInfoInfo(FeatureByteBaseModel):
    """
    RelationshipInfo info
    """

    id: PydanticObjectId
    relationship_type: RelationshipType
    relationship_status: RelationshipStatus
    entity_name: str
    related_entity_name: str
    table_name: str
    data_type: str
    created_at: datetime
    updated_at: Optional[datetime] = Field(default=None)
    updated_by: str
