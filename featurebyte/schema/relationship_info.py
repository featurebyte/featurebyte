"""
Relationship Info payload schema
"""

from datetime import datetime
from typing import List, Optional

from bson import ObjectId
from pydantic import Field

from featurebyte.models.base import FeatureByteBaseModel, NameStr, PydanticObjectId
from featurebyte.models.relationship import RelationshipInfoModel, RelationshipType
from featurebyte.schema.common.base import BaseDocumentServiceUpdateSchema, PaginationMixin


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


class RelationshipInfoUpdate(BaseDocumentServiceUpdateSchema):
    """
    RelationshipInfo update payload schema
    """

    enabled: Optional[bool] = Field(default=None)
    relationship_type: Optional[RelationshipType] = Field(default=None)


class RelationshipInfoInfo(FeatureByteBaseModel):
    """
    RelationshipInfo info
    """

    id: PydanticObjectId
    relationship_type: RelationshipType
    entity_name: str
    related_entity_name: str
    table_name: str
    data_type: str
    created_at: datetime
    updated_at: Optional[datetime] = Field(default=None)
    updated_by: str
