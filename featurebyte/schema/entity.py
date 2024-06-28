"""
Entity API payload schema
"""

from __future__ import annotations

from typing import List, Optional

from bson import ObjectId
from pydantic import Field

from featurebyte.models.base import (
    FeatureByteBaseModel,
    NameStr,
    PydanticObjectId,
    UniqueConstraintResolutionSignature,
    UniqueValuesConstraint,
)
from featurebyte.models.entity import EntityModel, ParentEntity
from featurebyte.schema.common.base import BaseDocumentServiceUpdateSchema, PaginationMixin


class EntityCreate(FeatureByteBaseModel):
    """
    Entity creation schema
    """

    id: Optional[PydanticObjectId] = Field(default_factory=ObjectId, alias="_id")
    name: NameStr
    serving_name: NameStr


class EntityList(PaginationMixin):
    """
    Paginated list of Entity
    """

    data: List[EntityModel]


class EntityUpdate(FeatureByteBaseModel):
    """
    Entity update schema
    """

    name: NameStr


class EntityServiceUpdate(BaseDocumentServiceUpdateSchema):
    """
    Entity service update schema
    """

    name: Optional[NameStr] = None
    ancestor_ids: Optional[List[PydanticObjectId]] = None
    parents: Optional[List[ParentEntity]] = None
    table_ids: Optional[List[PydanticObjectId]] = None
    primary_table_ids: Optional[List[PydanticObjectId]] = None

    class Settings(BaseDocumentServiceUpdateSchema.Settings):
        """
        Unique contraints checking
        """

        unique_constraints: List[UniqueValuesConstraint] = [
            UniqueValuesConstraint(
                fields=("name",),
                conflict_fields_signature={"name": ["name"]},
                resolution_signature=UniqueConstraintResolutionSignature.GET_NAME,
            ),
        ]
