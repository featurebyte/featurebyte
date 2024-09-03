"""
Entity API payload schema
"""

from __future__ import annotations

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

    id: PydanticObjectId | None = Field(default_factory=ObjectId, alias="_id")
    name: NameStr
    serving_name: NameStr


class EntityList(PaginationMixin):
    """
    Paginated list of Entity
    """

    data: list[EntityModel]


class EntityUpdate(FeatureByteBaseModel):
    """
    Entity update schema
    """

    name: NameStr


class EntityServiceUpdate(BaseDocumentServiceUpdateSchema):
    """
    Entity service update schema
    """

    name: NameStr | None = Field(default=None)
    ancestor_ids: list[PydanticObjectId] | None = Field(default=None)
    parents: list[ParentEntity] | None = Field(default=None)
    table_ids: list[PydanticObjectId] | None = Field(default=None)
    primary_table_ids: list[PydanticObjectId] | None = Field(default=None)

    class Settings(BaseDocumentServiceUpdateSchema.Settings):
        """
        Unique contraints checking
        """

        unique_constraints: list[UniqueValuesConstraint] = [
            UniqueValuesConstraint(
                fields=("name",),
                conflict_fields_signature={"name": ["name"]},
                resolution_signature=UniqueConstraintResolutionSignature.GET_NAME,
            ),
        ]
