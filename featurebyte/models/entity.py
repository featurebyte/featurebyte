"""
This module contains Entity related models
"""
# pylint: disable=too-few-public-methods
from __future__ import annotations

from typing import List

from datetime import datetime

from pydantic import Field, StrictStr

from featurebyte.enum import TableDataType
from featurebyte.models.base import (
    FeatureByteBaseModel,
    PydanticObjectId,
    UniqueConstraintResolutionSignature,
    UniqueValuesConstraint,
)
from featurebyte.models.relationship import Parent, Relationship


class EntityNameHistoryEntry(FeatureByteBaseModel):
    """
    Model for an entry in name history

    created_at: datetime
        Datetime when the history entry is created
    name: StrictStr
        Entity name that just becomes history (no longer used) at the time of the history entry creation
    """

    created_at: datetime
    name: StrictStr


class ParentEntity(Parent):
    """
    Model for parent entity

    data_type: TableDataType
        Type of data provided for constructing the entity relationship mapping
    data_id: PydanticObjectId
        ID of data provided for constructing the entity relationship mapping
    """

    data_type: TableDataType
    data_id: PydanticObjectId


class EntityRelationship(Relationship):
    """
    Model for entity relationship

    ancestor_ids: List[PydanticObjectId]
        Ancestor entities of this entity
    parents: List[ParentEntity]
        Parent entities of this entity
    """

    parents: List[ParentEntity] = Field(default_factory=list, allow_mutation=False)  # type: ignore


class EntityModel(EntityRelationship):
    """
    Model for Entity

    id: PydanticObjectId
        Entity id of the object
    name: str
        Name of the Entity
    serving_names: List[str]
        Name of the serving column
    created_at: datetime
        Datetime when the Entity object was first saved or published
    updated_at: datetime
        Datetime when the Entity object was last updated
    """

    serving_names: List[StrictStr] = Field(allow_mutation=False)

    class Settings:
        """
        MongoDB settings
        """

        collection_name: str = "entity"
        unique_constraints: List[UniqueValuesConstraint] = [
            UniqueValuesConstraint(
                fields=("_id",),
                conflict_fields_signature={"id": ["_id"]},
                resolution_signature=UniqueConstraintResolutionSignature.GET_NAME,
            ),
            UniqueValuesConstraint(
                fields=("name",),
                conflict_fields_signature={"name": ["name"]},
                resolution_signature=UniqueConstraintResolutionSignature.GET_NAME,
            ),
            UniqueValuesConstraint(
                fields=("serving_names",),
                conflict_fields_signature={"serving_name": ["serving_names", 0]},
                resolution_signature=UniqueConstraintResolutionSignature.GET_NAME,
            ),
        ]
