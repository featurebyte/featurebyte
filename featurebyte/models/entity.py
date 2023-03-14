"""
This module contains Entity related models
"""
from __future__ import annotations

from typing import Any, List

from datetime import datetime

from pydantic import Field, StrictStr, root_validator

from featurebyte.enum import TableDataType
from featurebyte.models.base import (
    FeatureByteBaseModel,
    PydanticObjectId,
    UniqueConstraintResolutionSignature,
    UniqueValuesConstraint,
)
from featurebyte.models.relationship import CatalogRelationship, Parent


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


class EntityRelationship(CatalogRelationship):
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
    tabular_data_ids: List[PydanticObjectId]
        ID of data with columns associated to the entity
    primary_tabular_data_ids: List[PydanticObjectId]
        ID of data with primary key columns associated to the entity
    """

    serving_names: List[StrictStr] = Field(allow_mutation=False)
    tabular_data_ids: List[PydanticObjectId] = Field(allow_mutation=False, default_factory=list)
    primary_tabular_data_ids: List[PydanticObjectId] = Field(
        allow_mutation=False, default_factory=list
    )

    @root_validator(pre=True)
    @classmethod
    def _validate_tabular_data_ids(cls, values: dict[str, Any]) -> dict[str, Any]:
        # DEV-752: track data ids in entity model
        values["tabular_data_ids"] = values.get("tabular_data_ids", [])
        values["primary_tabular_data_ids"] = values.get("primary_tabular_data_ids", [])
        return values

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
