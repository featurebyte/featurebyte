"""
This module contains Entity related models
"""

from __future__ import annotations

from datetime import datetime
from typing import List, Optional

import pymongo
from pydantic import Field, StrictStr

from featurebyte.enum import DBVarType, TableDataType
from featurebyte.models.base import (
    FeatureByteBaseModel,
    FeatureByteCatalogBaseDocumentModel,
    NameStr,
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

    table_type: TableDataType
        Type of table provided for constructing the entity relationship mapping
    table_id: PydanticObjectId
        ID of table provided for constructing the entity relationship mapping
    """

    table_type: TableDataType
    table_id: PydanticObjectId


class EntityRelationship(CatalogRelationship):
    """
    Model for entity relationship

    ancestor_ids: List[PydanticObjectId]
        Ancestor entities of this entity
    parents: List[ParentEntity]
        Parent entities of this entity
    """

    parents: List[ParentEntity] = Field(default_factory=list, frozen=True)  # type: ignore


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
    table_ids: List[PydanticObjectId]
        ID of table with columns associated to the entity
    primary_table_ids: List[PydanticObjectId]
        ID of table with primary key columns associated to the entity
    """

    dtype: Optional[DBVarType] = None
    serving_names: List[NameStr] = Field(frozen=True)
    table_ids: List[PydanticObjectId] = Field(frozen=True, default_factory=list)
    primary_table_ids: List[PydanticObjectId] = Field(frozen=True, default_factory=list)

    class Settings(FeatureByteCatalogBaseDocumentModel.Settings):
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

        indexes = FeatureByteCatalogBaseDocumentModel.Settings.indexes + [
            pymongo.operations.IndexModel("serving_names"),
            [
                ("name", pymongo.TEXT),
                ("description", pymongo.TEXT),
            ],
        ]
