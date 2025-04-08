"""
This module contains Relation mixin model
"""

from typing import List, Optional

import pymongo
from bson import ObjectId
from pydantic import Field, field_validator, model_validator

from featurebyte.common.validator import construct_sort_validator
from featurebyte.enum import StrEnum
from featurebyte.models.base import (
    FeatureByteBaseDocumentModel,
    FeatureByteBaseModel,
    FeatureByteCatalogBaseDocumentModel,
    PydanticObjectId,
    UniqueConstraintResolutionSignature,
    UniqueValuesConstraint,
)


class Parent(FeatureByteBaseModel):
    """
    Parent model
    """

    id: PydanticObjectId


class Relationship(FeatureByteBaseDocumentModel):
    """
    Catalog-agnostic relationship model
    """

    parents: List[Parent] = Field(default_factory=list, frozen=True)
    ancestor_ids: List[PydanticObjectId] = Field(default_factory=list, frozen=True)

    # pydantic validators
    _sort_ids_validator = field_validator("ancestor_ids")(construct_sort_validator())
    _sort_parent_validator = field_validator("parents")(construct_sort_validator(field="id"))

    class Settings(FeatureByteBaseDocumentModel.Settings):
        """
        MongoDB settings
        """

        indexes = FeatureByteBaseDocumentModel.Settings.indexes + [
            pymongo.operations.IndexModel("parents"),
            pymongo.operations.IndexModel("ancestor_ids"),
        ]


class CatalogRelationship(Relationship, FeatureByteCatalogBaseDocumentModel):
    """
    Catalog-specific relationship model
    """

    class Settings(FeatureByteBaseDocumentModel.Settings):
        """
        MongoDB settings
        """

        indexes = FeatureByteBaseDocumentModel.Settings.indexes + [
            pymongo.operations.IndexModel("catalog_id"),
            pymongo.operations.IndexModel("parents"),
            pymongo.operations.IndexModel("ancestor_ids"),
        ]


class RelationshipType(StrEnum):
    """
    Relationship Type enum
    """

    CHILD_PARENT = "child_parent"
    ONE_TO_ONE = "one_to_one"


class RelationshipInfoModel(FeatureByteCatalogBaseDocumentModel):
    """
    Relationship info table model.

    This differs from the Relationship class above, in that each relationship is stored as a separate document.
    The Relationship class above stores all relationships for a given child in a single document.
    """

    id: PydanticObjectId = Field(default_factory=ObjectId, alias="_id", frozen=True)
    relationship_type: RelationshipType
    entity_id: PydanticObjectId
    related_entity_id: PydanticObjectId
    relation_table_id: PydanticObjectId
    entity_column_name: Optional[str] = Field(default=None)
    related_entity_column_name: Optional[str] = Field(default=None)
    enabled: bool
    updated_by: Optional[PydanticObjectId] = Field(default=None)

    class Settings(FeatureByteCatalogBaseDocumentModel.Settings):
        """
        Settings
        """

        collection_name = "relationship_info"
        unique_constraints: List[UniqueValuesConstraint] = [
            UniqueValuesConstraint(
                fields=("_id",),
                conflict_fields_signature={"id": ["_id"]},
                resolution_signature=UniqueConstraintResolutionSignature.GET_BY_ID,
            ),
            UniqueValuesConstraint(
                fields=("entity_id", "related_entity_id"),
                conflict_fields_signature={
                    "entity_id": ["entity_id"],
                    "related_entity_id": ["related_entity_id"],
                },
                resolution_signature=UniqueConstraintResolutionSignature.GET_BY_ID,
            ),
        ]

        indexes = FeatureByteCatalogBaseDocumentModel.Settings.indexes + [
            pymongo.operations.IndexModel("relationship_type"),
            pymongo.operations.IndexModel("entity_id"),
            pymongo.operations.IndexModel("related_entity_id"),
            pymongo.operations.IndexModel("relation_table_id"),
            pymongo.operations.IndexModel("enabled"),
            [
                ("name", pymongo.TEXT),
            ],
        ]

    @model_validator(mode="after")
    def _validate_child_and_parent_id(self) -> "RelationshipInfoModel":
        if self.entity_id == self.related_entity_id:
            raise ValueError("Primary and Related entity id cannot be the same")
        return self
