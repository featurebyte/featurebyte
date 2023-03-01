"""
This module contains Relation mixin model
"""
from typing import Any, Dict, List

from bson import ObjectId
from pydantic import Field, root_validator, validator

from featurebyte.common.validator import construct_sort_validator
from featurebyte.enum import StrEnum
from featurebyte.models.base import (
    FeatureByteBaseDocumentModel,
    FeatureByteBaseModel,
    FeatureByteWorkspaceBaseDocumentModel,
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
    Workspace-agnostic relationship model
    """

    parents: List[Parent] = Field(default_factory=list, allow_mutation=False)
    ancestor_ids: List[PydanticObjectId] = Field(default_factory=list, allow_mutation=False)

    # pydantic validators
    _sort_ids_validator = validator("ancestor_ids", allow_reuse=True)(construct_sort_validator())
    _sort_parent_validator = validator("parents", allow_reuse=True)(
        construct_sort_validator(field="id")
    )


class WorkspaceRelationship(Relationship, FeatureByteWorkspaceBaseDocumentModel):
    """
    Workspace-specific relationship model
    """


class RelationshipType(StrEnum):
    """
    Relationship Type enum
    """

    CHILD_PARENT = "child_parent"


class RelationshipInfo(FeatureByteWorkspaceBaseDocumentModel):
    """
    Relationship info data model.

    This differs from the Relationship class above, in that each relationship is stored as a separate document.
    The Relationship class above stores all relationships for a given child in a single document.
    """

    id: PydanticObjectId = Field(default_factory=ObjectId, alias="_id", allow_mutation=False)
    relationship_type: RelationshipType
    primary_entity_id: PydanticObjectId
    related_entity_id: PydanticObjectId
    child_data_source_id: PydanticObjectId
    is_enabled: bool
    updated_by: PydanticObjectId
    comments: List[str] = Field(default_factory=list)

    class Settings:
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
                fields=("primary_entity_id", "related_entity_id"),
                conflict_fields_signature={
                    "primary_entity_id": ["primary_entity_id"],
                    "related_entity_id": ["related_entity_id"],
                },
                resolution_signature=UniqueConstraintResolutionSignature.GET_BY_ID,
            ),
        ]

    @root_validator(pre=True)
    @classmethod
    def _validate_child_and_parent_id(cls, values: Dict[str, Any]) -> Dict[str, Any]:
        child_id = values.get("primary_entity_id")
        parent_id = values.get("related_entity_id")
        if child_id == parent_id:
            raise ValueError("Primary and Related entity id cannot be the same")
        return values
