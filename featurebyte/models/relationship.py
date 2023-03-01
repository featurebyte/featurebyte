"""
This module contains Relation mixin model
"""
from typing import Any, Dict, List

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

    relationship_type: RelationshipType
    child_id: PydanticObjectId
    parent_id: PydanticObjectId
    child_data_source_id: PydanticObjectId
    is_enabled: bool
    updated_by: PydanticObjectId

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
                fields=("child_id", "parent_id"),
                conflict_fields_signature={"child_id": ["child_id"], "parent_id": ["parent_id"]},
                resolution_signature=UniqueConstraintResolutionSignature.GET_BY_ID,
            ),
        ]

    @root_validator(pre=True)
    @classmethod
    def _validate_child_and_parent_id(cls, values: Dict[str, Any]) -> Dict[str, Any]:
        child_id = values.get("child_id")
        parent_id = values.get("parent_id")
        if child_id == parent_id:
            raise ValueError("Child and parent id cannot be the same")
        return values
