"""
This module contains Relation mixin model
"""
from typing import List

from pydantic import Field, validator

from featurebyte.common.validator import construct_sort_validator
from featurebyte.models.base import (
    FeatureByteBaseDocumentModel,
    FeatureByteBaseModel,
    FeatureByteWorkspaceBaseDocumentModel,
    PydanticObjectId,
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
