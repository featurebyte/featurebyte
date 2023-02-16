"""
This module contains Relation mixin model
"""
from typing import List

from pydantic import Field, validator

from featurebyte.common.validator import construct_sort_validator
from featurebyte.models.base import (
    FeatureByteBaseModel,
    FeatureByteWorkspaceBaseDocumentModel,
    PydanticObjectId,
)


class Parent(FeatureByteBaseModel):
    """
    Parent model
    """

    id: PydanticObjectId


class Relationship(FeatureByteWorkspaceBaseDocumentModel):
    """
    Relationship model used to track parent (or ancestor) and child relationship
    """

    ancestor_ids: List[PydanticObjectId] = Field(default_factory=list, allow_mutation=False)
    parents: List[Parent] = Field(default_factory=list, allow_mutation=False)

    # pydantic validators
    _sort_ids_validator = validator("ancestor_ids", allow_reuse=True)(construct_sort_validator())
    _sort_parent_validator = validator("parents", allow_reuse=True)(
        construct_sort_validator(field="id")
    )
