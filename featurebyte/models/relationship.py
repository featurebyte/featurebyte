"""
This module contains Relation mixin model
"""
from typing import List

from bson import ObjectId
from pydantic import Field, validator

from featurebyte.models.base import (
    FeatureByteBaseDocumentModel,
    FeatureByteBaseModel,
    PydanticObjectId,
)
from featurebyte.models.validator import sort_ids_validator


class Parent(FeatureByteBaseModel):
    """
    Parent model
    """

    id: PydanticObjectId


class Relationship(FeatureByteBaseDocumentModel):
    """
    Relationship model used to track parent (or ancestor) and child relationship
    """

    ancestor_ids: List[PydanticObjectId] = Field(default_factory=list, allow_mutation=False)
    parents: List[Parent] = Field(default_factory=list, allow_mutation=False)

    # pydantic validators
    _sort_ids_validator = validator("ancestor_ids", allow_reuse=True)(sort_ids_validator)

    @validator("parents")
    @classmethod
    def _validate_parents(cls, value: List[Parent]) -> List[Parent]:
        # make sure list of parents is sorted
        return sorted(value, key=lambda parent: parent.id)
