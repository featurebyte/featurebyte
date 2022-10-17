"""
This module contains Relation mixin model
"""
# pylint: disable=too-few-public-methods
from typing import List

from bson import ObjectId
from pydantic import Field, validator

from featurebyte.models.base import (
    FeatureByteBaseDocumentModel,
    FeatureByteBaseModel,
    PydanticObjectId,
)


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

    @validator("ancestor_ids")
    @classmethod
    def _validate_ids(cls, value: List[ObjectId]) -> List[ObjectId]:
        # make sure list of ids get sorted
        return sorted(value)

    @validator("parents")
    @classmethod
    def _validate_parents(cls, value: List[Parent]) -> List[Parent]:
        # make sure list of parents get sorted
        return sorted(value, key=lambda parent: parent.id)
