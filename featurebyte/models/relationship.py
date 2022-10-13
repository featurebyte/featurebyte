"""
This module contains Relation mixin model
"""
# pylint: disable=too-few-public-methods
from typing import List, Optional

from bson import ObjectId
from pydantic import Field, validator

from featurebyte.models.base import FeatureByteBaseDocumentModel, PydanticObjectId


class Relationship(FeatureByteBaseDocumentModel):
    """
    Relationship model used to track parent (or ancestor) and child relationship
    """

    ancestor_ids: List[PydanticObjectId] = Field(default_factory=list)
    parent_id: Optional[PydanticObjectId] = Field(default=None)

    @validator("ancestor_ids")
    @classmethod
    def _validate_ids(cls, value: List[ObjectId]) -> List[ObjectId]:
        # make sure list of ids always sorted
        return sorted(value)
