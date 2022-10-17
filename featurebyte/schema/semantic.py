"""
Semantic API payload schema
"""
from typing import List, Optional

from bson.objectid import ObjectId
from pydantic import Field

from featurebyte.models.base import FeatureByteBaseModel, PydanticObjectId
from featurebyte.models.relationship import Parent
from featurebyte.models.semantic import SemanticModel, SemanticName
from featurebyte.routes.common.schema import PaginationMixin


class SemanticCreate(FeatureByteBaseModel):
    """
    Semantic creation schema
    """

    id: Optional[PydanticObjectId] = Field(default_factory=ObjectId, alias="_id")
    name: SemanticName


class SemanticList(PaginationMixin):
    """
    Paginated list of Semantic
    """

    data: List[SemanticModel]


class SemanticServiceUpdate(FeatureByteBaseModel):
    """
    Semantic service update schema
    """

    ancestor_ids: Optional[List[PydanticObjectId]]
    parents: Optional[List[Parent]]
