"""
Semantic API payload schema
"""

from typing import List, Optional

from bson import ObjectId
from pydantic import Field

from featurebyte.models.base import FeatureByteBaseModel, NameStr, PydanticObjectId
from featurebyte.models.relationship import Parent
from featurebyte.models.semantic import SemanticModel
from featurebyte.schema.common.base import BaseDocumentServiceUpdateSchema, PaginationMixin


class SemanticCreate(FeatureByteBaseModel):
    """
    Semantic creation schema
    """

    id: Optional[PydanticObjectId] = Field(default_factory=ObjectId, alias="_id")
    name: NameStr


class SemanticList(PaginationMixin):
    """
    Paginated list of Semantic
    """

    data: List[SemanticModel]


class SemanticServiceUpdate(BaseDocumentServiceUpdateSchema):
    """
    Semantic service update schema
    """

    ancestor_ids: Optional[List[PydanticObjectId]]
    parents: Optional[List[Parent]]
