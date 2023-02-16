"""
Workspace API payload schema
"""
from __future__ import annotations

from typing import List, Optional

from bson.objectid import ObjectId
from pydantic import Field, StrictStr

from featurebyte.models.base import (
    FeatureByteBaseModel,
    PydanticObjectId,
    UniqueConstraintResolutionSignature,
    UniqueValuesConstraint,
)
from featurebyte.models.workspace import WorkspaceModel
from featurebyte.schema.common.base import BaseDocumentServiceUpdateSchema, PaginationMixin


class WorkspaceCreate(FeatureByteBaseModel):
    """
    Workspace creation schema
    """

    id: Optional[PydanticObjectId] = Field(default_factory=ObjectId, alias="_id")
    name: StrictStr


class WorkspaceList(PaginationMixin):
    """
    Paginated list of Workspace
    """

    data: List[WorkspaceModel]


class WorkspaceUpdate(FeatureByteBaseModel):
    """
    Workspace update schema
    """

    name: StrictStr


class WorkspaceServiceUpdate(BaseDocumentServiceUpdateSchema):
    """
    Workspace service update schema
    """

    name: Optional[str]

    class Settings(BaseDocumentServiceUpdateSchema.Settings):
        """
        Unique contraints checking
        """

        unique_constraints: List[UniqueValuesConstraint] = [
            UniqueValuesConstraint(
                fields=("name",),
                conflict_fields_signature={"name": ["name"]},
                resolution_signature=UniqueConstraintResolutionSignature.GET_NAME,
            ),
        ]
