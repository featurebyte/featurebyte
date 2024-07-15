"""
Base info related schema
"""

from __future__ import annotations

from datetime import datetime
from typing import Any, List, Optional, Sequence

from pydantic import Field

from featurebyte.models.base import FeatureByteBaseModel, UniqueValuesConstraint


class BaseBriefInfo(FeatureByteBaseModel):
    """
    Base BriefInfo schema
    """

    name: str


class BaseInfo(BaseBriefInfo):
    """
    Base Info schema
    """

    created_at: datetime
    updated_at: Optional[datetime] = Field(default=None)
    description: Optional[str] = Field(default=None)


class BaseDocumentServiceUpdateSchema(FeatureByteBaseModel):
    """
    Base schema used for document service update
    """

    class Settings:
        """
        Unique constraints checking during update
        """

        unique_constraints: List[UniqueValuesConstraint] = []


class DocumentSoftDeleteUpdate(BaseDocumentServiceUpdateSchema):
    """
    Document soft delete update schema
    """

    is_deleted: bool


class PaginationMixin(FeatureByteBaseModel):
    """
    Add page and page_size
    """

    page: int = Field(default=1, gt=0)
    page_size: int = Field(default=10, gt=0, le=500)
    total: int
    data: Sequence[Any]


class DeleteResponse(FeatureByteBaseModel):
    """
    Delete response
    """


class DescriptionUpdate(FeatureByteBaseModel):
    """
    Description update schema
    """

    description: Optional[str]
