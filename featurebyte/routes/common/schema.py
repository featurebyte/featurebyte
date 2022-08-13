"""
Common classes mixin for API payload schema
"""
from typing import Any, List

from pydantic import Field

from featurebyte.models.base import FeatureByteBaseModel


class PaginationMixin(FeatureByteBaseModel):
    """
    Add page and page_size
    """

    page: int = Field(default=1, gt=0)
    page_size: int = Field(default=10, gt=0, lt=100)
    total: int
    data: List[Any]
