"""
Common classes mixin for API payload schema
"""
from typing import Any, List

from fastapi import Query
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


# route query parameters
COLUMN_STR_MAX_LENGTH = 255
COLUMN_STR_MIN_LENGTH = 1
PageQuery = Query(default=1, gt=0)
PageSizeQuery = Query(default=10, gt=0)
SortByQuery = Query(
    default="created_at", min_length=COLUMN_STR_MIN_LENGTH, max_length=COLUMN_STR_MAX_LENGTH
)
SortDirQuery = Query(default="desc", regex="^(asc|desc)$")
SearchQuery = Query(
    default=None, min_length=COLUMN_STR_MIN_LENGTH, max_length=COLUMN_STR_MAX_LENGTH
)
NameQuery = Query(default=None, min_length=COLUMN_STR_MIN_LENGTH, max_length=COLUMN_STR_MAX_LENGTH)
