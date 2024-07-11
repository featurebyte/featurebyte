"""
Common classes mixin for API payload schema
"""

from fastapi import Query

from featurebyte.models.base import PydanticObjectId

# route query parameters
COLUMN_STR_MAX_LENGTH = 255
COLUMN_STR_MIN_LENGTH = 1
PageQuery = Query(default=1, gt=0)
PageSizeQuery = Query(default=10, gt=0, le=500)
SortByQuery = Query(
    default="created_at", min_length=COLUMN_STR_MIN_LENGTH, max_length=COLUMN_STR_MAX_LENGTH
)
SortDirQuery = Query(default="desc")
SearchQuery = Query(
    default=None, min_length=COLUMN_STR_MIN_LENGTH, max_length=COLUMN_STR_MAX_LENGTH
)
NameQuery = Query(default=None, min_length=COLUMN_STR_MIN_LENGTH, max_length=COLUMN_STR_MAX_LENGTH)
VersionQuery = Query(
    default=None, min_length=COLUMN_STR_MIN_LENGTH, max_length=COLUMN_STR_MAX_LENGTH
)
AuditLogSortByQuery = Query(
    default="_id", min_length=COLUMN_STR_MIN_LENGTH, max_length=COLUMN_STR_MAX_LENGTH
)
VerboseQuery = Query(default=False)


# TODO: Change this to PyObjectId when upgrading to Pydantic v2
PyObjectId = PydanticObjectId
