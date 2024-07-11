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


# TODO: Change this to a PyObjectId class when upgrading to Pydantic V2.
# In Pydantic V2, PyObjectId behaves more like a str that is validated as an ObjectId whereas
# PydanticObjectId behaves like a real ObjectId. Newer FastAPI expects PyObjectId to behave like a str.
# Otherwise, FastAPI will throw an error when trying to parse the request body.
PyObjectId = PydanticObjectId
