"""
Common classes mixin for API payload schema
"""

from typing import Any

from bson import ObjectId
from fastapi import Query
from pydantic_core import core_schema

# route query parameters
COLUMN_STR_MAX_LENGTH = 255
COLUMN_STR_MIN_LENGTH = 1
PREVIEW_SEED = 1234
PREVIEW_LIMIT = 10_000
PREVIEW_DEFAULT = 10
DESCRIPTION_SIZE_LIMIT = 1_000_000
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


class PyObjectId(str):
    """
    Pydantic ObjectId type for API route path parameter
    """

    @classmethod
    def __get_pydantic_core_schema__(
        cls, _source_type: Any, _handler: Any
    ) -> core_schema.CoreSchema:
        return core_schema.json_or_python_schema(
            json_schema=core_schema.str_schema(),
            python_schema=core_schema.union_schema([
                core_schema.is_instance_schema(ObjectId),
                core_schema.chain_schema([
                    core_schema.str_schema(),
                    core_schema.no_info_plain_validator_function(cls.validate),
                ]),
            ]),
            serialization=core_schema.plain_serializer_function_ser_schema(str),
        )

    @classmethod
    def validate(cls, value: Any) -> ObjectId:
        """
        Validate ObjectId

        Parameters
        ----------
        value: Any
            value to validate

        Returns
        -------
        ObjectId
            ObjectId value
        """
        if not ObjectId.is_valid(value):
            raise ValueError("Invalid ObjectId")
        return ObjectId(value)
