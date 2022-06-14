"""
Common classes mixin for API payload schema
"""
from bson.objectid import ObjectId
from pydantic import BaseModel, validator


class PydanticObjectId(ObjectId):
    @classmethod
    def __get_validators__(cls):
        yield cls.validate

    @classmethod
    def validate(cls, v):
        if not ObjectId.is_valid(v):
            raise ValueError("Invalid ObjectId")
        return ObjectId(v)


class PaginationMixin(BaseModel):
    """
    Add page and page_size
    """

    page: int = 1
    page_size: int = 10
    total: int

    @validator("page_size")
    @classmethod
    def limit_size(cls, value: int) -> int:
        """
        Limit page size to 100

        Parameters
        ----------
        value: int
            value to be validated / updated

        Returns
        -------
        int
            validated / updated value
        """
        assert value <= 100, "Must not exceed 100"
        return value

    @validator("page", "page_size")
    @classmethod
    def min_value(cls, value: int) -> int:
        """
        Ensure page is 1 or larger

        Parameters
        ----------
        value: int
            value to be validated / updated

        Returns
        -------
        int
            validated / updated value
        """
        assert value > 0, "Must not be smaller than 1"
        return value
