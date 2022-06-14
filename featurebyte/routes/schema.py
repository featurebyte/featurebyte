"""
Common classes mixin for API payload schema
"""
from typing import Any, Callable, Generator, Iterator

from bson.objectid import ObjectId
from pydantic import BaseModel, validator


class PydanticObjectId(ObjectId):
    """
    Customized ObjectId type for pydantic schemas
    """

    @classmethod
    def __get_validators__(cls) -> Generator[Callable[[Any], ObjectId], Iterator[int], None]:
        """
        Retrieve validator function

        Returns
        -------
        Callable
        """
        yield cls.validate

    @classmethod
    def validate(cls, value: Any) -> ObjectId:
        """
        Validate value

        Parameters
        ----------
        value: Any
            Value to be validated

        Returns
        -------
        ObjectId
            Validated and casted value
        """
        if not ObjectId.is_valid(value):
            raise ValueError("Invalid ObjectId")
        return ObjectId(value)


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
