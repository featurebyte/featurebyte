"""
Common classes mixin for API payload schema
"""
from typing import Any, Optional

from beanie import PydanticObjectId
from pydantic import BaseModel, validator


class ResponseModel(BaseModel):
    """
    API Response Payload Base Class
    """

    id: Optional[PydanticObjectId]

    def __init__(self, **pydict: Any) -> None:
        super().__init__(**pydict)
        self.id = pydict.pop("_id", None)  # pylint: disable=invalid-name


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
