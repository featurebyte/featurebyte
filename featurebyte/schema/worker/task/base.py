"""
BaseTaskPayload schema
"""
from __future__ import annotations

from typing import Any, ClassVar, Optional

from enum import Enum

from beanie import PydanticObjectId
from bson.objectid import ObjectId
from pydantic import BaseModel, Field


class BaseTaskPayload(BaseModel):
    """
    Base class for Task payload
    """

    user_id: Optional[PydanticObjectId]
    output_document_id: PydanticObjectId = Field(default_factory=ObjectId)
    output_collection_name: ClassVar[Optional[str]] = None
    command: ClassVar[Optional[Enum]] = None

    class Config:
        """
        Configurations for BaseTaskPayload
        """

        # pylint: disable=too-few-public-methods

        # With `frozen` flag enable, all the object attributes are immutable.
        frozen = True

    @property
    def task_output_path(self) -> Optional[str]:
        """
        Redirect route used to retrieve the task result

        Returns
        -------
        Optional[str]
        """
        if self.output_collection_name:
            return f"{self.output_collection_name}/{self.output_document_id}"
        return None

    def dict(self, *args: Any, **kwargs: Any) -> dict[str, Any]:
        output: dict[str, Any] = super().dict(*args, **kwargs)
        if self.command:
            output["command"] = self.command.value
        return output
