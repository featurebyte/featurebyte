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
    document_id: PydanticObjectId = Field(default_factory=ObjectId)
    collection_name: ClassVar[Optional[str]] = None
    command: ClassVar[Optional[Enum]] = None

    @property
    def task_output_path(self) -> Optional[str]:
        """
        Redirect route used to retrieve the task result

        Returns
        -------
        Optional[str]
        """
        return f"{self.collection_name}/{self.document_id}" if self.collection_name else None

    def dict(self, *args: Any, **kwargs: Any) -> dict[str, Any]:
        output: dict[str, Any] = super().dict(*args, **kwargs)
        if self.command:
            output["command"] = self.command.value
        return output
