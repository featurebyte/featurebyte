"""
BaseTaskPayload schema
"""
from __future__ import annotations

from typing import Any, ClassVar, Optional

import json
from enum import Enum

from bson.objectid import ObjectId
from pydantic import Field

from featurebyte.models.base import FeatureByteBaseModel, PydanticObjectId


class BaseTaskPayload(FeatureByteBaseModel):
    """
    Base class for Task payload
    """

    user_id: Optional[PydanticObjectId]
    workspace_id: PydanticObjectId
    output_document_id: PydanticObjectId = Field(default_factory=ObjectId)
    output_collection_name: ClassVar[Optional[str]] = None
    command: ClassVar[Optional[Enum]] = None

    class Config:
        """
        Configurations for BaseTaskPayload
        """

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
            return f"/{self.output_collection_name}/{self.output_document_id}"
        return None

    def dict(self, *args: Any, **kwargs: Any) -> dict[str, Any]:
        output: dict[str, Any] = super().dict(*args, **kwargs)
        if self.command:
            output["command"] = self.command.value
        return output

    def json(self, *args: Any, **kwargs: Any) -> str:
        json_string = super().json(*args, **kwargs)
        json_dict = json.loads(json_string)

        # include class variables
        json_dict.update(
            {"command": self.command, "output_collection_name": self.output_collection_name}
        )
        return json.dumps(json_dict)
