"""
BaseTaskPayload schema
"""
from __future__ import annotations

from typing import Any, ClassVar, Optional

import json
from enum import Enum

from bson.objectid import ObjectId
from pydantic import Field

from featurebyte.enum import StrEnum
from featurebyte.models.base import FeatureByteBaseModel, PydanticObjectId


class TaskType(StrEnum):
    """
    Task type enum
    """

    CPU_TASK = "cpu_task"
    IO_TASK = "io_task"


class BaseTaskPayload(FeatureByteBaseModel):
    """
    Base class for Task payload
    """

    user_id: Optional[PydanticObjectId]
    catalog_id: PydanticObjectId
    output_document_id: PydanticObjectId = Field(default_factory=ObjectId)
    output_collection_name: ClassVar[Optional[str]] = None
    command: ClassVar[Optional[Enum]] = None
    task_type: TaskType = Field(default=TaskType.IO_TASK)
    priority: int = Field(default=0, ge=0, le=3)  # 0 is the highest priority
    is_scheduled_task: Optional[bool] = Field(default=False)

    class Config:
        """
        Configurations for BaseTaskPayload
        """

        # With `frozen` flag enable, all the object attributes are immutable.
        frozen = True

    @property
    def task(self) -> str:
        """
        Get task function to execute

        Returns
        -------
        str
        """
        task_map = {
            TaskType.CPU_TASK: "featurebyte.worker.task_executor.execute_cpu_task",
            TaskType.IO_TASK: "featurebyte.worker.task_executor.execute_io_task",
        }
        return task_map[self.task_type]

    @property
    def queue(self) -> str:
        """
        Get queue to use for task

        Returns
        -------
        str
        """
        queue_map = {
            TaskType.CPU_TASK: "cpu_task",
            TaskType.IO_TASK: "io_task",
        }
        queue_name = queue_map[self.task_type]
        if self.priority > 0:
            queue_name = f"{queue_name}:{self.priority}"
        return queue_name

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
