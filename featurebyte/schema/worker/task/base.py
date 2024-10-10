"""
BaseTaskPayload schema
"""

from __future__ import annotations

import json
from enum import IntEnum
from typing import Any, ClassVar, Optional

from bson import ObjectId
from pydantic import ConfigDict, Field

from featurebyte.enum import StrEnum, WorkerCommand
from featurebyte.models.base import FeatureByteBaseModel, PydanticObjectId


class TaskType(StrEnum):
    """
    Task type enum
    """

    CPU_TASK = "cpu_task"
    IO_TASK = "io_task"


class TaskPriority(IntEnum):
    """
    Task priority enum
    """

    CRITICAL = 0
    HIGH = 1
    MEDIUM = 2
    LOW = 3


class BaseTaskPayload(FeatureByteBaseModel):
    """
    Base class for Task payload
    """

    # class variables
    command: ClassVar[WorkerCommand]
    output_collection_name: ClassVar[Optional[str]] = None
    is_revocable: ClassVar[bool] = False
    is_rerunnable: ClassVar[bool] = False

    # instance variables
    task_type: TaskType = Field(default=TaskType.IO_TASK)
    priority: TaskPriority = Field(default=TaskPriority.MEDIUM)
    output_document_id: PydanticObjectId = Field(default_factory=ObjectId)
    is_scheduled_task: Optional[bool] = Field(default=False)
    user_id: Optional[PydanticObjectId] = Field(default=None)
    catalog_id: PydanticObjectId

    # pydantic model configuration
    model_config = ConfigDict(frozen=True)

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

    def model_dump(self, *args: Any, **kwargs: Any) -> dict[str, Any]:
        output: dict[str, Any] = super().model_dump(*args, **kwargs)
        if self.command:
            output["command"] = self.command.value
        output["output_collection_name"] = self.output_collection_name
        output["is_revocable"] = self.is_revocable
        output["is_rerunnable"] = self.is_rerunnable
        return output

    def model_dump_json(self, *args: Any, **kwargs: Any) -> str:
        json_string = super().model_dump_json(*args, **kwargs)
        json_dict = json.loads(json_string)

        # include class variables
        json_dict.update({
            "command": self.command,
            "output_collection_name": self.output_collection_name,
            "is_revocable": self.is_revocable,
            "is_rerunnable": self.is_rerunnable,
        })
        return json.dumps(json_dict)
