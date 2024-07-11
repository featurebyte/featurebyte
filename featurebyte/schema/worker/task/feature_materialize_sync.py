"""
FeatureMaterializeSyncTaskPayload schema
"""

from typing import ClassVar

from pydantic import Field

from featurebyte.enum import WorkerCommand
from featurebyte.models.base import PydanticObjectId
from featurebyte.schema.worker.task.base import BaseTaskPayload, TaskPriority, TaskType


class FeatureMaterializeSyncTaskPayload(BaseTaskPayload):
    """
    Feature materialize sync task payload
    """

    # class variables
    command: ClassVar[WorkerCommand] = WorkerCommand.FEATURE_MATERIALIZE_SYNC

    # instance variables
    task_type: TaskType = Field(default=TaskType.IO_TASK)
    priority: TaskPriority = Field(default=TaskPriority.CRITICAL)
    offline_store_feature_table_name: str
    offline_store_feature_table_id: PydanticObjectId
