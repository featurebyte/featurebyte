"""
ScheduledFeatureMaterializeTaskPayload schema
"""

from typing import ClassVar, Optional

from pydantic import Field

from featurebyte.enum import WorkerCommand
from featurebyte.models.base import PydanticObjectId
from featurebyte.schema.worker.task.base import BaseTaskPayload, TaskPriority, TaskType


class ScheduledFeatureMaterializeTaskPayload(BaseTaskPayload):
    """
    Scheduled feature materialize task payload
    """

    # class variables
    command: ClassVar[WorkerCommand] = WorkerCommand.SCHEDULED_FEATURE_MATERIALIZE

    # instance variables
    task_type: TaskType = Field(default=TaskType.CPU_TASK)
    priority: TaskPriority = Field(default=TaskPriority.CRITICAL)
    offline_store_feature_table_name: str
    offline_store_feature_table_id: PydanticObjectId
    feature_materialize_run_id: Optional[PydanticObjectId] = Field(default=None)
