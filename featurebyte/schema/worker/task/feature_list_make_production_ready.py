"""
Feature list make production ready task payload
"""
from __future__ import annotations

from pydantic import Field

from featurebyte.enum import WorkerCommand
from featurebyte.models.base import PydanticObjectId
from featurebyte.schema.worker.task.base import BaseTaskPayload, TaskType


class FeatureListMakeProductionReadyTaskPayload(BaseTaskPayload):
    """
    Feature list make production ready task payload
    """

    command = WorkerCommand.FEATURE_LIST_MAKE_PRODUCTION_READY
    task_type: TaskType = Field(default=TaskType.IO_TASK)
    feature_list_id: PydanticObjectId
    ignore_guardrails: bool = False
