"""
Batch feature create task payload
"""
from __future__ import annotations

from pydantic import Field

from featurebyte.enum import WorkerCommand
from featurebyte.schema.feature import BatchFeatureCreate
from featurebyte.schema.worker.task.base import BaseTaskPayload, TaskType


class BatchFeatureCreateTaskPayload(BaseTaskPayload, BatchFeatureCreate):
    """
    Batch Feature create task payload
    """

    command = WorkerCommand.BATCH_FEATURE_CREATE
    task_type: TaskType = Field(default=TaskType.CPU_TASK)
