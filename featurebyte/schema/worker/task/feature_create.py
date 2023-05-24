"""
Feature create task payload
"""
from __future__ import annotations

from pydantic import Field

from featurebyte.enum import WorkerCommand
from featurebyte.models.feature import FeatureModel
from featurebyte.schema.feature import FeatureCreate
from featurebyte.schema.worker.task.base import BaseTaskPayload, TaskType


class BatchFeatureCreateTaskPayload(BaseTaskPayload, FeatureCreate):
    """
    Batch Feature create task payload
    """

    output_collection_name = FeatureModel.collection_name()
    command = WorkerCommand.BATCH_FEATURE_CREATE
    task_type: TaskType = Field(default=TaskType.CPU_TASK)
