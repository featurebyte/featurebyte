"""
Feature list creation with batch feature creation schema
"""
from __future__ import annotations

from pydantic import Field

from featurebyte.enum import WorkerCommand
from featurebyte.schema.feature_list import FeatureListCreateWithBatchFeatureCreation
from featurebyte.schema.worker.task.base import BaseTaskPayload, TaskType


class FeatureListCreateWithBatchFeatureCreationPayload(
    BaseTaskPayload, FeatureListCreateWithBatchFeatureCreation
):
    """
    Feature list creation with batch feature creation payload
    """

    command = WorkerCommand.FEATURE_LIST_CREATE_WITH_BATCH_FEATURE_CREATE
    task_type: TaskType = Field(default=TaskType.CPU_TASK)
