"""
Feature list creation with batch feature creation schema
"""

from __future__ import annotations

from typing import ClassVar

from pydantic import Field

from featurebyte.enum import WorkerCommand
from featurebyte.models.feature_list import FeatureListModel
from featurebyte.schema.feature_list import FeatureListCreateWithBatchFeatureCreation
from featurebyte.schema.worker.task.base import BaseTaskPayload, TaskType


class FeatureListCreateWithBatchFeatureCreationTaskPayload(
    BaseTaskPayload, FeatureListCreateWithBatchFeatureCreation
):
    """
    Feature list creation with batch feature creation task payload
    """

    # class variables
    command: ClassVar[WorkerCommand] = WorkerCommand.FEATURE_LIST_CREATE_WITH_BATCH_FEATURE_CREATE
    output_collection_name: ClassVar[str] = FeatureListModel.collection_name()
    is_revocable: ClassVar[bool] = True
    is_rerunnable: ClassVar[bool] = True

    # instance variables
    task_type: TaskType = Field(default=TaskType.CPU_TASK)
