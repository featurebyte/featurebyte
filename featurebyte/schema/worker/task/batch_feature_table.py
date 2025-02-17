"""
Online prediction table task payload schema
"""

from __future__ import annotations

from typing import ClassVar, Optional

from pydantic import Field

from featurebyte.enum import WorkerCommand
from featurebyte.models.base import PydanticObjectId
from featurebyte.models.batch_feature_table import BatchFeatureTableModel
from featurebyte.schema.batch_feature_table import BatchFeatureTableCreate
from featurebyte.schema.worker.task.base import BaseTaskPayload, TaskPriority, TaskType


class BatchFeatureTableTaskPayload(BaseTaskPayload, BatchFeatureTableCreate):
    """
    BatchFeatureTable creation task payload
    """

    # class variables
    command: ClassVar[WorkerCommand] = WorkerCommand.BATCH_FEATURE_TABLE_CREATE
    output_collection_name: ClassVar[str] = BatchFeatureTableModel.collection_name()
    is_revocable: ClassVar[bool] = True
    is_rerunnable: ClassVar[bool] = True

    # instance variables
    task_type: TaskType = Field(default=TaskType.CPU_TASK)
    priority: TaskPriority = Field(default=TaskPriority.CRITICAL)
    parent_batch_feature_table_id: Optional[PydanticObjectId] = Field(default=None)
