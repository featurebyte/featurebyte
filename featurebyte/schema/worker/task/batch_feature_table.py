"""
Online prediction table task payload schema
"""

from __future__ import annotations

from typing import ClassVar, Optional

from pydantic import Field

from featurebyte.enum import WorkerCommand
from featurebyte.models.batch_feature_table import BatchFeatureTableModel
from featurebyte.schema.batch_feature_table import BatchFeatureTableCreate, OutputTableInfo
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

    output_table_info: Optional[OutputTableInfo] = Field(default=None)
    parent_batch_feature_table_name: Optional[str] = Field(default=None)

    @property
    def task_output_path(self) -> Optional[str]:
        if self.output_table_info is not None:
            return None
        return super().task_output_path
