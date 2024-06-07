"""
Online prediction table task payload schema
"""

from __future__ import annotations

from typing import ClassVar

from pydantic import Field

from featurebyte.enum import WorkerCommand
from featurebyte.models.batch_feature_table import BatchFeatureTableModel
from featurebyte.schema.batch_feature_table import BatchFeatureTableCreate
from featurebyte.schema.worker.task.base import BaseTaskPayload, TaskPriority


class BatchFeatureTableTaskPayload(BaseTaskPayload, BatchFeatureTableCreate):
    """
    BatchFeatureTable creation task payload
    """

    # class variables
    command: ClassVar[WorkerCommand] = WorkerCommand.BATCH_FEATURE_TABLE_CREATE
    output_collection_name: ClassVar[str] = BatchFeatureTableModel.collection_name()

    # instance variables
    priority: TaskPriority = Field(default=TaskPriority.CRITICAL)
