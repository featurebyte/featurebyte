"""
Online prediction table task payload schema
"""

from __future__ import annotations

from featurebyte.enum import WorkerCommand
from featurebyte.models.batch_feature_table import BatchFeatureTableModel
from featurebyte.schema.batch_feature_table import BatchFeatureTableCreate
from featurebyte.schema.worker.task.base import BaseTaskPayload, TaskPriority


class BatchFeatureTableTaskPayload(BaseTaskPayload, BatchFeatureTableCreate):
    """
    BatchFeatureTable creation task payload
    """

    output_collection_name: str = BatchFeatureTableModel.collection_name()
    command: WorkerCommand = WorkerCommand.BATCH_FEATURE_TABLE_CREATE
    priority: TaskPriority = TaskPriority.CRITICAL
