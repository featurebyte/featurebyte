"""
Online prediction table task payload schema
"""
from __future__ import annotations

from featurebyte.enum import WorkerCommand
from featurebyte.models.batch_feature_table import BatchFeatureTableModel
from featurebyte.schema.batch_feature_table import BatchFeatureTableCreate
from featurebyte.schema.worker.task.base import BaseTaskPayload


class BatchFeatureTableTaskPayload(BaseTaskPayload, BatchFeatureTableCreate):
    """
    BatchFeatureTable creation task payload
    """

    output_collection_name = BatchFeatureTableModel.collection_name()
    command = WorkerCommand.BATCH_FEATURE_TABLE_CREATE
