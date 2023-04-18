"""
BatchRequestTableTaskPayload schema
"""
from __future__ import annotations

from featurebyte.enum import WorkerCommand
from featurebyte.models.batch_request_table import BatchRequestTableModel
from featurebyte.schema.batch_request_table import BatchRequestTableCreate
from featurebyte.schema.worker.task.base import BaseTaskPayload


class BatchRequestTableTaskPayload(BaseTaskPayload, BatchRequestTableCreate):
    """
    BatchRequestTable creation task payload
    """

    output_collection_name = BatchRequestTableModel.collection_name()
    command = WorkerCommand.BATCH_REQUEST_TABLE_CREATE
