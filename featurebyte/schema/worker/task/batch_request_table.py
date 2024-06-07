"""
BatchRequestTableTaskPayload schema
"""

from __future__ import annotations

from typing import ClassVar

from pydantic import Field

from featurebyte.enum import WorkerCommand
from featurebyte.models.batch_request_table import BatchRequestTableModel
from featurebyte.schema.batch_request_table import BatchRequestTableCreate
from featurebyte.schema.worker.task.base import BaseTaskPayload, TaskPriority


class BatchRequestTableTaskPayload(BaseTaskPayload, BatchRequestTableCreate):
    """
    BatchRequestTable creation task payload
    """

    # class variables
    command: ClassVar[WorkerCommand] = WorkerCommand.BATCH_REQUEST_TABLE_CREATE
    output_collection_name: ClassVar[str] = BatchRequestTableModel.collection_name()

    # instance variables
    priority: TaskPriority = Field(default=TaskPriority.CRITICAL)
