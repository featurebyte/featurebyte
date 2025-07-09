"""
DevelopmentDatasetTaskPayload schema
"""

from __future__ import annotations

from typing import ClassVar

from pydantic import Field

from featurebyte.enum import WorkerCommand
from featurebyte.models.development_dataset import DevelopmentDatasetModel
from featurebyte.schema.development_dataset import (
    DevelopmentDatasetAddTables,
    DevelopmentDatasetCreate,
)
from featurebyte.schema.worker.task.base import BaseTaskPayload, TaskType


class DevelopmentDatasetCreateTaskPayload(BaseTaskPayload, DevelopmentDatasetCreate):
    """
    DevelopmentDataset creation task payload
    """

    # class variables
    command: ClassVar[WorkerCommand] = WorkerCommand.DEVELOPMENT_DATASET_CREATE
    output_collection_name: ClassVar[str] = DevelopmentDatasetModel.collection_name()
    is_revocable: ClassVar[bool] = True

    # instance variables
    task_type: TaskType = Field(default=TaskType.CPU_TASK)


class DevelopmentDatasetDeleteTaskPayload(BaseTaskPayload):
    """
    DevelopmentDataset deletion task payload
    """

    # class variables
    command: ClassVar[WorkerCommand] = WorkerCommand.DEVELOPMENT_DATASET_DELETE
    output_collection_name: ClassVar[str] = DevelopmentDatasetModel.collection_name()
    is_revocable: ClassVar[bool] = True


class DevelopmentDatasetAddTablesTaskPayload(BaseTaskPayload, DevelopmentDatasetAddTables):
    """
    DevelopmentDataset add tables task payload
    """

    # class variables
    command: ClassVar[WorkerCommand] = WorkerCommand.DEVELOPMENT_DATASET_ADD_TABLES
    output_collection_name: ClassVar[str] = DevelopmentDatasetModel.collection_name()
    is_revocable: ClassVar[bool] = True
